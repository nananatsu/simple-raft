package raft

import (
	"encoding/binary"
	"fmt"
	"kvdb/pkg/lsm"
	pb "kvdb/pkg/raftpb"
	"kvdb/pkg/skiplist"
	"os"
	"path"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

type SnapshotSegment struct {
	LastIncludeIndex uint64
	LastIncludeTerm  uint64
	datac            []chan *lsm.RawNodeData
}

type SnapshotFile struct {
	fd      *os.File
	level   int
	segment int
	offset  uint64
	done    bool
}

type Snapshot struct {
	dir              string
	data             *lsm.Tree
	lastIncludeIndex uint64
	lastIncludeTerm  uint64
	installingSnap   map[string]*SnapshotFile
	logger           *zap.SugaredLogger
}

func (ss *Snapshot) Close() {
	ss.data.Close()
}

func (ss *Snapshot) AddSnapshotSegment(segment *pb.Snapshot) (bool, error) {
	var err error
	var sf *SnapshotFile
	tmpPath := path.Join(ss.dir, "tmp")

	if ss.installingSnap == nil {
		ss.installingSnap = make(map[string]*SnapshotFile)
	}

	extra := fmt.Sprintf("%s@%d", strconv.FormatUint(segment.LastIncludeIndex, 16), segment.LastIncludeTerm)
	file := fmt.Sprintf("%d_%s_%d.sst", segment.Level, extra, segment.Segment)

	if segment.Offset == 0 {
		if _, err := os.Stat(tmpPath); err != nil {
			os.Mkdir(tmpPath, os.ModePerm)
		}
		filePath := path.Join(tmpPath, file)
		old, exsit := ss.installingSnap[file]
		if exsit {
			old.fd.Close()
		}
		os.Remove(filePath)
		fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			ss.logger.Errorf("创建临时快照文件%s失败:%v", file, err)
			return false, err
		}
		sf = &SnapshotFile{fd: fd, level: int(segment.Level), segment: int(segment.Segment), offset: 0}
		ss.installingSnap[file] = sf
	} else {
		sf = ss.installingSnap[file]
		if sf == nil {
			ss.logger.Errorf("未找到临时快照文件%s", file)
			return false, err
		}
		if sf.offset != segment.Offset {
			ss.logger.Errorf("临时快照文件%s 偏移与接收段偏移不一致", file)
			return false, err
		}
	}

	n, err := sf.fd.Write(segment.Data)
	if err != nil {
		ss.logger.Errorf("写入临时快照文件%s失败:%v", file, err)
		return false, err
	}

	if segment.Done {
		sf.fd.Close()
		sf.done = true
		ss.logger.Infof("临时快照文件%s接收完成", file)

		if segment.Level == 0 {
			ss.data.Merge(0, extra, path.Join(tmpPath, file))
			delete(ss.installingSnap, file)
			ss.lastIncludeIndex = segment.LastIncludeIndex
			ss.lastIncludeTerm = segment.LastIncludeTerm
			return true, nil
		} else if segment.Segment == 0 {
			var complete bool
			done := true
			for _, v := range ss.installingSnap {
				if v.level == int(segment.Level) {
					done = done && v.done
					if v.segment == 0 {
						complete = true
					}
				}
			}
			if complete && done {
				for k, v := range ss.installingSnap {
					ss.data.Merge(v.level, extra, path.Join(tmpPath, k))
					delete(ss.installingSnap, k)
				}
				ss.lastIncludeIndex = segment.LastIncludeIndex
				ss.lastIncludeTerm = segment.LastIncludeTerm
				return true, nil
			}
		}
	} else {
		sf.offset += uint64(n)
	}
	return false, err
}

func (ss *Snapshot) Add(sl *skiplist.SkipList) {
	if sl == nil {
		return
	}

	kvSL := skiplist.NewSkipList()
	k, v := sl.GetMax()
	index := binary.BigEndian.Uint64(k)
	term, _ := binary.Uvarint(v[1:])

	it := skiplist.NewSkipListIter(sl)
	for it.Next() {
		if pb.EntryType(uint8(it.Value[0])) == pb.EntryType_NORMAL {
			_, n := binary.Uvarint(it.Value[1:])
			kvSL.Put(Decode(it.Value[n+1:]))
		}
	}

	// n := binary.PutUvarint(ss.keyScratch[0:], index)
	// n += binary.PutUvarint(ss.keyScratch[n:], term)
	// kvSL.Put([]byte(LastLogKey), ss.keyScratch[:n])

	ss.data.FlushRecord(skiplist.NewSkipListIter(kvSL), fmt.Sprintf("%s@%s", strconv.FormatUint(index, 16), strconv.FormatUint(term, 16)))
	ss.lastIncludeIndex = index
	ss.lastIncludeTerm = term
}

func (ss *Snapshot) readSnapshot(send []*SnapshotSegment, snapc chan *pb.Snapshot) {
	defer close(snapc)
	for i := len(send) - 1; i >= 0; i-- {
		for j := len(send[i].datac) - 1; j >= 0; j-- {
			readc := send[i].datac[j]
			for {
				data := <-readc
				if data == nil {
					break
				}

				if data.Err != nil {
					ss.logger.Errorf("读取快照 %d_%d 失败: %v", data.Level, data.SeqNo, data.Err)
					return
				}

				snap := &pb.Snapshot{
					LastIncludeIndex: send[i].LastIncludeIndex,
					LastIncludeTerm:  send[i].LastIncludeTerm,
					Level:            uint32(data.Level),
					Segment:          uint32(j),
					Data:             data.Data,
					Offset:           uint64(data.Offset),
					Done:             data.Done,
				}
				snapc <- snap

				if data.Done {
					ss.logger.Debugf("读取快照 %d_%d 完成", data.Level, data.SeqNo)
					break
				}
			}
		}
	}

}

func (ss *Snapshot) getLastIncludeIndexAndTerm() (uint64, uint64, error) {
	tree := ss.data.GetNodes()
	for _, level := range tree {
		levelLen := len(level)
		if levelLen > 0 {
			lastIndex, lastTerm, err := getLastIncludeIndexAndTerm(level[levelLen-1])
			if err != nil {
				return 0, 0, err
			}
			return lastIndex, lastTerm, nil
		}
	}
	return 0, 0, nil
}

func (ss *Snapshot) GetSegment(index uint64) (chan *pb.Snapshot, error) {
	size := int64(4 * 1000 * 1000)

	send := make([]*SnapshotSegment, 0)
	tree := ss.data.GetNodes()
	var find bool

	for i, level := range tree {
		if i == 0 {
			for _, n := range level {
				lastIndex, lastTerm, err := getLastIncludeIndexAndTerm(n)
				if err != nil {
					return nil, fmt.Errorf("获取需发送快照失败: %v", err)
				}

				if lastIndex <= index {
					find = true
					break
				}

				ss.logger.Debugf("Level: %d ,Node: %d, LastIndex: %d, lastTerm: %d", n.Level, n.SeqNo, lastIndex, lastTerm)
				send = append(send, &SnapshotSegment{
					LastIncludeIndex: lastIndex,
					LastIncludeTerm:  lastTerm,
					datac:            []chan *lsm.RawNodeData{n.ReadRaw(size)},
				})
			}
		} else {
			var lastIndex uint64
			var lastTerm uint64
			for _, n := range level {
				nodeLastIndex, nodeLastTerm, err := getLastIncludeIndexAndTerm(n)
				if err != nil {
					return nil, fmt.Errorf("获取需发送快照失败: %v", err)
				}

				if nodeLastIndex > lastIndex {
					lastIndex = nodeLastIndex
					lastTerm = nodeLastTerm
				}
			}

			if lastIndex > 0 {
				datac := make([]chan *lsm.RawNodeData, len(level))
				for j, n := range level {
					datac[j] = n.ReadRaw(size)
				}
				send = append(send, &SnapshotSegment{
					LastIncludeIndex: lastIndex,
					LastIncludeTerm:  lastTerm,
					datac:            datac,
				})
				ss.logger.Debugf("待发送快照 Level: %d , LastIndex: %d, lastTerm: %d", i, lastIndex, lastTerm)

				if lastIndex <= index {
					find = true
					break
				}

			}
		}
		if find {
			break
		}
	}
	snapc := make(chan *pb.Snapshot)
	go ss.readSnapshot(send, snapc)

	return snapc, nil
}

func NewSnapshot(dir string, logger *zap.SugaredLogger) (*Snapshot, error) {

	snapDir := path.Join(dir, "snapshot")
	if _, err := os.Stat(snapDir); err != nil {
		os.Mkdir(snapDir, os.ModePerm)
	}

	tree, err := lsm.RestoreTree(lsm.NewConfig(snapDir, logger))

	if err != nil {
		return nil, fmt.Errorf("加载快照失败: %v", err)
	}

	ss := &Snapshot{
		dir:    snapDir,
		data:   tree,
		logger: logger,
	}

	lastIndex, lastTerm, err := ss.getLastIncludeIndexAndTerm()

	if err != nil {
		return ss, err
	}
	logger.Debugf("快照包含最新日志: %d", lastIndex)

	ss.lastIncludeTerm = lastTerm
	ss.lastIncludeIndex = lastIndex
	return ss, nil
}

func getLastIncludeIndexAndTerm(node *lsm.Node) (uint64, uint64, error) {
	meta := strings.Split(node.Extra, "@")
	if len(meta) != 2 {
		return 0, 0, fmt.Errorf("解析%s最后日志Index、Term失败", node.Extra)
	}

	lastIndex, err := strconv.ParseUint(meta[0], 16, 0)
	if err != nil {
		return 0, 0, err
	}

	lastTerm, err := strconv.ParseUint(meta[1], 16, 0)
	if err != nil {
		return 0, 0, err
	}
	return lastIndex, lastTerm, nil
}
