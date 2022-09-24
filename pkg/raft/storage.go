package raft

import (
	"encoding/binary"
	"io/fs"
	pb "kvdb/pkg/raftpb"
	"kvdb/pkg/skiplist"
	"kvdb/pkg/utils"
	"kvdb/pkg/wal"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const logSnapShotSize = 8 * 1024 * 1024
const walFlushInterval = 10 * time.Second

type Storage interface {
	Append(entries []*pb.LogEntry)
	Snapshot(force bool)
	InstallSnapshot(snap *pb.Snapshot) (bool, error)
	GetEntries(startIndex, endIndex uint64) ([]*pb.LogEntry, chan *pb.Snapshot)
	GetTerm(index uint64) uint64
	GetLast() (uint64, uint64)
	Notify() []*pb.MemberChange
	HasNotify() bool
	Close()
}

type RaftStorage struct {
	walw       *wal.WalWriter
	db         *skiplist.SkipList
	snap       *Snapshot
	keyScratch [20]byte
	notify     [][]*pb.MemberChange
	stopc      chan struct{}
	logger     *zap.SugaredLogger
}

func (rs *RaftStorage) HasNotify() bool {
	return len(rs.notify) > 0
}

func (rs *RaftStorage) Notify() []*pb.MemberChange {
	changes := rs.notify[0]
	rs.notify = rs.notify[1:]
	return changes
}

func (rs *RaftStorage) Append(entries []*pb.LogEntry) {
	for _, entry := range entries {
		if entry.Type == pb.EntryType_MEMBER_CHNAGE {
			var changeCol pb.MemberChangeCollection
			err := proto.Unmarshal(entry.Data, &changeCol)
			if err != nil {
				rs.logger.Warnf("解析成员变更日志失败: %v", err)
			}
			rs.notify = append(rs.notify, changeCol.Changes)
		}
		binary.BigEndian.PutUint64(rs.keyScratch[0:], entry.Index)
		rs.keyScratch[8] = uint8(entry.Type)
		n := binary.PutUvarint(rs.keyScratch[9:], entry.Term)

		k := rs.keyScratch[:8]
		v := append(rs.keyScratch[8:8+n+1], entry.Data...)

		rs.walw.Write(k, v)
		rs.db.Put(k, v)
	}
	rs.Snapshot(false)
}

func (rs *RaftStorage) Snapshot(force bool) {
	if rs.db.Size() > logSnapShotSize || force {
		oldWalw := rs.walw
		walw, err := rs.walw.Next()
		if err != nil {
			oldWalw = nil
			rs.logger.Errorf("新建预写日志失败: %v", err)
		} else {
			rs.walw = walw
		}
		go func(w *wal.WalWriter, sl *skiplist.SkipList) {
			rs.snap.Add(sl)
			if oldWalw != nil {
				oldWalw.Finish()
			}
		}(oldWalw, rs.db)
		rs.db = skiplist.NewSkipList()
	}
}

func (rs *RaftStorage) GetTerm(index uint64) (term uint64) {
	if index == rs.snap.lastIncludeIndex {
		return rs.snap.lastIncludeTerm
	}

	binary.BigEndian.PutUint64(rs.keyScratch[0:], index)
	value := rs.db.Get(rs.keyScratch[:8])

	if value != nil {
		term, _ = binary.Uvarint(value[1:])
	}
	return
}

func (rs *RaftStorage) GetLast() (uint64, uint64) {
	k, v := rs.db.GetMax()
	if len(k) > 0 {
		index := binary.BigEndian.Uint64(k)
		term, _ := binary.Uvarint(v[1:])
		return index, term
	}
	return rs.snap.lastIncludeIndex, rs.snap.lastIncludeTerm
}

// 区间: [)
func (rs *RaftStorage) GetEntries(startIndex, endIndex uint64) ([]*pb.LogEntry, chan *pb.Snapshot) {

	if startIndex < rs.snap.lastIncludeIndex {
		rs.logger.Infof("请求日志 %d 已压缩到快照: %d", startIndex, rs.snap.lastIncludeIndex)
		snapc, err := rs.getSnapshot(startIndex)
		if err != nil {
			rs.logger.Infof("获取快照失败: %v", err)
		}

		return nil, snapc
	}

	binary.BigEndian.PutUint64(rs.keyScratch[0:], startIndex)
	binary.BigEndian.PutUint64(rs.keyScratch[8:], endIndex)

	kvs := rs.db.GetRange(rs.keyScratch[:8], rs.keyScratch[8:16])
	size := len(kvs)

	ret := make([]*pb.LogEntry, len(kvs))
	for i, kv := range kvs {
		entryType := uint8(kv[1][0])
		termUint, n := binary.Uvarint(kv[1][1:])
		index := binary.BigEndian.Uint64(kv[0])
		ret[i] = &pb.LogEntry{Type: pb.EntryType(entryType), Index: index, Term: termUint, Data: kv[1][n+1:]}
	}
	if len(ret) > 0 {
		rs.logger.Warnf("请求日志: %d~%d ,实际加载: %d~%d (总数: %d)", startIndex, endIndex, ret[0].Index, ret[size-1].Index, size)
	} else {
		rs.logger.Warnf("请求日志: %d~%d ,实际加载： 0", startIndex, endIndex)
	}

	return ret, nil
}

func (rs *RaftStorage) getSnapshot(index uint64) (chan *pb.Snapshot, error) {
	return rs.snap.GetSegment(index)
}

func (rs *RaftStorage) InstallSnapshot(snap *pb.Snapshot) (bool, error) {
	return rs.snap.AddSnapshotSegment(snap)
}

func (rs *RaftStorage) Close() {

	rs.stopc <- struct{}{}
	rs.snap.Close()
}

func (rs *RaftStorage) checkFlush() {
	go func() {
		ticker := time.NewTicker(walFlushInterval)
		for {
			select {
			case <-ticker.C:
				rs.walw.Flush()
			case <-rs.stopc:
				rs.walw.Flush()
				return
			}
		}
	}()
}

func NewRaftStorage(dir string, logger *zap.SugaredLogger) *RaftStorage {

	if _, err := os.Stat(dir); err != nil {
		os.Mkdir(dir, os.ModePerm)
	}

	walDir := path.Join(dir, "wal")
	if _, err := os.Stat(walDir); err != nil {
		os.Mkdir(walDir, os.ModePerm)
	}

	memdbs := make(map[int]*skiplist.SkipList, 1)
	wals := *new(sort.IntSlice)

	callbacks := []func(string, fs.FileInfo){
		func(name string, fileInfo fs.FileInfo) {
			info := strings.Split(name, ".")
			if len(info) != 2 {
				return
			}

			seqNo, err := strconv.Atoi(info[0])
			if err != nil {
				return
			}

			if info[1] == "wal" {
				file := path.Join(walDir, strconv.Itoa(seqNo)+".wal")
				db, err := wal.Restore(file)
				if err != nil {
					logger.Errorf("还原%s失败:%v", file, err)
				}

				logger.Infof("还原预写日志 %s", file)
				if db != nil {
					wals = append(wals, seqNo)
					memdbs[seqNo] = db
				}
			}
		},
	}

	if err := utils.CheckDir(walDir, callbacks); err != nil {
		logger.Errorf("打开db文件夹%s 失败: %v", walDir, err)
	}

	var db *skiplist.SkipList
	var seq int
	wals.Sort()
	if wals.Len() > 0 {
		seq = wals[wals.Len()-1]
		db = memdbs[seq]
		delete(memdbs, seq)
	}

	if db == nil {
		db = skiplist.NewSkipList()
	}

	w, err := wal.NewWalWriter(walDir, seq, logger)
	if err != nil {
		logger.Errorf("创建wal writer失败: %v", walDir, err)
	}

	snap, err := NewSnapshot(dir, logger)

	if err != nil {
		logger.Errorf("读取快照失败", err)
	}

	s := &RaftStorage{
		walw:   w,
		db:     db,
		snap:   snap,
		notify: make([][]*pb.MemberChange, 0),
		stopc:  make(chan struct{}),
		logger: logger,
	}

	for seq, md := range memdbs {
		s.snap.Add(md)
		os.Remove(path.Join(walDir, strconv.Itoa(seq)+".wal"))
	}

	s.checkFlush()

	return s
}

func Decode(entry []byte) ([]byte, []byte) {
	var n int
	keyLen, m := binary.Uvarint(entry[n:])
	n += m
	valueLen, m := binary.Uvarint(entry[n:])
	n += m
	return entry[n : n+int(keyLen)], entry[n+int(keyLen) : n+int(keyLen)+int(valueLen)]
}

func Encode(key, value []byte) []byte {
	header := make([]byte, 20)

	n := binary.PutUvarint(header[0:], uint64(len(key)))
	n += binary.PutUvarint(header[n:], uint64(len(value)))
	length := len(key) + len(value) + n

	b := make([]byte, length)
	copy(b, header[:n])
	copy(b[n:], key)
	copy(b[n+len(key):], value)

	return b
}
