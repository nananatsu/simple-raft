package raft

import (
	"encoding/binary"
	"io/fs"
	pb "kvdb/pkg/raftpb"
	"kvdb/pkg/rawdb"
	"kvdb/pkg/utils"
	"os"
	"path"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const LastLogKey = "___last___include___"
const logSnapShotSize = 8 * 1024 * 1024
const memdbFlushInterval = 10 * time.Second

type Storage interface {
	Append(entries []*pb.LogEntry)
	GetEntries(startIndex, endIndex uint64) []*pb.LogEntry
	GetTerm(index uint64) uint64
	GetLast() (uint64, uint64)
	Notify() []*pb.MemberChange
	HasNotify() bool
	Close()
}

type RaftStorage struct {
	mu         sync.RWMutex
	db         *rawdb.MemDB
	snap       *Snapshot
	keyScratch [20]byte
	notify     [][]*pb.MemberChange
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
	rs.mu.Lock()
	defer rs.mu.Unlock()

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
		rs.db.Put(rs.keyScratch[:8], append(rs.keyScratch[8:8+n+1], entry.Data...))
	}

	if rs.db.Size() > logSnapShotSize {
		rs.snap.Add(rs.db)
		rs.db = rawdb.NewMemDB(&rawdb.MemDBConfig{Dir: rs.db.GetDir(), SeqNo: rs.db.GetSeqNo() + 1, WalFlushInterval: memdbFlushInterval}, rs.logger)
	}
}

func (rs *RaftStorage) GetTerm(index uint64) (term uint64) {
	if index == rs.snap.latIncludeIndex {
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
	return rs.snap.latIncludeIndex, rs.snap.lastIncludeTerm
}

// 区间: [)
func (rs *RaftStorage) GetEntries(startIndex, endIndex uint64) []*pb.LogEntry {

	if startIndex < rs.snap.latIncludeIndex {
		rs.logger.Infof("请求日志 %d 存在于snapshot: %d", startIndex, rs.snap.latIncludeIndex)
		return nil
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

	return ret
}

func (rs *RaftStorage) Close() {
	rs.db.Close()
	rs.snap.Close()
}

func NewRaftStorage(dir string, logger *zap.SugaredLogger) *RaftStorage {

	if _, err := os.Stat(dir); err != nil {
		os.Mkdir(dir, os.ModePerm)
	}

	walDir := path.Join(dir, "wal")
	if _, err := os.Stat(walDir); err != nil {
		os.Mkdir(walDir, os.ModePerm)
	}

	maxSeqNo := 0
	memdbs := make([]*rawdb.MemDB, 0)
	callbacks := []func(int, int, string, fs.FileInfo){
		func(level, seqNo int, subfix string, info fs.FileInfo) {
			if level == 0 && seqNo > maxSeqNo {
				maxSeqNo = seqNo
			}
		},
		func(level, seqNo int, subfix string, info fs.FileInfo) {
			if subfix == "wal" {
				db, err := rawdb.RestoreMemDB(&rawdb.MemDBConfig{Dir: walDir, SeqNo: seqNo, WalFlushInterval: memdbFlushInterval}, logger)
				if err != nil {
					logger.Errorf("还原memdb失败:%v", err)
				} else {
					memdbs = append(memdbs, db)
				}
			}
		},
	}

	if err := utils.CheckDir(walDir, callbacks); err != nil {
		logger.Infof("打开db文件夹失败", err)
	}

	var db *rawdb.MemDB
	for i, md := range memdbs {
		if md.GetSeqNo() == maxSeqNo {
			db = md
			memdbs[i] = nil
		}
	}
	if db == nil {
		db = rawdb.NewMemDB(&rawdb.MemDBConfig{Dir: walDir, SeqNo: maxSeqNo + 1, WalFlushInterval: memdbFlushInterval}, logger)
	}

	s := &RaftStorage{
		db:     db,
		snap:   NewSnapshot(dir, logger),
		notify: make([][]*pb.MemberChange, 0),
		logger: logger,
	}

	for _, md := range memdbs {
		s.snap.Add(md)
	}
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
