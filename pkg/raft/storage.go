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

	"go.uber.org/zap"
)

const logSnapShotSize = 4 * 1024 * 1024

type Storage interface {
	Append(entries []*pb.LogEntry)
	GetTerm(index uint64) uint64
	GetFirst() (uint64, uint64)
	GetLast() (uint64, uint64)
}

type RaftStorage struct {
	mu sync.RWMutex

	db         *rawdb.MemDB
	snap       *rawdb.DB
	keyScratch [20]byte
	logger     *zap.SugaredLogger
}

func (rs *RaftStorage) Append(entries []*pb.LogEntry) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	for _, entry := range entries {
		binary.BigEndian.PutUint64(rs.keyScratch[0:], entry.Index)
		n := binary.PutUvarint(rs.keyScratch[8:], entry.Term)
		rs.db.Put(rs.keyScratch[:8], append(rs.keyScratch[8:8+n], entry.Data...))
	}

	if rs.db.Size() > logSnapShotSize {
		memdb := rs.db
		rs.Snapshot(memdb)
		rs.db = rawdb.NewMemDB(memdb.Dir, memdb.SeqNo+1, rs.logger)
	}
}

func (rs *RaftStorage) GetTerm(index uint64) (term uint64) {

	binary.BigEndian.PutUint64(rs.keyScratch[0:], index)
	value := rs.db.Get(rs.keyScratch[:8])

	if value != nil {
		term, _ = binary.Uvarint(value)
	}
	return
}

func (rs *RaftStorage) GetFirst() (uint64, uint64) {
	k, v := rs.db.GetMin()
	index := binary.BigEndian.Uint64(k)
	term, _ := binary.Uvarint(v)
	return index, term
}

func (rs *RaftStorage) GetLast() (uint64, uint64) {
	k, v := rs.db.GetMax()
	index := binary.BigEndian.Uint64(k)
	term, _ := binary.Uvarint(v)
	return index, term
}

func (rs *RaftStorage) Snapshot(memdb *rawdb.MemDB) {
	if memdb == nil {
		return
	}

	it := memdb.GenerateIterator()
	for it.Next() {
		rs.snap.Put(Decode(it.Value))
	}

	memdb.Finish()
}

func NewRaftStorage(dir string, logger *zap.SugaredLogger) *RaftStorage {

	if _, err := os.Stat(dir); err != nil {
		os.Mkdir(dir, os.ModePerm)
	}

	snapDir := path.Join(dir, "snapshot")
	if _, err := os.Stat(snapDir); err != nil {
		os.Mkdir(snapDir, os.ModePerm)
	}
	rawdb.NewRawDB(snapDir, logger)

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
				memdbs = append(memdbs, rawdb.RestoreMemDB(walDir, seqNo, logger))
			}
		},
	}

	if err := utils.CheckDir(walDir, callbacks); err != nil {
		logger.Infof("打开db文件夹失败", err)
	}

	var db *rawdb.MemDB
	for i, md := range memdbs {
		if md.SeqNo == maxSeqNo {
			db = md
			memdbs[i] = nil
		}
	}
	if db == nil {
		db = rawdb.NewMemDB(walDir, maxSeqNo+1, logger)
	}

	snap := rawdb.NewRawDB(snapDir, logger)

	s := &RaftStorage{
		db:     db,
		snap:   snap,
		logger: logger,
	}
	for _, md := range memdbs {
		s.Snapshot(md)
	}
	return s
}

func Decode(entry []byte) ([]byte, []byte) {
	_, n := binary.Uvarint(entry)
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
