package raft

import (
	"encoding/binary"
	pb "kvdb/pkg/raftpb"
	"kvdb/pkg/rawdb"
	"os"
	"path"

	"go.uber.org/zap"
)

type Snapshot struct {
	data            *rawdb.DB
	latIncludeIndex uint64
	lastIncludeTerm uint64
	keyScratch      [20]byte
}

func (ss *Snapshot) Close() {
	ss.data.Close()
}

func (ss *Snapshot) Add(memdb *rawdb.MemDB) {
	if memdb == nil {
		return
	}

	k, v := memdb.GetMax()
	index := binary.BigEndian.Uint64(k)
	term, _ := binary.Uvarint(v[1:])

	it := memdb.GenerateIterator()
	for it.Next() {
		if pb.EntryType(uint8(it.Value[0])) == pb.EntryType_NORMAL {
			_, n := binary.Uvarint(it.Value[1:])
			ss.data.Put(Decode(it.Value[n+1:]))
		}
	}

	n := binary.PutUvarint(ss.keyScratch[0:], index)
	n += binary.PutUvarint(ss.keyScratch[n:], term)
	ss.data.Put([]byte(LastLogKey), ss.keyScratch[:n])
	ss.data.Flush()
	memdb.Finish()
}

func NewSnapshot(dir string, logger *zap.SugaredLogger) *Snapshot {

	snapDir := path.Join(dir, "snapshot")
	if _, err := os.Stat(snapDir); err != nil {
		os.Mkdir(snapDir, os.ModePerm)
	}

	dbconf := rawdb.NewConfig()
	dbconf.MaxMemDBSize = 0
	data := rawdb.NewRawDB(snapDir, dbconf, logger)

	ss := &Snapshot{
		data: data,
	}

	lastLogMeta := data.Get([]byte(LastLogKey))
	if lastLogMeta != nil {
		index, n := binary.Uvarint(lastLogMeta)
		term, _ := binary.Uvarint(lastLogMeta[n:])
		ss.lastIncludeTerm = term
		ss.latIncludeIndex = index
	}
	return ss
}
