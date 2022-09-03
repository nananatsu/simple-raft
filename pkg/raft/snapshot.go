package raft

import (
	"encoding/binary"
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

func (ss *Snapshot) Add(memdb *rawdb.MemDB) {
	if memdb == nil {
		return
	}

	k, v := memdb.GetMax()
	index := binary.BigEndian.Uint64(k)
	term, _ := binary.Uvarint(v)

	it := memdb.GenerateIterator()
	for it.Next() {
		ss.data.Put(Decode(it.Value))
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
