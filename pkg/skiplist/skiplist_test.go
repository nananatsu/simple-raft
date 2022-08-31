package skiplist

import (
	"bytes"
	"encoding/binary"
	"testing"

	"go.uber.org/zap"
)

func TestSkipList(t *testing.T) {

	db := NewSkipList()

	key := []byte("hello")
	key2 := []byte("olleh")

	db.Put(key, []byte("world"))

	if string(db.Get(key)) != "world" {
		t.Errorf("失败")
	}

	db.Put(key, []byte("dlrow"))

	if string(db.Get(key)) != "dlrow" {
		t.Errorf("失败")
	}

	db.Put(key2, []byte("llominus"))

	if string(db.Get(key2)) != "llominus" {
		t.Errorf("失败")
	}

}

func TestSkipList2(t *testing.T) {

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	db := NewSkipList()

	buf := make([]byte, 10)
	// for i := 0; i < 1000; i++ {
	//  n := binary.PutUvarint(buf[0:], uint64(i))
	// 	db.Put(buf[:n], buf[:n])
	// }

	for i := 0; i < 10000; i++ {
		binary.BigEndian.PutUint64(buf[0:], uint64(i))
		db.Put(buf[:8], buf[:8])
	}

	it := NewSkipListIter(db)

	var prevKey []byte
	var prev uint64

	for it.Next() {

		cmp := bytes.Compare(it.Key, prevKey)
		// k2, _ := binary.Uvarint(it.Key)
		k2 := binary.BigEndian.Uint64(it.Key)
		sugar.Debugf("当前KEY: %d ,前次KEY: %d , bytes.Compare(it.Key, preKey) = %d,", k2, prev, cmp)

		prev = k2
		prevKey = it.Key

	}

}
