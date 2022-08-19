package skiplist

import (
	"testing"
)

func TestMemTable(t *testing.T) {

	table := NewSkipList()

	key := []byte("hello")
	key2 := []byte("olleh")

	table.Put(key, []byte("world"))

	if string(table.Get(key)) != "world" {
		t.Errorf("失败")
	}

	table.Put(key, []byte("dlrow"))

	if string(table.Get(key)) != "dlrow" {
		t.Errorf("失败")
	}

	table.Put(key2, []byte("llominus"))

	if string(table.Get(key2)) != "llominus" {
		t.Errorf("失败")
	}

}
