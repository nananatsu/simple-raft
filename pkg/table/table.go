package table

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"kvdb/pkg/memtable/skiplist"
	"os"
	"path"
	"strconv"
	"sync"
)

const maxMemTableSize = 4096 * 1024
const restartInterval = 16

type Table struct {
	mu sync.RWMutex

	dbPath    string
	memTable  *skiplist.SkipList
	iMemTable []*ImmutableMemTable
}

func (t *Table) Put(key, value string) error {

	t.memTable.Put([]byte(key), []byte(value))

	if t.memTable.Size() > maxMemTableSize {
		t.flush()
	}

	return nil
}

func (t *Table) flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	filePath := path.Join(t.dbPath, "0."+strconv.Itoa(len(t.iMemTable))+".sst")
	immutableTable := NewImmutableMemTable(filePath, t.memTable, restartInterval)
	t.iMemTable = append(t.iMemTable, immutableTable)

	t.memTable = skiplist.NewMemTable()

	go immutableTable.FlushMemTable()

	return nil
}

func NewTable(dbPath string) *Table {

	sl := skiplist.NewMemTable()

	return &Table{dbPath: dbPath, memTable: sl}
}

func readSstTable(sstFilePath string) {

	fd, err := os.OpenFile(sstFilePath, os.O_RDONLY, 0644)

	if err != nil {
		fmt.Println("打开db文件失败", err)
	}

	reader := bufio.NewReader(fd)

	_, err = fd.Seek(-40, io.SeekEnd)

	if err != nil {
		fmt.Println("移动读取位置到meta data失败", err)
	}

	filterOffset, err := binary.ReadUvarint(reader)

	if err != nil {
		fmt.Println("读取过滤器偏移失败", err)
	}

	indexOffset, err := binary.ReadUvarint(reader)

	if err != nil {
		fmt.Println("读取索引偏移失败", err)
	}

	indexSize, err := binary.ReadUvarint(reader)

	if err != nil {
		fmt.Println("读取索引长度失败", err)
	}

	fmt.Println("过滤器偏移:", filterOffset, "索引偏移:", indexOffset, "索引长度", indexSize)

	_, err = fd.Seek(0, io.SeekStart)

	if err != nil {
		fmt.Println("移动读取位置到原点失败", err)
	}
	reader.Reset(fd)

	prevKey := make([]byte, 0)

	for {

		keyPrefixLen, err := binary.ReadUvarint(reader)

		if err != nil {
			fmt.Println("读取key共享长度失败", err)
			break
		}

		keyLen, err := binary.ReadUvarint(reader)

		if err != nil {
			fmt.Println("读取key长度失败", err)
			break
		}

		valueLen, err := binary.ReadUvarint(reader)

		if err != nil {
			fmt.Println("读取Value长度失败", err)
			break
		}

		key := make([]byte, keyLen)
		value := make([]byte, valueLen)

		_, err = io.ReadFull(reader, key)
		if err != nil {
			fmt.Println("读取Key失败", err)
			break
		}

		_, err = io.ReadFull(reader, value)
		if err != nil {
			fmt.Println("读取Value失败", err)
			break
		}

		fmt.Println("key:", keyPrefixLen+keyLen, string(prevKey[0:keyPrefixLen])+string(key), "value:", valueLen, string(value))

		prevKey = append(prevKey[:0], key...)

		offset, err := fd.Seek(0, io.SeekCurrent)
		if err != nil {
			fmt.Println("取得当前偏移失败", err)
		}

		if uint64(offset)-uint64(reader.Buffered()) >= filterOffset {
			fmt.Println("data读取结束")
			break
		}
	}

}
