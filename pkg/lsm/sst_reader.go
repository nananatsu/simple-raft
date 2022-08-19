package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/golang/snappy"
)

type SstReader struct {
	mu sync.RWMutex

	fd     *os.File
	reader *bufio.Reader

	FilterOffset int64
	IndexOffset  int64
	IndexSize    int64

	compressScratch []byte
}

type MetaData struct {
	FilterOffset int64
	IndexOffset  int64
	IndexSize    int64
}

type Index struct {
	Key    []byte
	Offset int64
	Size   int64
}

func (r *SstReader) ReadMetaData() error {

	_, err := r.fd.Seek(-footerLen, io.SeekEnd)

	if err != nil {
		return err
	}

	filterOffset, err := binary.ReadUvarint(r.reader)

	if err != nil {
		return err
	}

	indexOffset, err := binary.ReadUvarint(r.reader)

	if err != nil {
		return err
	}

	indexSize, err := binary.ReadUvarint(r.reader)

	if err != nil {
		return err
	}

	if filterOffset == 0 || indexOffset == 0 || indexSize == 0 {
		return fmt.Errorf("无法解析文件")
	}

	r.FilterOffset = int64(filterOffset)
	r.IndexOffset = int64(indexOffset)
	r.IndexSize = int64(indexSize)

	return nil
}

func (r *SstReader) ReadFilter() (filter []byte, err error) {

	if r.FilterOffset == 0 {
		if err = r.ReadMetaData(); err != nil {
			return nil, err
		}
	}

	if _, err = r.fd.Seek(r.FilterOffset, io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	filter, err = r.Read(r.IndexOffset - r.FilterOffset)
	return
}

func (r *SstReader) ReadIndex() (index []*Index, err error) {

	if r.IndexOffset == 0 {
		if err = r.ReadMetaData(); err != nil {
			return nil, err
		}
	}

	if _, err = r.fd.Seek(r.IndexOffset, io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	compress, err := r.Read(r.IndexSize - 4)

	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, compress)

	if err != nil {
		return nil, err
	}
	index = ReadIndex(data)

	return
}

func (r *SstReader) ReadBlock(offset, size int64) (data []byte, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err = r.fd.Seek(r.IndexOffset, io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	compressed, err := r.Read(size - 4)

	if err != nil {
		return nil, err
	}

	dataLen, err := snappy.DecodedLen(compressed)

	if err != nil {
		return nil, err
	}

	if len(r.compressScratch) < dataLen {
		r.compressScratch = make([]byte, dataLen)
	}

	data, err = snappy.Decode(r.compressScratch, compressed)

	if err != nil {
		return nil, err
	}

	return
}

func (r *SstReader) Read(size int64) (b []byte, err error) {

	b = make([]byte, size)

	_, err = io.ReadFull(r.reader, b)

	return b, err
}

func (r *SstReader) Destory() {
	r.reader.Reset(r.fd)
	r.fd.Close()
	os.Remove(r.fd.Name())
}
func NewSstReader(fd *os.File) *SstReader {

	return &SstReader{
		fd:     fd,
		reader: bufio.NewReader(fd),
	}
}

func DecodeBlock(block []byte) ([]byte, []int) {

	n := len(block)

	nRestartPoint := int(binary.LittleEndian.Uint32(block[n-4:]))
	oRestartPoint := n - (nRestartPoint * 4) - 4
	restartPoint := make([]int, nRestartPoint)

	for i := 0; i < nRestartPoint; i++ {
		restartPoint[i] = int(binary.LittleEndian.Uint32(block[oRestartPoint+i*4:]))
	}

	return block[:oRestartPoint], restartPoint
}

func ReadRecord(prevKey []byte, buf *bytes.Buffer) ([]byte, []byte, error) {

	keyPrefixLen, err := binary.ReadUvarint(buf)

	if err != nil {
		// log.Println("读取key共享长度失败", err)
		return nil, nil, err
	}

	keyLen, err := binary.ReadUvarint(buf)

	if err != nil {
		// log.Println("读取key长度失败", err)
		return nil, nil, err
	}

	valueLen, err := binary.ReadUvarint(buf)

	if err != nil {
		// log.Println("读取Value长度失败", err)
		return nil, nil, err
	}

	key := make([]byte, keyLen)

	_, err = io.ReadFull(buf, key)
	if err != nil {
		// log.Println("读取Key失败", err)
		return nil, nil, err
	}

	value := make([]byte, valueLen)

	_, err = io.ReadFull(buf, value)
	if err != nil {
		// log.Println("读取Value失败", err)
		return nil, nil, err
	}

	actualKey := make([]byte, keyPrefixLen)
	copy(actualKey, prevKey[0:keyPrefixLen])
	actualKey = append(actualKey, key...)

	return actualKey, value, nil
}

func ReadIndex(index []byte) []*Index {

	data, _ := DecodeBlock(index)
	indexBuf := bytes.NewBuffer(data)

	indexes := make([]*Index, 0)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, indexBuf)

		if err != nil {
			break
		}

		buf := bytes.NewBuffer(value)

		offset, err := binary.ReadUvarint(buf)

		if err != nil {
			log.Println("读取块偏移失败", err)
		}

		size, err := binary.ReadUvarint(buf)

		if err != nil {
			log.Println("读取块大小失败", err)
		}

		indexes = append(indexes, &Index{
			Key:    key,
			Offset: int64(offset),
			Size:   int64(size),
		})
		prevKey = key
	}
	return indexes
}
