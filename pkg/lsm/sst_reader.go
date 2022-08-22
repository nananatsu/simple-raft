package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"kvdb/pkg/utils"
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
	FilterSize   int64
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
	Offset uint64
	Size   uint64
}

func (r *SstReader) ReadFooter() error {

	_, err := r.fd.Seek(-footerLen, io.SeekEnd)

	if err != nil {
		return err
	}

	filterOffset, err := binary.ReadUvarint(r.reader)

	if err != nil {
		return err
	}

	filterSize, err := binary.ReadUvarint(r.reader)

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

	if filterOffset == 0 || filterSize == 0 || indexOffset == 0 || indexSize == 0 {
		log.Println(filterOffset, filterSize, indexOffset, indexSize)
		return fmt.Errorf("无法解析文件")
	}

	r.FilterOffset = int64(filterOffset)
	r.FilterSize = int64(filterSize)
	r.IndexOffset = int64(indexOffset)
	r.IndexSize = int64(indexSize)

	return nil
}

func (r *SstReader) ReadFilter() (filter map[uint64][]byte, err error) {

	if r.FilterOffset == 0 {
		if err = r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	if _, err = r.fd.Seek(r.FilterOffset, io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	compress, err := r.Read(r.FilterSize)

	if err != nil {
		return nil, err
	}

	crc := binary.LittleEndian.Uint32(compress[r.FilterSize-4:])
	compressData := compress[:r.FilterSize-4]

	if utils.Checksum(compressData) != crc {
		return nil, fmt.Errorf("数据块校验失败")
	}

	data, err := snappy.Decode(nil, compressData)

	if err != nil {
		return nil, err
	}
	filter = ReadFilter(data)

	return
}

func (r *SstReader) ReadIndex() (index []*Index, err error) {

	if r.IndexOffset == 0 {
		if err = r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	if _, err = r.fd.Seek(r.IndexOffset, io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	compress, err := r.Read(r.IndexSize)

	if err != nil {
		return nil, err
	}
	crc := binary.LittleEndian.Uint32(compress[r.IndexSize-4:])
	compressData := compress[:r.IndexSize-4]

	if utils.Checksum(compressData) != crc {
		return nil, fmt.Errorf("数据块校验失败")
	}

	data, err := snappy.Decode(nil, compressData)

	if err != nil {
		return nil, err
	}
	index = ReadIndex(data)

	return
}

func (r *SstReader) ReadBlock(offset, size uint64) (data []byte, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err = r.fd.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	compressed, err := r.Read(int64(size) - 4)

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
			Offset: uint64(offset),
			Size:   uint64(size),
		})
		prevKey = key
	}
	return indexes
}

func ReadFilter(index []byte) map[uint64][]byte {

	data, _ := DecodeBlock(index)
	indexBuf := bytes.NewBuffer(data)

	filterMap := make(map[uint64][]byte, 0)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, indexBuf)

		if err != nil {
			break
		}

		buf := bytes.NewBuffer(key)

		offset, err := binary.ReadUvarint(buf)

		if err != nil {
			log.Println("解析key失败", err)
			continue
		}

		filterMap[offset] = value
		prevKey = key
	}
	return filterMap
}
