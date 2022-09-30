package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"kvdb/pkg/utils"
	"os"
	"path"
	"sync"

	"github.com/golang/snappy"
)

type SstReader struct {
	mu              sync.RWMutex
	conf            *Config
	fd              *os.File      // sst文件(读)
	reader          *bufio.Reader //包装file reader
	FilterOffset    int64         // 过滤块起始偏移
	FilterSize      int64         // 过滤块大小
	IndexOffset     int64         // 索引块起始偏移
	IndexSize       int64         // 索引块大小
	compressScratch []byte        // 解压缓冲
}

// 索引格式
type Index struct {
	Key    []byte // 分隔键
	Offset uint64 // 前一数据块偏移
	Size   uint64 // 前一数据块大小
}

// 读取sst尾，解析元数据
func (r *SstReader) ReadFooter() error {
	_, err := r.fd.Seek(-int64(r.conf.SstFooterSize), io.SeekEnd)
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
		return fmt.Errorf("sst文件footer数据异常")
	}

	r.FilterOffset = int64(filterOffset)
	r.FilterSize = int64(filterSize)
	r.IndexOffset = int64(indexOffset)
	r.IndexSize = int64(indexSize)
	return nil
}

// 读取过滤块
func (r *SstReader) ReadFilter() (map[uint64][]byte, error) {
	if r.FilterOffset == 0 {
		if err := r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	if _, err := r.fd.Seek(r.FilterOffset, io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	compress, err := r.read(r.FilterSize)
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
	return ReadFilter(data), nil
}

// 读取索引块
func (r *SstReader) ReadIndex() ([]*Index, error) {
	if r.IndexOffset == 0 {
		if err := r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	if _, err := r.fd.Seek(r.IndexOffset, io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	compress, err := r.read(r.IndexSize)
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
	return ReadIndex(data), nil
}

// 读取块
func (r *SstReader) ReadBlock(offset, size uint64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := r.fd.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	compressed, err := r.read(int64(size) - 4)
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

	return snappy.Decode(r.compressScratch, compressed)
}

// 读取指定大小
func (r *SstReader) read(size int64) (b []byte, err error) {
	b = make([]byte, size)
	_, err = io.ReadFull(r.reader, b)
	return
}

// 读取原始字节
func (r *SstReader) Read(offset, size int64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := r.fd.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	r.reader.Reset(r.fd)

	return r.read(size)
}

// 销毁文件
func (r *SstReader) Destory() {
	r.reader.Reset(r.fd)
	r.fd.Close()
	os.Remove(r.fd.Name())
}

// 关闭文件读取
func (r *SstReader) Close() {
	r.reader.Reset(r.fd)
	r.fd.Close()
}

func NewSstReader(file string, conf *Config) (*SstReader, error) {
	fd, err := os.OpenFile(path.Join(conf.Dir, file), os.O_RDONLY, 0644)

	if err != nil {
		return nil, fmt.Errorf("无法加入节点，打开 %s文件失败:%v", file, err)
	}

	return &SstReader{
		fd:     fd,
		conf:   conf,
		reader: bufio.NewReader(fd),
	}, nil
}

// 解析数据块为 记录、重启点
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

// 从记录缓冲读取记录
func ReadRecord(prevKey []byte, buf *bytes.Buffer) ([]byte, []byte, error) {

	keyPrefixLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	keyLen, err := binary.ReadUvarint(buf)

	if err != nil {
		return nil, nil, err
	}

	valueLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	key := make([]byte, keyLen)
	_, err = io.ReadFull(buf, key)
	if err != nil {
		return nil, nil, err
	}

	value := make([]byte, valueLen)
	_, err = io.ReadFull(buf, value)
	if err != nil {
		return nil, nil, err
	}

	actualKey := make([]byte, keyPrefixLen)
	copy(actualKey, prevKey[0:keyPrefixLen])
	actualKey = append(actualKey, key...)

	return actualKey, value, nil
}

// 解析索引块为索引数组
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

		offset, n := binary.Uvarint(value)
		size, _ := binary.Uvarint(value[n:])

		indexes = append(indexes, &Index{
			Key:    key,
			Offset: uint64(offset),
			Size:   uint64(size),
		})
		prevKey = key
	}
	return indexes
}

// 解析过滤块为 偏移->布隆过滤器 map
func ReadFilter(index []byte) map[uint64][]byte {

	data, _ := DecodeBlock(index)
	buf := bytes.NewBuffer(data)

	filterMap := make(map[uint64][]byte, 0)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, buf)

		if err != nil {
			break
		}

		offset, _ := binary.Uvarint(key)
		filterMap[offset] = value
		prevKey = key
	}
	return filterMap
}
