package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"kvdb/pkg/filter"
	"os"
	"path"

	"go.uber.org/zap"
)

// 获取共享前缀长度
func SharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// 获取分隔字符串： a <= separator < b
func GetSeparator(a, b []byte) []byte {
	if len(a) == 0 {
		n := len(b) - 1
		c := b[n] - 1
		return append(b[0:n], c)
	}

	n := SharedPrefixLen(a, b)
	if n == 0 || n == len(a) {
		return a
	} else {
		c := a[n] + 1
		return append(a[0:n], c)
	}
}

// sst文件格式 |数据块|过滤块|索引块|尾元数据(定长)|
type SstWriter struct {
	conf            *Config
	fd              *os.File            // sst文件(写)
	dataBuf         *bytes.Buffer       // 数据缓冲
	filterBuf       *bytes.Buffer       // 过滤缓冲, key -> prev data block offset
	indexBuf        *bytes.Buffer       // 索引缓冲, offset->bloom fliter
	index           []*Index            // 索引数组,方便写入sst完成后直接加载到lsm树
	filter          map[uint64][]byte   // 过滤器map,方便写入sst完成后直接加载到lsm树
	bf              *filter.BloomFilter // 布隆过滤器生成
	dataBlock       *Block              // 数据块
	filterBlock     *Block              // 过滤器块
	indexBlock      *Block              // 索引块
	indexScratch    [20]byte            // 辅助byte数组，将uint64作为变长[]byte写入
	prevKey         []byte              // 前次key，生成分隔数据块的索引key
	prevBlockOffset uint64              // 前次数据块偏移, 生成分隔索引
	prevBlockSize   uint64              // 前次数据块大小, 生成分隔索引
	logger          *zap.SugaredLogger
}

// 添加索引到索引缓冲
func (w *SstWriter) addIndex(key []byte) {
	n := binary.PutUvarint(w.indexScratch[0:], w.prevBlockOffset)
	n += binary.PutUvarint(w.indexScratch[n:], w.prevBlockSize)
	separator := GetSeparator(w.prevKey, key)

	w.indexBlock.Append(separator, w.indexScratch[:n])
	// w.logger.Debugf("键 %v 添加索引 %v 偏移 %d 大小 %d ", key, separator, w.prevBlockOffset, w.prevBlockSize)

	w.index = append(w.index, &Index{Key: separator, Offset: w.prevBlockOffset, Size: w.prevBlockSize})
}

// 追加键值对到缓冲
func (w *SstWriter) Append(key, value []byte) {
	// 数据块数据量为0,添加分隔索引
	if w.dataBlock.nEntries == 0 {
		skey := make([]byte, len(key))
		copy(skey, key)
		w.addIndex(skey)
	}

	// 添加数据到数据块、布隆过滤器
	w.dataBlock.Append(key, value)
	w.bf.Add(key)
	// 记录前次key，以便生成分隔索引
	w.prevKey = key

	// 数据块大小超过阈值，打包写入数据缓冲
	if w.dataBlock.Size() > w.conf.SstDataBlockSize {
		w.flushBlock()
	}
}

func (w *SstWriter) flushBlock() {

	var err error
	// 记录当前数据缓冲大小，在下次添加分隔索引时使用
	w.prevBlockOffset = uint64(w.dataBuf.Len())
	n := binary.PutUvarint(w.indexScratch[0:], uint64(w.prevBlockOffset))

	// 生成布隆过滤器Hash，记录到map: 数据块偏移->布隆过滤器
	filter := w.bf.Hash()
	w.filter[w.prevBlockOffset] = filter
	// 添加数据块偏移->布隆过滤器关系到过滤块
	w.filterBlock.Append(w.indexScratch[:n], filter)
	// 重置布隆过滤器
	w.bf.Reset()

	// 将当前数据块写入数据缓冲
	w.prevBlockSize, err = w.dataBlock.FlushBlockTo(w.dataBuf)
	if err != nil {
		w.logger.Errorln("写入data block失败", err)
	}

}

func (w *SstWriter) Finish() (int64, map[uint64][]byte, []*Index) {

	if w.bf.KeyLen() > 0 {
		w.flushBlock()
	}
	// 将过滤块写入过滤缓冲
	if _, err := w.filterBlock.FlushBlockTo(w.filterBuf); err != nil {
		w.logger.Errorln("写入filter block失败", err)
	}

	// 添加分隔索引，将索引块写入索引缓冲
	w.addIndex(w.prevKey)
	if _, err := w.indexBlock.FlushBlockTo(w.indexBuf); err != nil {
		w.logger.Errorln("写入index block失败", err)
	}

	// 生成sst文件footer，记录各部分偏移、大小
	footer := make([]byte, w.conf.SstFooterSize)
	size := w.dataBuf.Len()
	// metadata 索引起始偏移，整体长度
	n := binary.PutUvarint(footer[0:], uint64(size))
	n += binary.PutUvarint(footer[n:], uint64(w.filterBuf.Len()))
	size += w.filterBuf.Len()
	n += binary.PutUvarint(footer[n:], uint64(size))
	n += binary.PutUvarint(footer[n:], uint64(w.indexBuf.Len()))
	size += w.indexBuf.Len()
	size += w.conf.SstFooterSize

	// 将缓冲写入文件
	w.fd.Write(w.dataBuf.Bytes())
	w.fd.Write(w.filterBuf.Bytes())
	w.fd.Write(w.indexBuf.Bytes())
	w.fd.Write(footer)

	// 返回lsm树属性
	return int64(size), w.filter, w.index
}

// 当前数据缓冲大小
func (w *SstWriter) Size() int {
	// return w.dataBuf.Len() + w.indexBuf.Len() + w.bf.Size()
	return w.dataBuf.Len()
}

// 关闭写入
func (w *SstWriter) Close() {
	w.fd.Close()
	w.dataBuf.Reset()
	w.indexBuf.Reset()
}

func NewSstWriter(file string, conf *Config, logger *zap.SugaredLogger) (*SstWriter, error) {
	fd, err := os.OpenFile(path.Join(conf.Dir, file), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("创建 %s 失败: %v", file, err)
	}

	return &SstWriter{
		conf:        conf,
		fd:          fd,
		dataBuf:     bytes.NewBuffer(make([]byte, 0)),
		filterBuf:   bytes.NewBuffer(make([]byte, 0)),
		indexBuf:    bytes.NewBuffer(make([]byte, 0)),
		filter:      make(map[uint64][]byte),
		index:       make([]*Index, 0),
		bf:          filter.NewBloomFilter(10),
		dataBlock:   NewBlock(conf),
		filterBlock: NewBlock(conf),
		indexBlock:  NewBlock(conf),
		prevKey:     make([]byte, 0),
		logger:      logger,
	}, nil
}
