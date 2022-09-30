package lsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"kvdb/pkg/utils"

	"github.com/golang/snappy"
)

// 块格式 |压缩块|CRC校验(4字节)|
// 未压缩块格式 |记录...|重启点(4字节)...|重启点长度(4字节)|
type Block struct {
	conf               *Config
	header             [30]byte      // 辅助填充 block、record 头
	record             *bytes.Buffer // 记录缓冲
	trailer            *bytes.Buffer // 块尾缓冲
	nEntries           int           // 数据条数
	prevKey            []byte        // 前次键
	compressionScratch []byte        // 压缩缓冲
}

// 追加记录，记录格式： |键共享前缀长度|键剩余长度|值长度|键非共享部分|值|
func (b *Block) Append(key, value []byte) {
	keyLen := len(key)
	valueLen := len(value)
	nSharePrefix := 0

	// 重启点，间隔一定量数据后，重新开始键共享
	if b.nEntries%b.conf.SstRestartInterval == 0 {
		// 重启点用4字节记录键对应偏移
		buf4 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf4, uint32(b.record.Len()))
		b.trailer.Write(buf4)
	} else {
		nSharePrefix = SharedPrefixLen(b.prevKey, key)
	}

	// 按记录格式将记录写入记录缓冲
	n := binary.PutUvarint(b.header[0:], uint64(nSharePrefix))
	n += binary.PutUvarint(b.header[n:], uint64(keyLen-nSharePrefix))
	n += binary.PutUvarint(b.header[n:], uint64(valueLen))

	// data
	b.record.Write(b.header[:n])
	b.record.Write(key[nSharePrefix:])
	b.record.Write(value)

	b.prevKey = append(b.prevKey[:0], key...)
	b.nEntries++
}

// 将当前块写入到目标，返回压缩后字节数
func (b *Block) FlushBlockTo(dest io.Writer) (uint64, error) {
	defer b.clear()

	// 尾最后4字节记录重启点数量
	buf4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf4, uint32(b.trailer.Len())/4)
	b.trailer.Write(buf4)

	n, err := dest.Write(b.compress())
	return uint64(n), err
}

// 压缩数据
func (b *Block) compress() []byte {

	// 将重启点数据写入记录缓冲
	b.record.Write(b.trailer.Bytes())
	// 计算并分配压缩需要空间
	n := snappy.MaxEncodedLen(b.record.Len())
	if n > len(b.compressionScratch) {
		b.compressionScratch = make([]byte, n+b.conf.SstBlockTrailerSize)
	}

	// 压缩记录
	compressed := snappy.Encode(b.compressionScratch, b.record.Bytes())

	// 添加crc检验到块尾
	crc := utils.Checksum(compressed)
	size := len(compressed)
	compressed = compressed[:size+b.conf.SstBlockTrailerSize]
	binary.LittleEndian.PutUint32(compressed[size:], crc)

	return compressed
}

// 清空
func (b *Block) clear() {

	b.nEntries = 0
	b.prevKey = b.prevKey[:0]
	b.record.Reset()
	b.trailer.Reset()
}

// 返回记录及重启点总大小
func (b *Block) Size() int {
	return b.record.Len() + b.trailer.Len() + 4
}

func NewBlock(conf *Config) *Block {

	return &Block{
		record:  bytes.NewBuffer(make([]byte, 0)),
		trailer: bytes.NewBuffer(make([]byte, 0)),
		conf:    conf,
	}

}
