package lsm

import (
	"bytes"
	"encoding/binary"
	"io"
	"kvdb/pkg/utils"

	"github.com/golang/snappy"
)

type Block struct {
	conf               *Config
	header             [30]byte
	record             *bytes.Buffer
	trailer            *bytes.Buffer
	nEntries           int
	prevKey            []byte
	compressionScratch []byte
}

func (b *Block) Append(key, value []byte) {
	keyLen := len(key)
	valueLen := len(value)
	nSharePrefix := 0

	// restart point
	if b.nEntries%b.conf.SstRestartInterval == 0 {
		buf4 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf4, uint32(b.record.Len()))
		b.trailer.Write(buf4)
	} else {
		nSharePrefix = SharedPrefixLen(b.prevKey, key)
	}

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

func (b *Block) FlushBlockTo(dest io.Writer) (uint64, error) {
	defer b.clear()

	buf4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf4, uint32(b.trailer.Len())/4)
	b.trailer.Write(buf4)

	n, err := dest.Write(b.compress())
	return uint64(n), err
}

func (b *Block) compress() []byte {

	b.record.Write(b.trailer.Bytes())
	n := snappy.MaxEncodedLen(b.record.Len())

	if n > len(b.compressionScratch) {
		b.compressionScratch = make([]byte, n+b.conf.SstBlockTrailerSize)
	}

	compressed := snappy.Encode(b.compressionScratch, b.record.Bytes())
	crc := utils.Checksum(compressed)

	size := len(compressed)
	compressed = compressed[:size+b.conf.SstBlockTrailerSize]

	binary.LittleEndian.PutUint32(compressed[size:], crc)

	return compressed
}

func (b *Block) clear() {

	b.nEntries = 0
	b.prevKey = b.prevKey[:0]
	b.record.Reset()
	b.trailer.Reset()
}

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
