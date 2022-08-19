package wal

import (
	"bytes"
	"encoding/binary"
	"kvdb/pkg/utils"
	"log"
	"os"
	"path"
	"strconv"
)

const walBlockDataSize = 32*1024 - 7

const (
	kFull = iota
	kFirst
	kMiddle
	kLast
)

type WalWriter struct {
	fd            *os.File
	header        [20]byte
	blockHeader   [7]byte
	buf           *bytes.Buffer
	prevBlockType uint8
}

func (w *WalWriter) Write(key, value []byte) {

	n := binary.PutUvarint(w.header[0:], uint64(len(key)))
	n += binary.PutUvarint(w.header[n:], uint64(len(value)))
	length := len(key) + len(value) + n

	b := make([]byte, length)
	copy(b, w.header[:n])
	copy(b[n:], key)
	copy(b[n+len(key):], value)

	size := walBlockDataSize - w.buf.Len()

	var blockType uint8
	if size < length {
		w.buf.Write(b[:size])
		if w.prevBlockType == kFirst || w.prevBlockType == kMiddle {
			blockType = kMiddle
		} else {
			blockType = kFirst
		}
		w.WriteBlock(blockType, uint16(w.buf.Len()))
		w.prevBlockType = blockType
		w.buf.Write(b[size:])

	} else {
		w.buf.Write(b)
		remian := size - length
		if remian < 7 {
			w.buf.Write(make([]byte, remian))
			if w.prevBlockType == kFirst || w.prevBlockType == kMiddle {
				blockType = kLast
			} else {
				blockType = kFull
			}
			w.WriteBlock(blockType, uint16(w.buf.Len()-remian))
			w.prevBlockType = blockType
		}
	}
}

func (w *WalWriter) WriteBlock(blockType uint8, length uint16) {

	data := w.buf.Bytes()
	crc := utils.Checksum(data)

	binary.LittleEndian.PutUint32(w.blockHeader[:4], crc)
	binary.LittleEndian.PutUint16(w.blockHeader[4:6], length)
	w.blockHeader[6] = byte(blockType)

	w.fd.Write(w.blockHeader[:7])
	w.fd.Write(data)
	w.buf.Reset()

}

func (w *WalWriter) Finish() {
	file := w.fd.Name()
	w.fd.Close()
	os.Remove(file)
}

func NewWalWriter(dir string, seqNo int) *WalWriter {

	fd, err := os.OpenFile(path.Join(dir, strconv.Itoa(0)+"."+strconv.Itoa(seqNo)+".wal"), os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		log.Println("打开预写日志文件失败", err)
	}

	return &WalWriter{
		fd:  fd,
		buf: bytes.NewBuffer(make([]byte, 0)),
	}
}
