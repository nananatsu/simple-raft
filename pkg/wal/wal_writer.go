package wal

import (
	"bytes"
	"encoding/binary"
	"kvdb/pkg/utils"
	"os"
	"sync"

	"go.uber.org/zap"
)

const (
	kFull = iota
	kFirst
	kMiddle
	kLast
)

type WalWriter struct {
	mu            sync.RWMutex
	fd            *os.File
	header        [20]byte
	buf           *bytes.Buffer
	prevBlockType uint8
	logger        *zap.SugaredLogger
}

func (w *WalWriter) Write(key, value []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := binary.PutUvarint(w.header[0:], uint64(len(key)))
	n += binary.PutUvarint(w.header[n:], uint64(len(value)))
	length := len(key) + len(value) + n

	b := make([]byte, length)
	copy(b, w.header[:n])
	copy(b[n:], key)
	copy(b[n+len(key):], value)

	size := walBlockSize - w.buf.Len()
	if size < length {
		w.buf.Write(b[:size])
		w.PaddingBlock(size-length, false)
		w.buf.Write(b[size:])
	} else {
		w.buf.Write(b)
		w.PaddingBlock(size-length, false)
	}
}

func (w *WalWriter) PaddingBlock(remian int, force bool) {

	var blockType uint8
	if remian < 0 {
		if w.prevBlockType == kFirst || w.prevBlockType == kMiddle {
			blockType = kMiddle
		} else {
			blockType = kFirst
		}
		w.WriteBlock(blockType, uint16(w.buf.Len())-7)
		w.prevBlockType = blockType
	} else if remian < 7 || force {
		w.buf.Write(make([]byte, remian))
		if w.prevBlockType == kFirst || w.prevBlockType == kMiddle {
			blockType = kLast
		} else {
			blockType = kFull
		}
		w.WriteBlock(blockType, uint16(w.buf.Len()-remian-7))
		w.prevBlockType = blockType
	}
}

func (w *WalWriter) PaddingFile() {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, _ := w.fd.Stat()
	n := info.Size() % walBlockSize
	if n > 0 {
		if _, err := w.fd.Write(make([]byte, walBlockSize-n)); err != nil {
			w.logger.Warnf("填充未完成写入文件块失败：%d", err)
		}
	}
}

func (w *WalWriter) WriteBlock(blockType uint8, length uint16) {

	data := w.buf.Bytes()
	binary.LittleEndian.PutUint16(data[4:6], length)
	data[6] = byte(blockType)
	crc := utils.Checksum(data[4:])
	binary.LittleEndian.PutUint32(data[:4], crc)
	w.fd.Write(data)

	w.buf.Truncate(7)
}

func (w *WalWriter) Flush() {
	if w.buf.Len() > 7 {
		w.mu.Lock()
		w.PaddingBlock(walBlockSize-w.buf.Len(), true)
		w.mu.Unlock()
	}
}

func (w *WalWriter) Finish() {
	file := w.fd.Name()
	w.fd.Close()
	os.Remove(file)
}

func NewWalWriter(fd *os.File, logger *zap.SugaredLogger) *WalWriter {
	w := &WalWriter{
		fd:     fd,
		buf:    bytes.NewBuffer(make([]byte, 7)),
		logger: logger,
	}
	return w
}
