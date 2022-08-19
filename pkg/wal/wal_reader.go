package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"kvdb/pkg/utils"
	"log"
	"os"
)

const walBlockSize = 32 * 1024

type WalReader struct {
	fd *os.File

	block []byte
	data  []byte

	buf *bytes.Buffer
}

func (r *WalReader) Read() error {
	_, err := io.ReadFull(r.fd, r.block)

	if err != nil {
		return err
	}
	return nil
}

func (r *WalReader) Next() ([]byte, []byte) {

	var prevBlockType uint8
	for r.buf == nil {
		err := r.Read()
		if err != nil {
			return nil, nil
		}

		crc := binary.LittleEndian.Uint32(r.block[0:4])
		length := binary.LittleEndian.Uint16(r.block[4:6])
		blockType := uint8(r.block[6])

		if crc == utils.Checksum(r.block[4:]) {
			switch blockType {
			case kFull:
				r.data = r.block[7 : length+7]
				r.buf = bytes.NewBuffer(r.data)
			case kFirst:
				r.data = r.block[7 : length+7]
			case kMiddle:
				if prevBlockType == kMiddle || prevBlockType == kFirst {
					r.data = append(r.data, r.block[7:length+7]...)
				}
			case kLast:
				if prevBlockType == kMiddle || prevBlockType == kFirst {
					r.data = append(r.data, r.block[7:length+7]...)
					r.buf = bytes.NewBuffer(r.data)
				}
			}
			prevBlockType = blockType
		} else {
			if prevBlockType == kMiddle || prevBlockType == kLast {
				r.buf = bytes.NewBuffer(r.data)
			}
		}
	}

	key, value, err := ReadRecord(r.buf)

	if err == nil {
		return key, value
	}

	r.buf = nil
	return r.Next()

}

func NewWalReader(walFile string) *WalReader {

	fd, err := os.OpenFile(walFile, os.O_RDONLY, 0644)

	if err != nil {
		log.Println("打开文件失败", err)
		return nil
	}

	return &WalReader{
		fd:    fd,
		block: make([]byte, walBlockSize),
	}
}

func ReadRecord(buf *bytes.Buffer) ([]byte, []byte, error) {

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

	return key, value, nil
}
