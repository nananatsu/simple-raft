package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"kvdb/pkg/utils"
	"os"

	"go.uber.org/zap"
)

const walBlockSize = 32 * 1024

type WalReader struct {
	fd     *os.File
	block  []byte
	data   []byte
	buf    *bytes.Buffer
	logger *zap.SugaredLogger
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
			if err != io.EOF {
				r.logger.Errorf("读取预写日志块失败:%v", err)
			}
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
				r.data = make([]byte, length)
				copy(r.data, r.block[7:length+7])
			case kMiddle:
				if prevBlockType == kMiddle || prevBlockType == kFirst {
					d := r.block[7 : length+7]
					r.data = append(r.data, d...)
				}
			case kLast:
				if prevBlockType == kMiddle || prevBlockType == kFirst {
					r.data = append(r.data, r.block[7:length+7]...)
					r.buf = bytes.NewBuffer(r.data)
				}
			}
			prevBlockType = blockType
			// r.logger.Debugf("wal块类型 %d, 块记录数据长度； %d, 数据长度： %d ,块: %v", blockType, length, len(r.data), r.data[:20])
		} else {
			r.logger.Debugln("预写日志校验失败")
			if prevBlockType == kMiddle || prevBlockType == kLast {
				r.buf = bytes.NewBuffer(r.data)
			}
		}
	}

	key, value, err := ReadRecord(r.buf, r.logger)
	if err == nil {
		return key, value
	}

	if err != io.EOF {
		r.logger.Errorf("读取预写日志失败", err)
	}

	r.buf = nil
	return r.Next()

}

func (r *WalReader) Close() {
	r.fd.Close()
	r.block = nil
	r.data = nil
}

func NewWalReader(fd *os.File, logger *zap.SugaredLogger) *WalReader {
	return &WalReader{
		fd:     fd,
		block:  make([]byte, walBlockSize),
		logger: logger,
	}
}

func ReadRecord(buf *bytes.Buffer, logger *zap.SugaredLogger) ([]byte, []byte, error) {

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
