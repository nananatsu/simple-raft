package raft

import (
	"encoding/binary"
	"kvdb/pkg/clientpb"
	pb "kvdb/pkg/raftpb"
	"kvdb/pkg/skiplist"

	"google.golang.org/protobuf/proto"
)

// 存储键前缀，区分集群成员变更数据，以便自快照恢复集群状态
const (
	MEMBER_PREFIX   = uint8(0)
	DEFAULT_PREFIX  = uint8(1)
	PREFIX_SPLITTER = byte('_')
)

type Encoding interface {
	// 编码日志索引
	EncodeIndex(index uint64) []byte
	// 解码日志索引
	DecodeIndex(key []byte) uint64
	// 编码日志条目
	EncodeLogEntry(entry *pb.LogEntry) ([]byte, []byte)
	// 解码日志条目
	DecodeLogEntry(key, value []byte) *pb.LogEntry
	// 批量解码日志条目(raft log -> kv  )
	DecodeLogEntries(logEntry *skiplist.SkipList) (*skiplist.SkipList, uint64, uint64)
	// 编码日志条目键值对
	EncodeLogEntryData(key, value []byte) []byte
	// 解码日志条目键值对
	DecodeLogEntryData(entry []byte) ([]byte, []byte)
	// 添加默认前缀
	DefaultPrefix(key []byte) []byte
	// 添加集群成员前缀
	MemberPrefix(key []byte) []byte
}

// 日志简单编解码
type SimpleEncoding struct {
	scratch [20]byte
}

func (se *SimpleEncoding) DefaultPrefix(key []byte) []byte {
	prefix := []byte{DEFAULT_PREFIX, PREFIX_SPLITTER}
	return append(prefix, key...)
}

func (se *SimpleEncoding) MemberPrefix(key []byte) []byte {
	prefix := []byte{MEMBER_PREFIX, PREFIX_SPLITTER}
	return append(prefix, key...)
}

// 将日志索引按大端序编码字节数组,按字节排序时有序
func (se *SimpleEncoding) EncodeIndex(index uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, index)
	return b
}

func (se *SimpleEncoding) DecodeIndex(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}

// 键: 大端日志索引 , 值： |日志类型(1字节)|任期|实际数据|
func (se *SimpleEncoding) EncodeLogEntry(entry *pb.LogEntry) ([]byte, []byte) {
	binary.BigEndian.PutUint64(se.scratch[0:], entry.Index)
	se.scratch[8] = uint8(entry.Type)
	n := binary.PutUvarint(se.scratch[9:], entry.Term)

	return se.scratch[:8], append(se.scratch[8:8+n+1], entry.Data...)
}

func (se *SimpleEncoding) DecodeLogEntry(key, value []byte) *pb.LogEntry {
	entryType := uint8(value[0])
	term, n := binary.Uvarint(value[1:])
	index := binary.BigEndian.Uint64(key)
	return &pb.LogEntry{Type: pb.EntryType(entryType), Index: index, Term: term, Data: value[n+1:]}
}

// |键长度|值长度|键|值|
func (se *SimpleEncoding) EncodeLogEntryData(key, value []byte) []byte {
	header := make([]byte, 20)

	n := binary.PutUvarint(header[0:], uint64(len(key)))
	n += binary.PutUvarint(header[n:], uint64(len(value)))
	length := len(key) + len(value) + n

	b := make([]byte, length)
	copy(b, header[:n])
	copy(b[n:], key)
	copy(b[n+len(key):], value)

	return b
}

func (se *SimpleEncoding) DecodeLogEntryData(entry []byte) ([]byte, []byte) {
	var n int
	keyLen, m := binary.Uvarint(entry[n:])
	n += m
	valueLen, m := binary.Uvarint(entry[n:])
	n += m
	return entry[n : n+int(keyLen)], entry[n+int(keyLen) : n+int(keyLen)+int(valueLen)]
}

// 解析raft log为原kv
func (se *SimpleEncoding) DecodeLogEntries(logEntry *skiplist.SkipList) (*skiplist.SkipList, uint64, uint64) {

	if logEntry.Size() == 0 {
		return skiplist.NewSkipList(), 0, 0
	}

	k, v := logEntry.GetMax()
	index := binary.BigEndian.Uint64(k)
	term, _ := binary.Uvarint(v[1:])

	kvSL := skiplist.NewSkipList()
	it := skiplist.NewSkipListIter(logEntry)
	for it.Next() {
		_, n := binary.Uvarint(it.Value[1:])
		if pb.EntryType(uint8(it.Value[0])) == pb.EntryType_NORMAL {
			kvSL.Put(se.DecodeLogEntryData(it.Value[n+1:]))
		} else {
			kvSL.Put(se.MemberPrefix(it.Key), it.Value[n+1:])
		}
	}
	return kvSL, index, term
}

// 日志通过protobuf序列化/反序列化
type ProtobufEncoding struct {
}

func (pe *ProtobufEncoding) DefaultPrefix(key []byte) []byte {
	prefix := []byte{DEFAULT_PREFIX, PREFIX_SPLITTER}
	return append(prefix, key...)
}

func (pe *ProtobufEncoding) MemberPrefix(key []byte) []byte {
	prefix := []byte{MEMBER_PREFIX, PREFIX_SPLITTER}
	return append(prefix, key...)
}

func (pe *ProtobufEncoding) EncodeIndex(index uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, index)
	return b
}

func (pe *ProtobufEncoding) DecodeIndex(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}

func (pe *ProtobufEncoding) EncodeLogEntry(entry *pb.LogEntry) ([]byte, []byte) {
	data, _ := proto.Marshal(entry)
	return pe.EncodeIndex(entry.Index), data
}

func (pe *ProtobufEncoding) DecodeLogEntry(key, value []byte) *pb.LogEntry {
	var entry pb.LogEntry
	proto.Unmarshal(value, &entry)
	return &entry
}

// 解析raft log为原kv
func (pe *ProtobufEncoding) DecodeLogEntries(logEntry *skiplist.SkipList) (*skiplist.SkipList, uint64, uint64) {
	var index uint64
	var term uint64

	if logEntry.Size() == 0 {
		return skiplist.NewSkipList(), index, term
	}

	k, v := logEntry.GetMax()
	max := pe.DecodeLogEntry(k, v)
	index = max.Index
	term = max.Term

	kvSL := skiplist.NewSkipList()
	it := skiplist.NewSkipListIter(logEntry)

	for it.Next() {
		entry := pe.DecodeLogEntry(it.Key, it.Value)
		if entry.Type == pb.EntryType_NORMAL {
			key, value := pe.DecodeLogEntryData(entry.Data)
			if key != nil {
				kvSL.Put(key, value)
			} else {
				kvSL.Put(pe.MemberPrefix(it.Key), it.Value)
			}
		}
	}
	return kvSL, index, term
}

func (pe *ProtobufEncoding) EncodeLogEntryData(key, value []byte) []byte {
	data, _ := proto.Marshal(&clientpb.KvPair{Key: key, Value: value})
	return data
}

func (pe *ProtobufEncoding) DecodeLogEntryData(entry []byte) ([]byte, []byte) {
	var pair clientpb.KvPair
	proto.Unmarshal(entry, &pair)
	return pair.Key, pair.Value
}
