package raft

import (
	"fmt"
	"io/fs"
	"kvdb/pkg/lsm"
	pb "kvdb/pkg/raftpb"
	"kvdb/pkg/skiplist"
	"kvdb/pkg/sql"
	"kvdb/pkg/utils"
	"kvdb/pkg/wal"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// 数据生成快照阈值
const LOG_SNAPSHOT_SIZE = 32 * 1024 * 1024
const WAL_FLUSH_INTERVAL = 10 * time.Second

type Storage interface {
	Append(entries []*pb.LogEntry)
	MakeSnapshot(force bool)
	InstallSnapshot(snap *pb.Snapshot) (bool, error)
	GetEntries(startIndex, endIndex uint64) []*pb.LogEntry
	GetSnapshot(index uint64) (chan *pb.Snapshot, error)
	GetTerm(index uint64) uint64
	GetLastLogIndexAndTerm() (uint64, uint64)
	NotifyChan() chan []*pb.MemberChange
	Close()
}

type RaftStorage struct {
	encoding            Encoding                // 日志编解码
	walw                *wal.WalWriter          // 预写日志
	logEntries          *skiplist.SkipList      // raft 日志
	logState            *skiplist.SkipList      // kv 数据
	immutableLogEntries *skiplist.SkipList      // 上次/待写入快照 raft日志
	immutableLogState   *skiplist.SkipList      // 上次/待写入快照 kv 数据
	snap                *Snapshot               // 快照实例
	notifyc             chan []*pb.MemberChange // 变更提交通知通道
	stopc               chan struct{}           // 停止通道
	logger              *zap.SugaredLogger
}

// 返回通知通道
func (rs *RaftStorage) NotifyChan() chan []*pb.MemberChange {
	return rs.notifyc
}

// 追加日志到存储
func (rs *RaftStorage) Append(entries []*pb.LogEntry) {
	for _, entry := range entries {
		logKey, logValue := rs.encoding.EncodeLogEntry(entry)
		rs.walw.Write(logKey, logValue)
		rs.logEntries.Put(logKey, logValue)

		if entry.Type == pb.EntryType_MEMBER_CHNAGE {
			var changeCol pb.MemberChangeCol
			err := proto.Unmarshal(entry.Data, &changeCol)
			if err != nil {
				rs.logger.Warnf("解析成员变更日志失败: %v", err)
			}
			rs.logState.Put(rs.encoding.MemberPrefix(logKey), entry.Data)
			// 成员变更提交需通知外部
			rs.notifyc <- changeCol.Changes
		} else {
			k, v := rs.encoding.DecodeLogEntryData(entry.Data)
			if k != nil {
				rs.logState.Put(k, v)
			}
		}
	}

	rs.MakeSnapshot(false)
}

// 生成快照
func (rs *RaftStorage) MakeSnapshot(force bool) {
	if rs.logState.Size() > LOG_SNAPSHOT_SIZE || (force && rs.logState.Size() > 0) {
		oldWalw := rs.walw
		walw, err := rs.walw.Next()
		if err != nil {
			oldWalw = nil
			rs.logger.Errorf("新建预写日志失败: %v", err)
		} else {
			rs.walw = walw
		}

		rs.immutableLogEntries = rs.logEntries
		rs.immutableLogState = rs.logState
		rs.logEntries = skiplist.NewSkipList()
		rs.logState = skiplist.NewSkipList()

		go func(w *wal.WalWriter, logState *skiplist.SkipList, logEntries *skiplist.SkipList) {
			k, v := logEntries.GetMax()
			entry := rs.encoding.DecodeLogEntry(k, v)

			rs.snap.MakeSnapshot(logState, entry.Index, entry.Term)
			if oldWalw != nil {
				oldWalw.Finish()
			}
		}(oldWalw, rs.immutableLogState, rs.immutableLogEntries)
	}
}

// 获取日志任期
func (rs *RaftStorage) GetTerm(index uint64) (term uint64) {
	if index == rs.snap.lastIncludeIndex {
		return rs.snap.lastIncludeTerm
	}

	key := rs.encoding.EncodeIndex(index)
	value := rs.logEntries.Get(key)

	if value == nil {
		return
	}

	return rs.encoding.DecodeLogEntry(key, value).Term
}

// 获取最后提交日志、任期
func (rs *RaftStorage) GetLastLogIndexAndTerm() (uint64, uint64) {
	k, v := rs.logEntries.GetMax()
	if len(k) > 0 {
		entry := rs.encoding.DecodeLogEntry(k, v)
		return entry.Index, entry.Term
	}
	return rs.snap.lastIncludeIndex, rs.snap.lastIncludeTerm
}

// 获取区间: [)日志
func (rs *RaftStorage) GetEntries(startIndex, endIndex uint64) []*pb.LogEntry {

	if startIndex < rs.snap.lastIncludeIndex {
		rs.logger.Infof("日志 %d 已压缩到快照: %d", startIndex, rs.snap.lastIncludeIndex)
		return nil
	}

	startByte := rs.encoding.EncodeIndex(startIndex)
	endByte := rs.encoding.EncodeIndex(endIndex)

	kvs := rs.logEntries.GetRange(startByte, endByte)
	ret := make([]*pb.LogEntry, len(kvs))
	for i, kv := range kvs {
		ret[i] = rs.encoding.DecodeLogEntry(kv.Key, kv.Value)
	}
	return ret
}

func (rs *RaftStorage) GetValue(key []byte) []byte {

	value := rs.logState.Get(key)

	if value == nil && rs.immutableLogState != nil {
		value = rs.immutableLogState.Get(key)
	}

	if value == nil {
		value = rs.snap.data.Get(key)
	}
	return value
}

func (rs *RaftStorage) GetRange(start, end []byte, filter func([]byte, []byte) (bool, error)) error {

	complete, err := rs.logState.GetRangeWithFilter(start, end, filter)
	if err != nil || complete {
		return err
	}

	if rs.immutableLogState != nil {
		complete, err = rs.immutableLogState.GetRangeWithFilter(start, end, filter)
		if err != nil || complete {
			return err
		}
	}

	_, err = rs.snap.data.GetRangeWithFilter(start, end, filter)

	return err
}

// 获取快照发送
func (rs *RaftStorage) GetSnapshot(index uint64) (chan *pb.Snapshot, error) {
	return rs.snap.GetSegment(index)
}

// 安装快照
func (rs *RaftStorage) InstallSnapshot(snap *pb.Snapshot) (bool, error) {
	if rs.logState.Size() > 0 {
		rs.MakeSnapshot(true)
	}
	return rs.snap.AddSnapshotSegment(snap)
}

// 关闭
func (rs *RaftStorage) Close() {

	rs.stopc <- struct{}{}
	rs.snap.Close()
	close(rs.notifyc)
}

// 定时预写日志刷新
func (rs *RaftStorage) checkFlush() {
	go func() {
		ticker := time.NewTicker(WAL_FLUSH_INTERVAL)
		for {
			select {
			case <-ticker.C:
				rs.walw.Flush()
			case <-rs.stopc:
				rs.walw.Flush()
				return
			}
		}
	}()
}

// 还原集群成员
func (rs *RaftStorage) RestoreMember() (map[uint64]string, error) {

	start := []byte{MEMBER_PREFIX, PREFIX_SPLITTER}
	end := []byte{MEMBER_PREFIX, PREFIX_SPLITTER + 1}

	memnbers := make(map[uint64]string)
	changes := rs.snap.data.GetRange(start, end)
	changes = append(changes, rs.logState.GetRange(start, end)...)

	for _, v := range changes {
		var changes pb.MemberChangeCol
		err := proto.Unmarshal(v.Value, &changes)
		if err != nil {
			return memnbers, fmt.Errorf("恢复集群变更失败: %v", err)
		}

		if len(changes.Changes) > 0 {
			for _, mc := range changes.Changes {
				if mc.Type == pb.MemberChangeType_ADD_NODE {
					memnbers[mc.Id] = mc.Address
				} else if mc.Type == pb.MemberChangeType_REMOVE_NODE {
					delete(memnbers, mc.Id)
				}
			}
		}
	}
	return memnbers, nil
}

// 新建raft存储
func NewRaftStorage(dir string, encoding Encoding, logger *zap.SugaredLogger) *RaftStorage {

	// 保证文件夹存在
	if _, err := os.Stat(dir); err != nil {
		os.Mkdir(dir, os.ModePerm)
	}

	walDir := path.Join(dir, "wal")
	if _, err := os.Stat(walDir); err != nil {
		os.Mkdir(walDir, os.ModePerm)
	}

	memLogs := make(map[int]*skiplist.SkipList, 1)
	wals := *new(sort.IntSlice)

	// 文件处理回调
	callbacks := []func(string, fs.FileInfo){
		func(name string, fileInfo fs.FileInfo) {
			info := strings.Split(name, ".")
			if len(info) != 2 {
				return
			}

			seqNo, err := strconv.Atoi(info[0])
			if err != nil {
				return
			}

			// 文件为wal类型时，尝试还原日志
			if info[1] == "wal" {
				file := path.Join(walDir, strconv.Itoa(seqNo)+".wal")
				db, err := wal.Restore(file)
				if err != nil {
					logger.Errorf("还原 %s 失败:%v", file, err)
				}

				if db != nil {
					wals = append(wals, seqNo)
					memLogs[seqNo] = db
				}
			}
		},
	}

	// 扫描文件夹，执行回调
	if err := utils.CheckDir(walDir, callbacks); err != nil {
		logger.Errorf("打开db文件夹 %s 失败: %v", walDir, err)
	}

	var logEntries *skiplist.SkipList
	var logState *skiplist.SkipList
	var seq int
	// 重新排序预写日志序号
	wals.Sort()
	// 取最新序号预写日志继续使用
	if wals.Len() > 0 {
		seq = wals[wals.Len()-1]
		logEntries = memLogs[seq]
		delete(memLogs, seq)
	}
	if logEntries == nil {
		logEntries = skiplist.NewSkipList()
	}

	// 从raft日志还原实际数据
	logState, _, _ = encoding.DecodeLogEntries(logEntries)

	// 打开预写日志wal
	w, err := wal.NewWalWriter(walDir, seq, logger)
	if err != nil {
		logger.Errorf("创建wal writer失败: %v", walDir, err)
	}

	snapConf := lsm.NewConfig(path.Join(dir, "snapshot"), logger)
	snapConf.SstSize = LOG_SNAPSHOT_SIZE

	// 从文件夹恢复快照状态
	snap, err := NewSnapshot(snapConf)
	if err != nil {
		logger.Errorf("读取快照失败", err)
	}

	s := &RaftStorage{
		walw:       w,
		logEntries: logEntries,
		logState:   logState,
		snap:       snap,
		notifyc:    make(chan []*pb.MemberChange),
		stopc:      make(chan struct{}),
		encoding:   encoding,
		logger:     logger,
	}

	lastIndex, lastTerm := s.GetLastLogIndexAndTerm()
	logger.Infof("存储最后日志 %d 任期 %d ,快照最后日志 %d 任期 %d ", lastIndex, lastTerm, snap.lastIncludeIndex, snap.lastIncludeTerm)

	// 将旧预写日志更新到快照
	for seq, logEntry := range memLogs {
		s.snap.MakeSnapshot(encoding.DecodeLogEntries(logEntry))
		os.Remove(path.Join(walDir, strconv.Itoa(seq)+".wal"))
	}

	// 定时刷新预写日志
	s.checkFlush()

	return s
}

type Calculator struct {
	Filter []sql.BoolExpr
	Field  []int
	Offset int
	Size   int
}
