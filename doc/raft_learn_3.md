## 用go实现Raft(3) - 日志压缩篇
---

Raft中如直接持久化日志，在运行期间日志会无增长，于是需要一些机制来丢弃过时的日志，论文中有提单两种方式：
- 快照，将当前系统状态写入磁盘，再清理已快照状态对应的日志；
- 渐进使日志压缩，一次对部分数据进行处理，将负载在时间上均匀分布，如LSM Tree；

在之前 LSM Tree的三篇文章，我们简单实现了LSM Tree，这里我们使用LSM Tree来实现Raft的日志压缩，实现时使用与快照相同的接口、术语。
- [用go实现LSM Tree (1) - SSTable](./doc/lsm_tree_1.md)
- [用go实现LSM Tree (2) - memtable & wal](./doc/lsm_tree_2.md)
- [用go实现LSM Tree (2) - compaction](./doc/lsm_tree_3.md)

Raft要存储的数据可分为两个部分：
- 日志，顺序写入磁盘，日志更新到状态机后可删除；
- 日志的执行结果，节点按顺序执行日志，执行结果是一个状态机；

对照LSM Tree我们将日志通过预写式日志方式写入磁盘，将日志执行结果写入LSM Tree将整棵树作为Raft的状态机，LSM Tree包含了合并压缩机制，从而在Raft不需要再额外实现压缩。

为将数据写入LSM Tree，日志的内容需要可以被解析为键值对形式，为方便起见，客户端的提案内容就是键值对，这样最终实现的Raft Server会是一个键值对数据库。

定义RaftStorage如下：
```go
type RaftStorage struct {
	encoding            Encoding                // 日志编解码
	walw                *wal.WalWriter          // 预写日志
	logEntries          *skiplist.SkipList      // raft 日志
	logState            *skiplist.SkipList      // kv 数据
	immutableLogEntries *skiplist.SkipList      // 上次/待写入快照 raft日志
	immutableLogState   *skiplist.SkipList      // 上次/待写入快照 kv 数据
	snap                *Snapshot               // 快照实例
	stopc               chan struct{}           // 停止通道
	logger              *zap.SugaredLogger
}
```
实现日志持久化方法，raftlog在提交日志时取得日志内容，写入LSM Tree。
- 遍历日志，首先将日志记录编码写入WAL，再将日志解析为原始的键值对写入LSM Tree。
	- 在LSM Tree中我们实现的WAL Writer时按键值对写入的，将日志编号作为键，日志作为值写入；
	- 日志pb.LogEntry是protobuf定义的，可直接使用protobuf序列化为二进制；
- 最后检查内存中日志是否达到阈值，是否需要压缩快照。
```go
func (rs *RaftStorage) Append(entries []*pb.LogEntry) {
	for _, entry := range entries {
		logKey, logValue := rs.encoding.EncodeLogEntry(entry)
		rs.walw.Write(logKey, logValue)
		rs.logEntries.Put(logKey, logValue)

		k, v := rs.encoding.DecodeLogEntryData(entry.Data)
		if k != nil {
			rs.logState.Put(k, v)
		}
	}
	rs.MakeSnapshot(false)
}
```
快照对应到LSM Tree为memtable的minor compaction，将数据持久化。
- 我们将Raft中的快照与LSM Tree的compaction视为同一机制，SStable文件为快照文件。
- leveldb(LSM Tree实现)中的快照机制是提供一致性视图，与此处的快照不同。
```go
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

func (ss *Snapshot) MakeSnapshot(logState *skiplist.SkipList, lastIndex, lastTerm uint64) {
	ss.data.FlushRecord(skiplist.NewSkipListIter(logState), fmt.Sprintf("%s@%s", strconv.FormatUint(lastIndex, 16), strconv.FormatUint(lastTerm, 16)))
	ss.lastIncludeIndex = lastIndex
	ss.lastIncludeTerm = lastTerm
}
```
加入LSM Tree后，我们的日志只有一部分能够被读取到，这些日志分别在保存在：raftlog、memtable中，添加从LSM Tree memtable读取日志方法。
```go
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
```
如在这raftlog、memtable没有找到日志，则表明日志在被持久化到状态机后被清除，状态机中数据已是日志内容执行后的结果，无法取到原始日志，这时leader的无法向follower追加日志，我们需要将快照直接发送给follower。
添加一个新的RPC请求InstallSnashot，定义快照结构如下：
- 快照发送时，每次发送一部分给follower(便于发送失败重发)，folower自行将各部分拼接为原始文件。
```protobuf
enum MessageType {
  ...
  INSTALL_SNAPSHOT = 8;
  INSTALL_SNAPSHOT_RESP = 9;
  ...
}

message Snapshot {
  uint64 lastIncludeIndex = 1;
  uint64 lastIncludeTerm = 2;
  uint32 level = 3;
  uint32 segment = 4;
  uint64 offset = 5;
  bytes data = 6;
  bool done = 7;
}

message RaftMessage {
  ···
  Snapshot snapshot = 10;
}
```
在同步进度中加入快照信息，以便记录快照发送进度方便重发、发送下一部分。
```go
type ReplicaProgress struct {
	...
	snapc              chan *pb.Snapshot // 快照读取通道
	prevSnap           *pb.Snapshot      // 上次发送快照
	maybePrevSnapLost  *pb.Snapshot      // 可能丢失快照,标记上次发送未完成以重发
}
```
在leader发送日志到其他节点时检查是否需要发送快照。
- 当无法从内存中的raftlog、memtable取到待发送日志记录，即日志已被删除时，只能通过快照发送follower请求数据，依据follower需求日志编号找到对应快照文件，逐个读取发送；
	- 为方便读取文件内容，用额外协程遍历读取文件，raft中通过通道接收读取到的数据，一部分发送完成后再接收下一部分；
- 发送快照时，标记该follower正在发送快照，暂停一般日志发送，直到快照发送完成，再继续进行正常日志同步；
```go
func (r *Raft) SendAppendEntries(to uint64) {
	...
	entries := r.raftlog.GetEntries(nextIndex, maxSize)
	size := len(entries)
	if size == 0 {
		if nextIndex <= r.raftlog.lastAppliedIndex && p.prevResp {
			snapc, err := r.raftlog.GetSnapshot(nextIndex)
			if err != nil {
				r.logger.Errorf("获取快照失败: %v", err)
				return
			}
			r.cluster.InstallSnapshot(to, snapc)
			r.sendSnapshot(to, true)
			return
		}
	} else {
		r.cluster.AppendEntry(to, entries[size-1].Index)
	}
	...
}

func (rp *ReplicaProgress) IsPause() bool {
	return rp.installingSnapshot || (!rp.prevResp && len(rp.pending) > 0)
}

func (l *RaftLog) GetSnapshot(index uint64) (chan *pb.Snapshot, error) {
	return l.storage.GetSnapshot(index)
}
```
添加发送快照方法，快照发送与日志消息发送过程并无不同，将快照包装成RaftMessage使用send方法发出。
- 每次发送快照检查上次快照发送是否完成
	- 如已完成，从快照通道读取一次数据并保存，发送读取到的数据；
	- 如未完成，发送缓存的快照数据
```go
func (r *Raft) sendSnapshot(to uint64, prevSuccess bool) {
	snap := r.cluster.GetSnapshot(to, prevSuccess)
	if snap == nil {
		r.SendAppendEntries(to)
		return
	}
	msg := &pb.RaftMessage{
		MsgType:  pb.MessageType_INSTALL_SNAPSHOT,
		Term:     r.currentTerm,
		From:     r.id,
		To:       to,
		Snapshot: snap,
	}
	r.Msg = append(r.Msg, msg)
}

func (c *Cluster) GetSnapshot(id uint64, prevSuccess bool) *pb.Snapshot {
	p := c.progress[id]
	if p != nil {
		return p.GetSnapshot(prevSuccess)
	} else {
		c.logger.Debugf("%s 未初始化完成，无法发送快照", strconv.FormatUint(id, 16))
	}
	return nil
}

func (rp *ReplicaProgress) GetSnapshot(prevSuccess bool) *pb.Snapshot {
	if !prevSuccess {
		return rp.prevSnap
	}

	if rp.snapc == nil {
		return nil
	}
	snap := <-rp.snapc
	if snap == nil {
		rp.snapc = nil
		rp.installingSnapshot = false
	}
	rp.prevSnap = snap
	return snap
}
```
为防止快照发送丢失，在leader发送心跳时检查快照发送状态，一直未收到快照响应便重发快照。
```go
func (r *Raft) TickHeartbeat() {
	...
	if r.hearbeatTick >= r.heartbeatTimeout {
		r.hearbeatTick = 0
		r.BroadcastHeartbeat(nil)
		r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
			...
			// 重发快照,条件：上次快照在两次心跳内未发送完成
			if p.installingSnapshot && p.prevSnap != nil && p.MaybeSnapLost(p.prevSnap) {
				r.logger.Debugf("重发 %d_%s@%d_%d 偏移 %d", p.prevSnap.Level, strconv.FormatUint(p.prevSnap.LastIncludeIndex, 16), p.prevSnap.LastIncludeTerm, p.prevSnap.Segment, p.prevSnap.Offset)
				r.sendSnapshot(id, false)
			}
		})
	}
}
```
实现快照处理方法,将快照写入到磁盘，响应leader已收到快照。
- LSM Tree已有一些SSTable，各节点各自进行快照/压缩，导致个节点各快照文件含有键值对数据不一致，在将新快照加入树时，提交所有未提交日志，对已存在SStable进行压缩合并，使接收到的快照在更小层以覆盖重复范围、保证数据可见性与leader一致。
- 在快照添加后，加载快照数据，更新最新日志信息。
```go
func (r *Raft) ReciveInstallSnapshot(from, term uint64, snap *pb.Snapshot) {
	var installed bool
	if snap.LastIncludeIndex > r.raftlog.lastAppliedIndex {
		installed, _ = r.raftlog.InstallSnapshot(snap)
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_INSTALL_SNAPSHOT_RESP,
		Term:         r.currentTerm,
		From:         r.id,
		To:           from,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      installed,
	})
}

func (l *RaftLog) InstallSnapshot(snap *pb.Snapshot) (bool, error) {
	// 当前日志未提交,强制提交并更新快照
	if len(l.logEnties) > 0 {
		l.Apply(l.lastAppendIndex, l.lastAppendIndex)
	}
	// 添加快照到存储
	added, err := l.storage.InstallSnapshot(snap)
	if added { // 添加完成,更新最后提交
		l.ReloadSnapshot()
	}
	return added, err
}

func (l *RaftLog) ReloadSnapshot() {
	lastIndex, lastTerm := l.storage.GetLastLogIndexAndTerm()
	if lastIndex > l.lastAppliedIndex {
		l.lastAppliedIndex = lastIndex
		l.lastAppliedTerm = lastTerm
	}
}
```
在follower消息处理中加入快照处理。
```go
func (r *Raft) HandleFollowerMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	...
	case pb.MessageType_INSTALL_SNAPSHOT:
		r.ReciveInstallSnapshot(msg.From, msg.Term, msg.Snapshot)
	...
	}
}
```
实现快照响应处理，收到响应后更新快照发送进度，继续发送快照下一部分。
```go
func (r *Raft) ReciveInstallSnapshotResult(from, term, lastLogIndex uint64, installed bool) {
	if installed {
		leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
		r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)
		r.logger.Debugf("%s 快照更新 ,当前最后日志 %d ", strconv.FormatUint(from, 16), lastLogIndex)
	}
	r.sendSnapshot(from, true)

}
```
在leader消息处理中加入快照响应处理。
```go
func (r *Raft) HandleLeaderMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	...
	case pb.MessageType_INSTALL_SNAPSHOT_RESP:
		r.ReciveInstallSnapshotResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	...
	}
}
```
添加RaftStorage新建函数,读取WAL还原日志，启动协程定时写入WAL。
```go
func NewRaftStorage(dir string, encoding Encoding, logger *zap.SugaredLogger) *RaftStorage {
	// 保证文件夹存在
	if _, err := os.Stat(dir); err != nil {
		os.Mkdir(dir, os.ModePerm)
	}

	snapConf := lsm.NewConfig(path.Join(dir, "snapshot"), logger)
	snapConf.SstSize = LOG_SNAPSHOT_SIZE

	// 从文件夹恢复快照状态
	snap, err := NewSnapshot(snapConf)
	if err != nil {
		logger.Errorf("读取快照失败", err)
	}

	// 从raft日志还原实际数据
	logEntries, w := restoreLogEntries(dir, encoding, snap, logger)
	logState, _, _ := encoding.DecodeLogEntries(logEntries)

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
	// 定时刷新预写日志
	s.checkFlush()

	return s
}

func restoreLogEntries(dir string, encoding Encoding, snap *Snapshot, logger *zap.SugaredLogger) (*skiplist.SkipList, *wal.WalWriter) {
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

	// 打开预写日志wal
	w, err := wal.NewWalWriter(walDir, seq, logger)
	if err != nil {
		logger.Errorf("创建wal writer失败: %v", walDir, err)
	}

	// 将旧预写日志更新到快照
	for seq, logEntry := range memLogs {
		snap.MakeSnapshot(encoding.DecodeLogEntries(logEntry))
		os.Remove(path.Join(walDir, strconv.Itoa(seq)+".wal"))
	}
	return logEntries, w
}
```
在启动节点时加入RaftSorage。
```go
func Bootstrap(conf *Config) *RaftServer {
	dir := path.Join(conf.Dir, conf.Name)
	storage := raft.NewRaftStorage(dir, &raft.SimpleEncoding{}, conf.Logger)
	...

	if len(peers) != 0 {
	...
		node = raft.NewRaftNode(nodeId, storage, peers, conf.Logger)
	...
	}
	...
	server := &RaftServer{
		...
		storage:       storage,
		...
	}
	return server
}
```

### KVDB Server实现
---
定义grpc服务，添加键值对查询、添加、删除，服务器注册接口。
- 连接到服务需要提供 token
- 查询时需要提供 key
- 添加时需要提供 key、value
- 删除时需要提供 key
- 添加/删除会对状态机造成影响，再raft中需进行提案，封装为Command
```protobuf
service Kvdb {
  rpc register(Auth) returns (Response) {}
  rpc get(ReadonlyQuery) returns (Response) {}
  rpc put(Request) returns (Response) {}
  rpc delete (Request) returns (Response) {}
}

message Auth { string token = 1; }

message ReadonlyQuery { bytes key = 1; }

message PutCommand { repeated KvPair data = 1; }

message DeleteCommand { repeated bytes keys = 1; }

message KvPair {
  bytes key = 1;
  bytes value = 2;
}

enum Operate {
  PUT = 0;
  DELETE = 1;
}

message Command {
  Operate operateType = 1;
  PutCommand put = 2;
  DeleteCommand del = 3;
}
```
定义请求、响应
- 请求含： 客户端编号、命令序列号、请求体（查询请求、需执行命令）
- 响应含： 请求状态、信息、键值对(查询返回)、客户端编号(注册返回)
```protobuf
message Request {
  uint64 clientId = 1;
  uint64 seq = 2;
  Command cmd = 3;
  ReadonlyQuery query = 4;
}

message Response {
  bool success = 1;
  repeated KvPair data = 3;
  uint64 clientId = 2;
  string msg = 4;
}
```
使Raft Server继承KvdbServer接口
```go
type RaftServer struct {
	pb.RaftServer
	clientpb.KvdbServer
	...
}
```
实现添加键值对到Raft集群方法
- 从客户端得到键值对后需要提案到raft集群中，需要将键值对再进行编码，提案在集群达成共识后，再解码为原始键值对写入LSM Tree。
```go
func (s *RaftServer) Put(ctx context.Context, req *clientpb.Request) (*clientpb.Response, error) {
	if !s.node.Ready() {
		return &clientpb.Response{Success: false}, fmt.Errorf("集群未就绪")
	}
	for _, kp := range req.Cmd.Put.Data {
		err := s.put(kp.Key, kp.Value)
		if err != nil {
			return &clientpb.Response{Success: false}, err
		}
	}
	return &clientpb.Response{Success: true}, nil
}

func (s *RaftServer) put(key, value []byte) error {
	data := s.encoding.EncodeLogEntryData(s.encoding.DefaultPrefix(key), value)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.node.Propose(ctx, []*pb.LogEntry{{Data: data}})
}
```
定义日志的编解码接口，对键值对/日志进行编解码。
```go
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
}
```
在集群中我们已经使用了protobuf对日志进行序列化/反序列化，对键值对的编解码我们也直接使用protobuf，实现如下：
```go
type ProtobufEncoding struct {
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

func (pe *ProtobufEncoding) EncodeLogEntryData(key, value []byte) []byte {
	data, _ := proto.Marshal(&clientpb.KvPair{Key: key, Value: value})
	return data
}

func (pe *ProtobufEncoding) DecodeLogEntryData(entry []byte) ([]byte, []byte) {
	var pair clientpb.KvPair
	proto.Unmarshal(entry, &pair)
	return pair.Key, pair.Value
}

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
```
实现查询接口，使用指定key查询数据
```go
func (s *RaftServer) Get(ctx context.Context, req *clientpb.ReadonlyQuery) (*clientpb.Response, error) {
	var res *clientpb.Response
	if s.node.IsLeader() {
		value, err := s.get(req.Key)
		if err != nil {
			res = &clientpb.Response{Success: false, Msg: fmt.Sprintf("查询key失败: %s", err.Error())}
		} else {
			res = &clientpb.Response{Success: true, Data: []*clientpb.KvPair{{Key: req.Key, Value: value}}}
		}
	} else {
		res = &clientpb.Response{Success: false}
	}
	return res, nil
}
```
### 快照发送实现
---
定义实际快照如下:
```go
type SnapshotSegment struct {
	LastIncludeIndex uint64                  // 最后包含日志
	LastIncludeTerm  uint64                  // 最后包含任期
	datac            []chan *lsm.RawNodeData // 数据读取通道
}

// 快照文件
type SnapshotFile struct {
	fd      *os.File
	level   int    // sst level
	segment int    // 文件在快照对应片段序号(SnapshotSegment.datac 下标)
	offset  uint64 // 已读取偏移
	done    bool   // 是否读取完成
}

type Snapshot struct {
	dir              string
	data             *lsm.Tree                // lsm 保存实际数据
	lastIncludeIndex uint64                   // 最后包含日志
	lastIncludeTerm  uint64                   // 最后包含任期
	installingSnap   map[string]*SnapshotFile // 对应快照文件
	logger           *zap.SugaredLogger
}
```
实现快照文件查找方法，依据follower节点最新日志编号，找到需要发送的快照文件。
- 已实现的LSM Tree中没有日志编号数据，于是我们在每次将内存数据写入磁盘SSTable时，将对应最后包含日志编号、任期记入文件名称，这样当需求日志编号LSM Tree第0层找到时，可以只发送部分SStable文件，如在其他层，日志编号信息已丢失，需将文件全部发送。
```go
func (ss *Snapshot) GetSegment(index uint64) (chan *pb.Snapshot, error) {
	size := int64(4 * 1000 * 1000)
	send := make([]*SnapshotSegment, 0)
	tree := ss.data.GetNodes()
	var find bool

	// 0层文件最后包含日志完整，可单个发送
	for i := len(tree[0]) - 1; i >= 0; i-- {
		n := tree[0][i]
		lastIndex, lastTerm, err := getLastIncludeIndexAndTerm(n)
		if err != nil {
			return nil, fmt.Errorf("获取需发送快照失败: %v", err)
		}
		if lastIndex <= index {
			find = true
			break
		}
		ss.logger.Debugf("日志 %d 对应快照文件 %d_%d, 最后日志 %d 任期 %d", index, n.Level, n.SeqNo, lastIndex, lastTerm)
		send = append(send, &SnapshotSegment{
			LastIncludeIndex: lastIndex,
			LastIncludeTerm:  lastTerm,
			datac:            []chan *lsm.RawNodeData{n.ReadRaw(size)},
		})
	}

	if !find {
		// 非0层文件，最后包含日志在lsm合并时会按大小拆分，最后包含日志存在误差，需发送全部
		for i, level := range tree[1:] {
			var lastIndex uint64
			var lastTerm uint64
			for _, n := range level {
				nodeLastIndex, nodeLastTerm, err := getLastIncludeIndexAndTerm(n)
				if err != nil {
					return nil, fmt.Errorf("获取需发送快照失败: %v", err)
				}
				if nodeLastIndex > lastIndex {
					lastIndex = nodeLastIndex
					lastTerm = nodeLastTerm
				}
			}
			if lastIndex > 0 {
				datac := make([]chan *lsm.RawNodeData, len(level))
				for j, n := range level {
					datac[j] = n.ReadRaw(size)
				}
				send = append(send, &SnapshotSegment{
					LastIncludeIndex: lastIndex,
					LastIncludeTerm:  lastTerm,
					datac:            datac,
				})
			}
		}
	}
	snapc := make(chan *pb.Snapshot)
	go ss.readSnapshot(send, snapc)
	return snapc, nil
}
```
实现快照文件读取，遍历需发送快照信息，按发送大小读取文件，包装为InstallSnapshot请求，发送到快照读取通道。
```go
func (ss *Snapshot) readSnapshot(send []*SnapshotSegment, snapc chan *pb.Snapshot) {
	defer close(snapc)
	// 倒序遍历待发送快照，逐个读取文件发送
	for i := len(send) - 1; i >= 0; i-- {
		for j := len(send[i].datac) - 1; j >= 0; j-- {
			readc := send[i].datac[j]
			for {
				data := <-readc
				if data == nil {
					break
				}
				if data.Err != nil {
					ss.logger.Errorf("读取快照文件 %d_%d 失败: %v", data.Level, data.SeqNo, data.Err)
					return
				}
				snap := &pb.Snapshot{
					LastIncludeIndex: send[i].LastIncludeIndex,
					LastIncludeTerm:  send[i].LastIncludeTerm,
					Level:            uint32(data.Level),
					Segment:          uint32(j),
					Data:             data.Data,
					Offset:           uint64(data.Offset),
					Done:             data.Done,
				}
				snapc <- snap
				if data.Done {
					break
				}
			}
		}
	}
}
func (n *Node) ReadRaw(perSize int64) chan *RawNodeData {
	readc := make(chan *RawNodeData)
	remain := n.FileSize
	var offset int64
	var data []byte
	var err error
	var done bool

	n.wg.Add(1)
	go func() {
		defer func() {
			close(readc)
			n.wg.Done()
		}()
		for remain > 0 {
			if remain > perSize {
				data, err = n.sr.Read(offset, perSize)
			} else {
				data, err = n.sr.Read(offset, remain)
				if err == nil {
					done = true
				}
			}
			if err != nil {
				err = fmt.Errorf("读取 %d_%d_%s 数据失败: %v", n.Level, n.SeqNo, n.Extra, err)
			}
			readc <- &RawNodeData{Level: n.Level, SeqNo: n.SeqNo, Offset: offset, Data: data, Done: done, Err: err}
			if err != nil {
				break
			} else {
				readSize := int64(len(data))
				offset += readSize
				remain -= readSize
			}
		}
	}()
	return readc
}
```
实现快照文件接收，各文件独立接收管理进度，当文件接收完成将文件合并到已存在的LSM Tree中。
```go
func (ss *Snapshot) AddSnapshotSegment(segment *pb.Snapshot) (bool, error) {
	var err error
	var sf *SnapshotFile
	tmpPath := path.Join(ss.dir, "tmp")

	if ss.installingSnap == nil {
		ss.installingSnap = make(map[string]*SnapshotFile)
	}

	extra := fmt.Sprintf("%s@%d", strconv.FormatUint(segment.LastIncludeIndex, 16), segment.LastIncludeTerm)
	file := fmt.Sprintf("%d_%s_%d.sst", segment.Level, extra, segment.Segment)

	// 片段偏移为0,新建文件
	if segment.Offset == 0 {
		if _, err := os.Stat(tmpPath); err != nil {
			os.Mkdir(tmpPath, os.ModePerm)
		}
		filePath := path.Join(tmpPath, file)
		// 文件已存在，关闭旧文件写入并删除文件
		old, exsit := ss.installingSnap[file]
		if exsit {
			old.fd.Close()
		}
		os.Remove(filePath)
		// 创建临时文件，保存句柄
		fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			ss.logger.Errorf("创建临时快照文件%s失败:%v", file, err)
			return false, err
		}
		sf = &SnapshotFile{fd: fd, level: int(segment.Level), segment: int(segment.Segment), offset: 0}
		ss.installingSnap[file] = sf
	} else { // 偏移不为0,查找已存在文件
		sf = ss.installingSnap[file]
		if sf == nil {
			ss.logger.Errorf("未找到临时快照文件%s", file)
			return false, err
		}
		if sf.offset != segment.Offset {
			ss.logger.Errorf("临时快照文件%s 偏移与接收段偏移不一致", file)
			return false, err
		}
	}
	// 写入片段到文件
	n, err := sf.fd.Write(segment.Data)
	if err != nil {
		ss.logger.Errorf("写入临时快照文件%s失败:%v", file, err)
		return false, err
	}

	// 片段写入完成
	if segment.Done {
		sf.fd.Close()
		sf.done = true
		if segment.Level == 0 { // 文件为第0层，单个文件为快照,合并到lsm
			ss.data.Merge(0, extra, path.Join(tmpPath, file))
			delete(ss.installingSnap, file)
			ss.lastIncludeIndex = segment.LastIncludeIndex
			ss.lastIncludeTerm = segment.LastIncludeTerm
			return true, nil
		} else { // 快照不为0层，存在多个文件，片段序号0表示最后一个文件
			var complete bool
			done := true
			// 检查同层是否所有文件传输完成
			for _, v := range ss.installingSnap {
				if v.level == int(segment.Level) {
					done = done && v.done
					if v.segment == 0 {
						complete = true
					}
				}
			}
			// 全部文件传输完成，合并所有文件到层
			if complete && done {
				for k, v := range ss.installingSnap {
					ss.data.Merge(v.level, extra, path.Join(tmpPath, k))
					delete(ss.installingSnap, k)
				}
				ss.lastIncludeIndex = segment.LastIncludeIndex
				ss.lastIncludeTerm = segment.LastIncludeTerm
				return true, nil
			}
		}
	} else {
		sf.offset += uint64(n)
	}
	return false, err
}

func (t *Tree) Merge(level int, extra string, filePath string) error {
	// 强制合并指定层之前数据
	if level > 0 && level < t.conf.MaxLevel {
		for i := 0; i < level; i++ {
			for len(t.tree[i]) > 0 {
				err := t.compaction(i)
				if err != nil {
					return err
				}
			}
		}
	}
	// 移动&重命名文件
	newFile := formatName(level, t.NextSeqNo(level), extra)
	os.Rename(filePath, path.Join(t.conf.Dir, newFile))

	// 加载文件数据
	t.LoadNode(newFile)
	return nil
}
```

[完整代码](https://github.com/nananatsu/simple-raft)

参考：
- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [CONSENSUS: BRIDGING THEORY AND PRACTICE](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [etcd/raft](https://github.com/etcd-io/etcd)








