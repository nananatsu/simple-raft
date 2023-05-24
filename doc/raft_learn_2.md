## 用go实现Raft-日志同步篇
---

在raft中，选取成功后集群就可以正常工作，一次正常的客户端提案过程如下：
- 客户端连接到leader，发起提案
- leader收到提案后将提案，包装为一条日志
- leader将日志缓存到待提交日志
- leader发送日志到集群其他节点
- follower收到日志后，将日志缓存到待提交日志，并响应leader请求
- leader收到follower响应，检查是否集群大多数节点已响应
- 集群中大多数节点已缓存日志后，leader提交日志，发送空日志要求follower提交日志
- leader响应客户端提案已接收

raft中日志规则如下：
- 如果在不同节点日志中的两条日志记录有相同的任期、编号，则这两条日志记录具有相同的内容
    - leader在单个任期中最多创建一个给定编号的日志记录
    - leader不会改变日志记录所在的位置（leader不会覆盖、删除日志记录）
- 如果在不同节点日志中的两条日志记录有相同的任期、编号，则这两条日志之前的所有日志相同
    - leader发送日志记录到follower会包含上次日志记录编号、任期
    - follower在收到追加日志时请求时会进行一致性检查，如检查失败，follower会拒绝追加请求，保证了日志与leader一致，一致性检查失败时leader会强迫follower接受leader日志，从而保证日志一致性。一致性检查失败分两种情况，
        - follower日志存在缺失，收到失败响应后leader会将发送日志编号减一（follower检查失败时也可以返回当前最新日志编号），发送follower缺失的日志
            - 当节点下线过一段时间/网络异常消息丢失会出现日志缺失
        - follower有多余/不一致的日志，收到失败响应后leader会将发送日志编号减一（follower检查失败时也可以返回当前最后提交的日志编号），找到两份日志中最新的一致记录编号，删除follower中不一致的部分
            - 节点当选leader收到消息只追加到本地尚未同步到集群便异常下线，后续上线会出现内存中有不一致的日志，已提交日志不会出现不一致

日志在节点间的同步分两步完成，
- leader通过rpc将日志复制到其他节点
- 当leader确认日志已被复制到集群中大多数节点后，将日志持久化到磁盘，leader追踪已提交的日志的最大编号，通过日志rpc（含心跳）发送该编号告知follower需提交日志
    - 日志提交时会持久化当前编号及该编号之前的未提交日志

每条日志到包含客户端的实际提案内容、leader任期、日志编号，定义日志记录为如下结构：
- type 日志类型，当前只有一种日志，客户端的提案
- term 日志产生的任期，编号和任期相同代表为同一条日志
- index 日志编号，单调增加
- data 提案的实际内容
```protobuf
enum EntryType {
  NORMAL = 0;
}

message LogEntry {
  EntryType type = 1;
  uint64 term = 2;
  uint64 index = 3;
  bytes data = 4;
}
```
定义日志整体结构
- 未提交日志，保存在内存切片中
- 日志提交时，取出切片待提交部分，进行持久化
```go
type WaitApply struct {
	done  bool
	index uint64
	ch    chan struct{}
}
type RaftLog struct {
	logEnties        []*pb.LogEntry // 未提交日志
	storage          Storage        // 已提交日志存储
	commitIndex      uint64         // 提交进度
	lastAppliedIndex uint64         // 最后提交日志
	lastAppliedTerm  uint64         // 最后提交日志任期
	lastAppendIndex  uint64         // 最后追加日志
	logger           *zap.SugaredLogger
}
```
定义日志持久化接口，实际存储实现由外部提供
```go
type Storage interface {
	Append(entries []*pb.LogEntry)
	GetEntries(startIndex, endIndex uint64) []*pb.LogEntry
	GetTerm(index uint64) uint64
	GetLastLogIndexAndTerm() (uint64, uint64)
	Close()
}
```
实现一致性检查，已持久化的日志必然与leader一致，检查一致时只需检查内存中日志切片，存在以下几种情况：
- 节点日志中有找到leader上次追加日志
    - 为节点追加的最后一条日志
    - 为节点内存切片中的某条日志
        - 节点网络波动，导致未响应leader，leader重发了记录，清除重复日志记录
        - 节点作为leader期间有部分日志未同步到其他节点就失效，集群重新选举，导致后续日志不一致，清除冲突日志(内存中后续日志)
    - 为节点最后提交日志
        - 如内存中存在日志记录，则内存中的记录皆不一致，清除内存日志记录
- 节点未找到leader上次追加日志
    - 存在相同日志编号记录，任不相同
        - 节点作为leader期间有部分日志未同步到其他节点就失效，集群重新选举，导致使用了相同日志编号，清除冲突日志(相同任期的日志)，从节点未冲突部分开始重发
    - 没有相同日志编号记录
        - 日志缺失，需从最后提交开始重发
```go
func (l *RaftLog) HasPrevLog(lastIndex, lastTerm uint64) bool {
	if lastIndex == 0 {
		return true
	}
	var term uint64
	size := len(l.logEnties)
	if size > 0 {
		lastlog := l.logEnties[size-1]
		if lastlog.Index == lastIndex {
			term = lastlog.Term
		} else if lastlog.Index > lastIndex {
			// 检查最后提交
			if lastIndex == l.lastAppliedIndex { // 已提交日志必然一致
				l.logEnties = l.logEnties[:0]
				return true
			} else if lastIndex > l.lastAppliedIndex {
				// 检查未提交日志
				for i, entry := range l.logEnties[:size] {
					if entry.Index == lastIndex {
						term = entry.Term
						// 将leader上次追加后日志清理
						// 网络异常未收到响应导致leader重发日志/leader重选举使旧leader未同步数据失效
						l.logEnties = l.logEnties[:i+1]
						break
					}
				}
			}
		}
	} else if lastIndex == l.lastAppliedIndex {
		return true
	}

	b := term == lastTerm
	if !b {
		l.logger.Debugf("最新日志: %d, 任期: %d ,本地记录任期: %d", lastIndex, lastTerm, term)
		if term != 0 { // 当日志与leader不一致，删除内存中不一致数据同任期日志记录
			for i, entry := range l.logEnties {
				if entry.Term == term {
					l.logEnties = l.logEnties[:i]
					break
				}
			}
		}
	}
	return b
}
```
实现日志追加，将新的日志添加到内存切片，更新最后追加日志编号
```go
func (l *RaftLog) AppendEntry(entry []*pb.LogEntry) {

	size := len(entry)
	if size == 0 {
		return
	}
	l.logEnties = append(l.logEnties, entry...)
	l.lastAppendIndex = entry[size-1].Index
}
```
实现日志提交
- follower可能未同步全部日志，同步时如节点日志已同步全部待提交日志，则提交待提交日志，否则提交索引已追加日志
- 取出日志中待提交部分，添加到持久化存储，更新提交进度、内存切片
```go
func (l *RaftLog) Apply(lastCommit, lastLogIndex uint64) {
	// 更新可提交索引
	if lastCommit > l.commitIndex {
		if lastLogIndex > lastCommit {
			l.commitIndex = lastCommit
		} else {
			l.commitIndex = lastLogIndex
		}
	}

	// 提交索引
	if l.commitIndex > l.lastAppliedIndex {
		n := 0
		for i, entry := range l.logEnties {
			if l.commitIndex >= entry.Index {
				n = i
			} else {
				break
			}
		}
		entries := l.logEnties[:n+1]

		l.storage.Append(entries)
		l.lastAppliedIndex = l.logEnties[n].Index
		l.lastAppliedTerm = l.logEnties[n].Term
		l.logEnties = l.logEnties[n+1:]

        l.NotifyReadIndex()
	}
}
```
定义新建函数，创建实例时需提供存储实现
```go
func NewRaftLog(storage Storage, logger *zap.SugaredLogger) *RaftLog {
	lastIndex, lastTerm := storage.GetLastLogIndexAndTerm()

	return &RaftLog{
		logEnties:        make([]*pb.LogEntry, 0),
		storage:          storage,
		commitIndex:      lastIndex,
		lastAppliedIndex: lastIndex,
		lastAppliedTerm:  lastTerm,
		lastAppendIndex:  lastIndex,
		logger:           logger,
	}
}
```
实现了日志的一致性检查、追加、提交，接下实现raft中日志处理逻辑，首先我们需要在leader节点中保存集群其他节点的日志同步进度
- 节点在切换为leader时会将进度重置
    - 投票响应中会返回节点最新日志信息
    - 未收到投票响应的，使用leader最新日志，在一致性检查后动态更新
- 在集群使用中通过第一条消息确认网络可用，后续假设网络正常，消息发送即成功，不等待节点响应消息，直到出现同步失败
    - prevResp 记录上次发送结果，初始时为flase
    - pending 中记录未发送完成的日志编号
    - 消息发送时如 !prevResp && len(pending) 为true，表示上次发送未完成，延迟后续信息发送
    - 一次消息发送成功后，prevResp标记为true，后续有待发送日志都直接发送
```go
type ReplicaProgress struct {
	MatchIndex         uint64            // 已接收日志
	NextIndex          uint64            // 下次发送日志
	pending            []uint64          // 未发送完成日志
	prevResp           bool              // 上次日志发送结果
	maybeLostIndex     uint64            // 可能丢失的日志,记上次发送未完以重发
}
```
leader将日志记录追加到本地，再广播到集群
```go
func (r *Raft) BroadcastAppendEntries() {
	r.cluster.Foreach(func(id uint64, _ *ReplicaProgress) {
		if id == r.id {
			return
		}
		r.SendAppendEntries(id)
	})
}

func (r *Raft) SendAppendEntries(to uint64) {
	p := r.cluster.progress[to]
	if p == nil || p.IsPause() {
		return
	}

	nextIndex := r.cluster.GetNextIndex(to)
	lastLogIndex := nextIndex - 1
	lastLogTerm := r.raftlog.GetTerm(lastLogIndex)
	maxSize := MAX_LOG_ENTRY_SEND

	if !p.prevResp {
		maxSize = 1
	}
	// var entries []*pb.LogEntry
	entries := r.raftlog.GetEntries(nextIndex, maxSize)
	size := len(entries)
	if size > 0  {
		r.cluster.AppendEntry(to, entries[size-1].Index)
	}

	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_APPEND_ENTRY,
		Term:         r.currentTerm,
		From:         r.id,
		To:           to,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		LastCommit:   r.raftlog.commitIndex,
		Entry:        entries,
	})
}

```
- 从日志中取得最新日志编号，遍历待追加日志，设置日志编号
- 追加日志到内存切片
- 更新leader追加进度
- 广播日志到集群
```go
func (r *Raft) AppendEntry(entries []*pb.LogEntry) {
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	for i, entry := range entries {
		entry.Index = lastLogIndex + 1 + uint64(i)
		entry.Term = r.currentTerm
	}
	r.raftlog.AppendEntry(entries)
	r.cluster.UpdateLogIndex(r.id, entries[len(entries)-1].Index)

	r.BroadcastAppendEntries()
}

func (c *Cluster) UpdateLogIndex(id uint64, lastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.NextIndex = lastIndex
		p.MatchIndex = lastIndex + 1
	}
}
```
广播日志与之前广播心跳一致，遍历集群信息发送到每个节点，发送按下述流程
- 检查发送状态，如上次发送未完成，暂缓发送
```go
func (rp *ReplicaProgress) IsPause() bool {
	return (!rp.prevResp && len(rp.pending) > 0)
}
```
- 从节点同步进度中取得当前需发送日志编号
```go
func (c *Cluster) GetNextIndex(id uint64) uint64 {
	p := c.progress[id]
	if p != nil {
		return p.NextIndex
	}
	return 0
}
```
- 从leader的日志中取到要发送的日志
```go
func (l *RaftLog) GetEntries(index uint64, maxSize int) []*pb.LogEntry {
	// 请求日志已提交，从存储获取
	if index <= l.lastAppliedIndex {
		endIndex := index + MAX_APPEND_ENTRY_SIZE
		if endIndex >= l.lastAppliedIndex {
			endIndex = l.lastAppliedIndex + 1
		}
		return l.storage.GetEntries(index, endIndex)
	} else { // 请求日志未提交,从数组获取
		var entries []*pb.LogEntry
		for i, entry := range l.logEnties {
			if entry.Index == index {
				if len(l.logEnties)-i > maxSize {
					entries = l.logEnties[i : i+maxSize]
				} else {
					entries = l.logEnties[i:]
				}
				break
			}
		}
		return entries
	}
}
```
- 更新节点发送进度，将节点待发送日志编号加一，将发送的日志编号加入未发送完成切片
    - 上次发送成功时，假设本次也会成功，如发送失败再回退发送进度
```go
func (c *Cluster) AppendEntry(id uint64, lastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.AppendEntry(lastIndex)
	}
}

func (rp *ReplicaProgress) AppendEntry(lastIndex uint64) {
	rp.pending = append(rp.pending, lastIndex)
	if rp.prevResp {
		rp.NextIndex = lastIndex + 1
	}
}
```
日志发送后，是follower收到日志进行处理
- 进行一致性检查
    - 检查成功，将日志追加到follower内存中，标记追加成功
    - 检查失败，一致性检查中已处理冲突日志，直接标记追加失败
- 尝试提交日志，每次日志消息都会包含leader提交进度，按leader提交进度，提交follower日志
- 响应leader本次追加结果
```go
func (r *Raft) ReciveAppendEntries(mLeader, mTerm, mLastLogTerm, mLastLogIndex, mLastCommit uint64, mEntries []*pb.LogEntry) {
	var accept bool
	if !r.raftlog.HasPrevLog(mLastLogIndex, mLastLogTerm) { // 检查节点日志是否与leader一致
		r.logger.Infof("节点未含有上次追加日志: Index: %d, Term: %d ", mLastLogIndex, mLastLogTerm)
		accept = false
	} else {
		r.raftlog.AppendEntry(mEntries)
		accept = true
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	r.raftlog.Apply(mLastCommit, lastLogIndex)
	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_APPEND_ENTRY_RESP,
		Term:         r.currentTerm,
		From:         r.id,
		To:           mLeader,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      accept,
	})
}
```
leader处理follower日志追加响应，响应分为日志追加成功、日志追加失败
```go
func (r *Raft) ReciveAppendEntriesResult(from, term, lastLogIndex uint64, success bool) {
	leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	if success {
		r.cluster.AppendEntryResp(from, lastLogIndex)
		if lastLogIndex > r.raftlog.commitIndex {
			// 取已同步索引更新到lastcommit
			if r.cluster.CheckCommit(lastLogIndex) {
				prevApplied := r.raftlog.lastAppliedIndex
				r.raftlog.Apply(lastLogIndex, lastLogIndex)
				r.BroadcastAppendEntries()
			}
		} else if len(r.raftlog.waitQueue) > 0 {
			r.raftlog.NotifyReadIndex()
		}
		if r.cluster.GetNextIndex(from) <= leaderLastLogIndex {
			r.SendAppendEntries(from)
		}
	} else {
		r.logger.Infof("节点 %s 追加日志失败, Leader记录节点最新日志: %d ,节点最新日志: %d ", strconv.FormatUint(from, 16), r.cluster.GetNextIndex(from)-1, lastLogIndex)

		r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)
		r.SendAppendEntries(from)
	}
}
```
- 日志追加成功时
    - 更新同步进度，更新节点已接收进度，从未发送完成切片中清除已发送部分，标记上次发送成功
    ```go
    func (c *Cluster) AppendEntryResp(id uint64, lastIndex uint64) {
        p := c.progress[id]
        if p != nil {
            p.AppendEntryResp(lastIndex)
        }
    }

    func (rp *ReplicaProgress) AppendEntryResp(lastIndex uint64) {

        if rp.MatchIndex < lastIndex {
            rp.MatchIndex = lastIndex
        }

        idx := -1
        for i, v := range rp.pending {
            if v == lastIndex {
                idx = i
            }
        }

        // 标记前次日志发送成功，更新下次发送
        if !rp.prevResp {
            rp.prevResp = true
            rp.NextIndex = lastIndex + 1
        }

        if idx > -1 {
            // 清除之前发送
            rp.pending = rp.pending[idx+1:]
        }
    }
    ```
    - 检查follower数据同步进度，判断响应对应日志编号是否在集群中大多数节点已同步
    ```go
    func (c *Cluster) CheckCommit(index uint64) bool {
        // 集群达到多数共识才允许提交
        incomingLogged := 0
        for id := range c.progress {
            if index <= c.progress[id].MatchIndex {
                incomingLogged++
            }
        }
        incomingCommit := incomingLogged >= len(c.progress)/2+1
        return incomingCommit
    }
    ```
    - 集群达成多数共识时，提交日志，继续广播日志
    - 当响应follower待发送日志编号小于leader最新日志时继续发送日志
- 当日志追加失败时
    - 按follower响应的日志进度重置日志同步进度，标记上次发送失败，以延缓发送起始日志编号与follower不一致的日志，直到日志正确追加
    ```go
    func (c *Cluster) ResetLogIndex(id uint64, lastIndex uint64, leaderLastIndex uint64) {
        p := c.progress[id]
        if p != nil {
            p.ResetLogIndex(lastIndex, leaderLastIndex)
        }
    }

    func (rp *ReplicaProgress) ResetLogIndex(lastLogIndex uint64, leaderLastLogIndex uint64) {

        // 节点最后日志小于leader最新日志按节点更新进度，否则按leader更新进度
        if lastLogIndex < leaderLastLogIndex {
            rp.NextIndex = lastLogIndex + 1
            rp.MatchIndex = lastLogIndex
        } else {
            rp.NextIndex = leaderLastLogIndex + 1
            rp.MatchIndex = leaderLastLogIndex
        }

        if rp.prevResp {
            rp.prevResp = false
            rp.pending = nil
        }
    }
    ```
    - 按更新后同步进度重发日志

修改raft新建函数，参数中加入存储接口
```go
func NewRaft(id uint64, storage Storage, peers map[uint64]string, logger *zap.SugaredLogger) *Raft {
	raftlog := NewRaftLog(storage, logger)
	...
}
```
raft日志同步逻辑基本实现，接下来实现raftNode中的提案方法以追加日志，在raftNode主循环中已实现读取recv通道，调用raft消息处理方法，当为leader时会将提案追加到日志，当前只需要将提案消息加入recv通道
- 当前leader将提案加入读写通道后视为写入成功，暂不实现集群多数共识后响应客户端
    - 要实现多数响应后通知，可添加一个新的结构含RaftMessage和一个channel，在raftlog添加一个等待队列，当raft处理追加消息时，将日志最后一条记录的日志编号通过channel返回给提案方法，提案方法再将channel放入raftlog中等待队列，提交日志检查等待队列待通知对象，通过channel通知提案方法指定日志编号已提交
```go
func (n *RaftNode) Propose(ctx context.Context, entries []*pb.LogEntry) error {
	msg := &pb.RaftMessage{
		MsgType: pb.MessageType_PROPOSE,
		Term:    n.raft.currentTerm,
		Entry:   entries,
	}
	return n.Process(ctx, msg)
}
```
修改raftNode新建函数添加存储接口，存储实现在下篇lsm中实现
```go
func NewRaftNode(id uint64, storage Storage, peers map[uint64]string, logger *zap.SugaredLogger) *RaftNode {

	node := &RaftNode{
		raft:       NewRaft(id, storage, peers, logger),
		...
	}
    ...
}
```
修改raft server中批量发送消息方法，将多个日志记录合并到一个raft meassage进行发送
```go
func (p *Peer) SendBatch(msgs []*pb.RaftMessage) {
	p.wg.Add(1)
	var appEntryMsg *pb.RaftMessage
	var propMsg *pb.RaftMessage
	for _, msg := range msgs {
		if msg.MsgType == pb.MessageType_APPEND_ENTRY {
			if appEntryMsg == nil {
				appEntryMsg = msg
			} else {
				size := len(appEntryMsg.Entry)
				if size == 0 || len(msg.Entry) == 0 || appEntryMsg.Entry[size-1].Index+1 == msg.Entry[0].Index {
					appEntryMsg.LastCommit = msg.LastCommit
					appEntryMsg.Entry = append(appEntryMsg.Entry, msg.Entry...)
				} else if appEntryMsg.Entry[0].Index >= msg.Entry[0].Index {
					appEntryMsg = msg
				}
			}
		} else if msg.MsgType == pb.MessageType_PROPOSE {
			if propMsg == nil {
				propMsg = msg
			} else {
				propMsg.Entry = append(propMsg.Entry, msg.Entry...)
			}
		} else {
			p.send(msg)
		}
	}

	if appEntryMsg != nil {
		p.send(appEntryMsg)
	}

	if propMsg != nil {
		p.send(propMsg)
	}
	p.wg.Done()
}
```
通过上述代码实现了提案到leader，leader包装为日志，同步到集群的过程，后续将通过lsm实现日志落盘并将raft server作为一个简单的kv数据库。

[完整代码](https://github.com/nananatsu/simple-raft)

参考：
- <https://github.com/etcd-io/etcd>