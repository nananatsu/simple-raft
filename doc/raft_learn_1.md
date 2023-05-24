## 用go实现Raft-leader选举篇
---

raft是一种简单、易理解的分布式共识算法，通过强领导者管理日志复制/提交，集群中数据总是从领导者流向追随者，从而将共识问题分为了三个子问题：
- 领导选举，当旧的leader失效后必须选举一个新的leader
- 日志同步，leader同步日志到集群，强制其他节点接受日志
- 安全，任意节点提交了特定编号日志后，其他节点不能提交不同的内容到同一日志编号

raft中节点必定处于leader、follower、candidate三种状态之一
- leader 处理客户端请求，进行日志同步、发送心跳
- follower 不主动发起请求，只会响应Leader、Candicate的请求，如收到客户端请求则转发到leader
- Candicate 发起选举，投票出一个新的Leader

节点状态按以下规则转换：
<img src=./raft_node_state_chage.png width=40% />

- 节点初始状态 follower
- follower节点状态转换
	- 一个选举周期未收到消息 -> canidate
- canidate节点状态转换
 	- 选举周期内未选出leader,重新选举 - canidate
 	- 选举获胜 -> leader
 	- 收到更大任期的请求 -> follower
- leader节点状态转换
	- 收到更大任期的请求 -> follower

raft中将时间分为一个个任期，任期编号是连续、单调增加的整数，每个任期从选举开始，当一个candicate赢得选举后，将在剩下的任期中作为leader，raft保证一个任期中最多有一个leader

raft每个节点都保存了任期编号，节点间通信时会交换任期编号
- 当一个节点任期小于另一个节点，该节点会将任期编号更新到较大值
- 当leader、canidate收到更大任期的请求，会立即转为follower状态
- 节点收到更小任期的请求会将其拒绝

节点间通过RPC进行通信，基本的RPC请求只有两种：
- RequestVote，canidate在选举期间发送到其他节点请求投票
- AppendEntries，leader收到新的提案后，转换为日志同步到集群，使用空日志作为心跳

## Leader选举
---
leader选举规则：
- follower在一个选举周期，未收到消息，切换状态到canidate，更新任期，投票给自己，重置选举计时,广RequestVote请求
- canidate获得集群中大多数节点投票，赢得选举，切换状态到leader，向集群广播空日志，阻止新选举
- canidate收到新leader消息，转换为follower
- canidate在一个选举周期未能获胜或收到新leader消息，开始下一次选举
- 节点在单个任期只能进行一次投票

rpc框架选用grpc，使用proto定义raft消息格式
- 每个消息都会携带消息类型、任期、来源节点编号、目的节点编号
- 日志信息会携带最新日志编号、最新日志任期、最新已提交日志编号、日志
- 响应信息会携带是否成功

```protobuf
enum MessageType {
  VOTE = 0;
  VOTE_RESP = 1;
  HEARTBEAT = 2;
  HEARTBEAT_RESP = 3;
  APPEND_ENTRY = 4;
  APPEND_ENTRY_RESP = 5;
}

message RaftMessage {
  MessageType msgType = 1;
  uint64 term = 2;
  uint64 from = 3;
  uint64 to = 4;
  uint64 lastLogIndex = 5;
  uint64 lastLogTerm = 6;
  uint64 lastCommit = 7;
  repeated LogEntry entry = 8;
  bool success = 9;
}
```
定义raft数据结构，保存节点状态、集群中leader编号、当前任期、上次投票对象、日志、集群信息、选举周期等信息
```go
type Raft struct {
	id                    uint64
	state                 RaftState             // 节点类型
	leader                uint64                // leader id
	currentTerm           uint64                // 当前任期
	voteFor               uint64                // 投票对象
	raftlog               *RaftLog              // 日志
	cluster               *Cluster              // 集群节点
	electionTimeout       int                   // 选举周期
	heartbeatTimeout      int                   // 心跳周期
	randomElectionTimeout int                   // 随机选举周期
	electtionTick         int                   // 选举时钟
	hearbeatTick          int                   // 心跳时钟
	Tick                  func()                // 时钟函数,Leader为心跳时钟，其他为选举时钟
	hanbleMessage         func(*pb.RaftMessage) // 消息处理函数,按节点状态对应不同处理
	Msg                   []*pb.RaftMessage     // 待发送消息
	ReadIndex             []*ReadIndexResp      // 检查Leader完成的readindex
	logger                *zap.SugaredLogger
}
```
分布式系统中各个节点物理时钟存在误差，在实现中使用逻辑时钟，定义Tick()方法，每次调用使时钟加一，leader中时钟控制心跳发送，follower及canidate中时钟控制选举，为了防止leader失效后follower在相同时间都切换为canidate，使得无法选举出leader，每个节点使用随机的选举周期（基础选举周期 + 随机选举周期）,心跳时间需要小于选举周期。
- 每次tick选举时钟加一
- 当选举时钟大于等于玄奇周期
	- follower切换为canidate
	- canidate重新进行选举
```go
func (r *Raft) TickElection() {
	r.electtionTick++

	if r.electtionTick >= r.randomElectionTimeout {
		r.electtionTick = 0
		if r.state == CANDIDATE_STATE {
			r.BroadcastRequestVote()
		}
		if r.state == FOLLOWER_STATE {
			r.SwitchCandidate()
		}
	}
}
```
协议中指定raft节点初始状态为follower，首先实现函数将节点状态转换为canidate
- 将节点状态更改为canidate，更改消息处理函数为canidate消息处理
- 重新设定选举周期，并重新计时，更新时钟Tick()方法为选举时钟处理方法
- 增加任期
- 给自己投票
- 广播请求投票到集群其他节点
```go
func (r *Raft) SwitchCandidate() {
	r.state = CANDIDATE_STATE
	r.leader = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection
	r.hanbleMessage = r.HandleCandidateMessage

	r.BroadcastRequestVote()
	r.electtionTick = 0
	r.logger.Debugf("成为候选者, 任期 %d , 选举周期 %d s", r.currentTerm, r.randomElectionTimeout)
}

func (r *Raft) BroadcastRequestVote() {
	r.currentTerm++
	r.voteFor = r.id
	r.cluster.ResetVoteResult()
	r.cluster.Vote(r.id, true)

	r.logger.Infof("%s 发起投票", strconv.FormatUint(r.id, 16))

	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
		r.send(&pb.RaftMessage{
			MsgType:      pb.MessageType_VOTE,
			Term:         r.currentTerm,
			From:         r.id,
			To:           id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		})
	})
}
```
集群信息保存在cluster字段中，集群信息暂包含节点投票记录、日志同步进度，遍历日志同步记录的key得到集群中的节点编号，发送日志到指定节点
- 日志同步进度，在raft日志同步篇中再实现
```go
type Cluster struct {
	voteResp           map[uint64]bool     // 投票节点
	progress           map[uint64]*ReplicaProgress // 各节点进度
	logger             *zap.SugaredLogger
}

func (c *Cluster) Foreach(f func(id uint64, p *ReplicaProgress)) {
	for id, p := range c.progress {
		f(id, p)
	}
}
```
广播时调用send方法，将数据添加到消息切片，后续在外部读取切片，发送到其他节点
```go
func (r *Raft) send(msg *pb.RaftMessage) {
	r.Msg = append(r.Msg, msg)
}
```
集群中的follower、canidate都会处理投票请求，并将结果发送回请求投票的节点，响应额外包含节点日志进度，成为leader的节点可以安照日志进度继续进行日志同步
- 当前任期未投票，请求方最新日志编号大于等于自身日志最新编号，则投票给请求者
	- 最新日志编号从日志记录中取得，后续篇章中实现
- 当前任期已投票、或请求方最新日志编号小于自身日志最新编号，拒绝投票
```go
func (r *Raft) ReciveRequestVote(mTerm, mCandidateId, mLastLogTerm, mLastLogIndex uint64) (success bool) {

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	if r.voteFor == 0 || r.voteFor == mCandidateId {
		if mTerm > r.currentTerm && mLastLogTerm >= lastLogTerm && mLastLogIndex >= lastLogIndex {
			r.voteFor = mCandidateId
			success = true
		}
	}

	r.logger.Debugf("候选人: %s, 投票: %t ", strconv.FormatUint(mCandidateId, 16), success)

	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_VOTE_RESP,
		Term:         mTerm,
		From:         r.id,
		To:           mCandidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      success,
	})
	return
}
```
发起投票节点收到响应后记录节点对应投票结果及日志进度，判断是否已赢得选举/选举失败
- 得到大多数节点投票
	- 切换状态为leader
	- 广播心跳到集群，阻止其他节点再进行选举
- 没有得到大多数投票
	- 清空投票状态
	 - 收到leader心跳，则切换状态为folloer
	 - 选举周期超时，开始下一次选举
```go
func (r *Raft) ReciveVoteResp(from, term, lastLogTerm, lastLogIndex uint64, success bool) {

	leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	r.cluster.Vote(from, success)
	r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)

	voteRes := r.cluster.CheckVoteResult()
	if voteRes == VoteWon {
		r.logger.Debugf("节点 %s 发起投票, 赢得选举", strconv.FormatUint(r.id, 16))
		for k, v := range r.cluster.voteResp {
			if !v {
				r.cluster.ResetLogIndex(k, lastLogIndex, leaderLastLogIndex)
			}
		}
		r.SwitchLeader()
		r.BroadcastAppendEntries()
	} else if voteRes == VoteLost {
		r.logger.Debugf("节点 %s 发起投票, 输掉选举", strconv.FormatUint(r.id, 16))
		r.voteFor = 0
		r.cluster.ResetVoteResult()
	}
}

func (c *Cluster) ResetVoteResult() {
	c.voteResp = make(map[uint64]bool)
}

func (c *Cluster) Vote(id uint64, granted bool) {
	c.voteResp[id] = granted
}
```
定义集群大多数为: 节点数量/2+1
- canidate收到 节点数量/2+1 的投票，为赢得选举
- canidate收到 节点数量/2 的拒绝投票，为选举失败
```go
func (c *Cluster) CheckVoteResult() VoteResult {
	granted := 0
	reject := 0
	// 统计承认/拒绝数量
	for _, v := range c.voteResp {
		if v {
			granted++
		} else {
			reject++
		}
	}

	// most := len(c.progress)/2 + 1
	half := len(c.progress) / 2
	// 多数承认->赢得选举
	if granted >= half+1 {
		return VoteWon
	} else if reject >= half { // 半数拒绝，选举失败
		return VoteLost
	}
	// 尚在选举
	return Voting
}
```
实现消息处理方法，分为公共处理部分、特定消息处理部分
- 公共处理部分，检查消息的任期，拒绝过时消息及收到消息任期比节点新时，更新节点任期及状态
- 特定消息处理，按节点状态(leader、follower、canidate)处理消息
```go
func (r *Raft) HandleMessage(msg *pb.RaftMessage) {
	if msg == nil {
		return
	}

	// 消息任期小于节点任期,拒绝消息: 1、网络延迟，节点任期是集群任期; 2、网络断开,节点增加了任期，集群任期是消息任期
	if msg.Term < r.currentTerm {
		r.logger.Debugf("收到来自 %s 过期 (%d) %s 消息 ", strconv.FormatUint(msg.From, 16), msg.Term, msg.MsgType)
		return
	} else if msg.Term > r.currentTerm {
		// 消息非请求投票，集群发生选举，新任期产生
		if msg.MsgType != pb.MessageType_VOTE {
			// 日志追加、心跳、快照为leader发出，，节点成为该leader追随者
			if msg.MsgType == pb.MessageType_APPEND_ENTRY || msg.MsgType == pb.MessageType_HEARTBEAT || msg.MsgType == pb.MessageType_INSTALL_SNAPSHOT {
				r.SwitchFollower(msg.From, msg.Term)
			} else { // 变更节点为追随者，等待leader消息
				r.SwitchFollower(msg.From, 0)
			}
		}
	}

	r.hanbleMessage(msg)
}
```

实现canidate消息处理方法，处理投票请求、投票响应、心跳、日志
- 新leader上线时，节点日志进度进度与leader一致则发送空日志(心跳)，如节点日志未同步到最新则直接发送缺失日志
```go
func (r *Raft) HandleCandidateMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_VOTE:
		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant { // 投票后重置选举时间
			r.electtionTick = 0
		}
	case pb.MessageType_VOTE_RESP:
		r.ReciveVoteResp(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.Success)
	case pb.MessageType_HEARTBEAT:
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context)
	case pb.MessageType_APPEND_ENTRY:
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}
```
实现心跳处理方法
- 当收到心跳时，检查leader最新提交进度，提交本地日志
- 响应心跳给leader
```go
func (r *Raft) ReciveHeartbeat(mFrom, mTerm, mLastLogIndex, mLastCommit uint64, context []byte) {
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	r.raftlog.Apply(mLastCommit, lastLogIndex)

	r.send(&pb.RaftMessage{
		MsgType: pb.MessageType_HEARTBEAT_RESP,
		Term:    r.currentTerm,
		From:    r.id,
		To:      mFrom,
		Context: context,
	})
}
```
实现切换leader方法
- 更新节点状态为leader，保存leader编号，重置集群投票、同步进度状态
- 重置心跳、选举时钟，更新时钟Tick()方法为心跳时钟处理方法
- 切换节点消息处理方法为leader消息处理方法
-
```go
func (r *Raft) SwitchLeader() {
	r.logger.Debugf("成为领导者, 任期: %d", r.currentTerm)

	r.state = LEADER_STATE
	r.leader = r.id
	r.voteFor = 0
	// r.cluster.ResetVoteResult()
	r.Tick = r.TickHeartbeat
	r.hanbleMessage = r.HandleLeaderMessage
	r.electtionTick = 0
	r.hearbeatTick = 0
	r.cluster.Reset()
}
```
实现leader消息处理方法，处理提案、投票、心跳响应、日志响应
- 旧leader重新上线时，如集群正在选举，没有新leader旧leader无法转为follower，保留状态按follower行为参与投票
- 心跳响应暂时无需处理，后续在readIndex中处理
```go
func (r *Raft) HandleLeaderMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_PROPOSE:
		r.AppendEntry(msg.Entry)
	case pb.MessageType_VOTE:
		r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
	case pb.MessageType_VOTE_RESP:
		break
	case pb.MessageType_HEARTBEAT_RESP:
		r.ReciveHeartbeatResp(msg.From, msg.Term, msg.LastLogIndex, msg.Context)
	case pb.MessageType_APPEND_ENTRY_RESP:
		r.ReciveAppendEntriesResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}
```
实现心跳时钟处理方法
- 每次tick，将心跳时钟加一
- 当心跳时钟大于等于心跳周期时，向集群广播心跳
	- 心跳是空的日志消息
- 一并检查同步记录，重发两次心跳间未响应的日志
```go
func (r *Raft) TickHeartbeat() {
	r.hearbeatTick++

	lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	if r.hearbeatTick >= r.heartbeatTimeout {
		r.hearbeatTick = 0
		r.BroadcastHeartbeat(nil)
		r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
			if id == r.id {
				return
			}

			pendding := len(p.pending)
			// 重发消息，重发条件：
			// 上次消息发送未响应且当前有发送未完成,且上次心跳该消息就已处于等待响应状态
			// 当前无等待响应消息，且节点下次发送日志小于leader最新日志
			if !p.prevResp && pendding > 0 && p.MaybeLogLost(p.pending[0]) || (pendding == 0 && p.NextIndex <= lastIndex) {
				p.pending = nil
				r.SendAppendEntries(id)
			}
		})
	}
}

func (r *Raft) BroadcastHeartbeat(context []byte) {
	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		lastLogIndex := p.NextIndex - 1
		lastLogTerm := r.raftlog.GetTerm(lastLogIndex)
		r.send(&pb.RaftMessage{
			MsgType:      pb.MessageType_HEARTBEAT,
			Term:         r.currentTerm,
			From:         r.id,
			To:           id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
			LastCommit:   r.raftlog.commitIndex,
			Context:      context,
		})
	})
}
```
实现切换follower方法
- 更新状态为follower，更新leader编号，更新当前任期，重置集群投票、同步进度状态
- 重置选举时钟，更新时钟Tick()方法为选举时钟处理方法
- 更新消息处理方法为follower消息处理方法
```go
func (r *Raft) SwitchFollower(leaderId, term uint64) {

	r.state = FOLLOWER_STATE
	r.leader = leaderId
	r.currentTerm = term
	r.voteFor = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection
	r.hanbleMessage = r.HandleFollowerMessage
	r.electtionTick = 0
	r.cluster.Reset()

	r.logger.Debugf("成为追随者, 领导者 %s, 任期 %d , 选举周期 %d s", strconv.FormatUint(leaderId, 16), term, r.randomElectionTimeout)
}

```
实现follower消息处理方法，处理投票请求、心跳、日志
```go
func (r *Raft) HandleFollowerMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_VOTE:
		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant {
			r.electtionTick = 0
		}
	case pb.MessageType_HEARTBEAT:
		r.electtionTick = 0
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context)
	case pb.MessageType_APPEND_ENTRY:
		r.electtionTick = 0
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}
```

添加新建函数，实例化raft
```go
func NewRaft(id uint64, peers map[uint64]string, logger *zap.SugaredLogger) *Raft {

	raftlog := NewRaftLog(logger)
	raft := &Raft{
		id:               id,
		currentTerm:      raftlog.lastAppliedTerm,
		raftlog:          raftlog,
		cluster:          NewCluster(peers, raftlog.commitIndex, logger),
		electionTimeout:  10,
		heartbeatTimeout: 5,
		logger:           logger,
	}

	logger.Infof("实例: %s ,任期: %d ", strconv.FormatUint(raft.id, 16), raft.currentTerm)
	raft.SwitchFollower(0, raft.currentTerm)

	return raft
}
```

## grpc实现节点通信
---
之前实现了raft的leader选举部分，没有进行实际消息发送接受，接下来我们将通过grpc实现在不同节点间通信，定义RaftNode结构，将消息发送接收通过不通通道
- 内部消息接收流程： grpc server -> raftNode.recvc -> raft.handleMessage()
- 客户端消息接收流程: raft server -> raftNode.propc -> raft.AppendEntry()
- 消息发送流程：raft.send() -> raft.Msg -> raftNode.sendc -> grpc server
```go
type RaftNode struct {
	raft       *Raft                  // raft实例
	recvc      chan *pb.RaftMessage   // 一般消息接收通道
	propc      chan *pb.RaftMessage   // 提议消息接收通道
	sendc      chan []*pb.RaftMessage // 消息发送通道
	stopc      chan struct{}           // 停止
	ticker     *time.Ticker            // 定时器(选取、心跳)
	logger     *zap.SugaredLogger
}
```
添加raftNode主循环
- 检查raft中是否有待发送消息
	- 是，将循环中的sendc设置rafNode中的sendc，以将消息写入通道
	- 否，将循环中的sendc设置为nil，不写入
- select 时钟、发送通道、接收通道、提案通道，择一执行
```go
func (n *RaftNode) Start() {
	go func() {
		var propc chan *pb.RaftMessage
		var sendc chan []*pb.RaftMessage

		for {
			var msgs []*pb.RaftMessage
			// 存在待发送消息，启用发送通道以发送
			if len(n.raft.Msg) > 0 {
				msgs = n.raft.Msg
				sendc = n.sendc
			} else { // 无消息发送隐藏发送通道
				sendc = nil
			}

			select {
			case <-n.ticker.C:
				n.raft.Tick()
			case msg := <-n.recvc:
				n.raft.HandleMessage(msg)
			case msg := <-propc:
				n.raft.HandleMessage(msg)
			case sendc <- msgs:
				n.raft.Msg = nil
			case <-n.stopc:
				return
			}
		}
	}()
}
```
添加外部调用方法，将数据添加到读取通道
```go
func (n *RaftNode) Process(ctx context.Context, msg *pb.RaftMessage) error {
	var ch chan *pb.RaftMessage
	if msg.MsgType == pb.MessageType_PROPOSE {
		ch = n.propc
	} else {
		ch = n.recvc
	}

	select {
	case ch <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
```
将数据发送通道暴露给外部，以读取待发送数据
```go
func (n *RaftNode) SendChan() chan []*pb.RaftMessage {
	return n.sendc
}
```
添加raftNode新建函数，初始化各通道
```go
func NewRaftNode(id uint64, peers map[uint64]string, logger *zap.SugaredLogger) *RaftNode {

	node := &RaftNode{
		raft:       NewRaft(id, peers, logger),
		recvc:      make(chan *pb.RaftMessage),
		propc:      make(chan *pb.RaftMessage),
		sendc:      make(chan []*pb.RaftMessage),
		stopc:      make(chan struct{}),
		ticker:     time.NewTicker(time.Second),
		logger:     logger,
	}

	node.Start()
	return node
}
```
在grpc中定义raft service，节点间通信使用双向流

```protobuf
service Raft {
  rpc consensus(stream RaftMessage) returns (stream RaftMessage) {}
}
```
定义Peer用来保存grpc流
- 将grpc流的客户端流、服务器流重新抽象为Stream，raft中各节点对等，不区分服务器、客户端，客户端、服务器具有相同的处理逻辑
- 两个节点间只需要一条连接，一的节点主动建立连接后，对等节点，不能再建立连接，故设置连接在消息发送前由消息发送方建立，消息接收方直接使用已建立的连接
```go
type Stream interface {
	Send(*pb.RaftMessage) error
	Recv() (*pb.RaftMessage, error)
}

type Remote struct {
	address      string
	conn         *grpc.ClientConn
	client       pb.RaftClient
	clientStream pb.Raft_ConsensusClient
	serverStream pb.Raft_ConsensusServer
}

type Peer struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	id     uint64
	node   *raft.RaftNode      // raft节点实例
	stream Stream              // grpc双向流
	remote *Remote             // 远端连接信息
	close  bool                // 是否准备关闭
	logger *zap.SugaredLogger
}
```
实现消息发送功能
- 在实际发送前，消息的主动发起方建立连接
```go
func (p *Peer) send(msg *pb.RaftMessage) {
	if msg == nil {
		return
	}

	if p.stream == nil {
		if err := p.Connect(); err != nil {
			return
		}
	}

	if err := p.stream.Send(msg); err != nil {
		p.logger.Errorf("发送消息 %s 到 %s 失败 ,日志数量: %d %v", msg.MsgType.String(), strconv.FormatUint(msg.To, 16), len(msg.Entry), err)
		return
	}
}

func (p *Peer) SendBatch(msgs []*pb.RaftMessage) {
	for _, msg := range msgs {
		p.send(msg)
	}
}
```

- 消息的接收方在连接建立后，设置流到对等节点信息中
```go
func (p *Peer) SetStream(stream pb.Raft_ConsensusServer) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stream == nil {
		p.stream = stream
		p.remote.serverStream = stream

		if p.remote.clientStream != nil {
			p.remote.clientStream.CloseSend()
			p.remote.conn.Close()
			p.remote.clientStream = nil
			p.remote.conn = nil
		}

		return true
	}
	return false
}
```
将主动建立连接分为两个部分：
- grpc初始化，只执行一次，创建配置及客户端实例
- 实际连接，也用在重连，连接到对等节点，并在创建连接后，启动协程读取流
```go
func (p *Peer) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stream != nil {
		return nil
	}

	if p.remote.conn == nil {
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

		conn, err := grpc.Dial(p.remote.address, opts...)
		if err != nil {
			p.logger.Errorf("创建连接 %s 失败: %v", strconv.FormatUint(p.id, 16), err)
			return err
		}

		p.remote.conn = conn
		p.remote.client = pb.NewRaftClient(conn)
	}

	return p.Reconnect()
}

func (p *Peer) Reconnect() error {

	if p.remote.clientStream != nil {
		p.remote.clientStream.CloseSend()
		p.remote.clientStream = nil
		p.stream = nil
	}

	stream, err := p.remote.client.Consensus(context.Background())
	// var delay time.Duration
	for err != nil {
		p.logger.Errorf("连接raft服务 %s 失败: %v", p.remote.address, err)
		return err
	}

	p.logger.Debugf("创建 %s 读写流", strconv.FormatUint(p.id, 16))
	p.stream = stream
	p.remote.clientStream = stream

	go p.Recv()
	return nil
}
```
被动建立连接时，直接将流设置到节点
```go
func (p *Peer) SetStream(stream pb.Raft_ConsensusServer) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stream == nil {
		p.stream = stream
		p.remote.serverStream = stream

		if p.remote.clientStream != nil {
			p.remote.clientStream.CloseSend()
			p.remote.conn.Close()
			p.remote.clientStream = nil
			p.remote.conn = nil
		}

		return true
	}
	return false
}
```
实现流读取，循环从流中读取raft消息，调用raftNode消息处理方法进行处理
```go
func (p *Peer) Recv() {
	// 接收消息
	for {
		msg, err := p.stream.Recv()
		if err == io.EOF {
			p.stream = nil
			p.logger.Errorf("读取 %s 流结束", strconv.FormatUint(p.id, 16))
			return
		}

		if err != nil {
			p.stream = nil
			p.logger.Errorf("读取 %s 流失败： %v", strconv.FormatUint(p.id, 16), err)
			return
		}

		err = p.Process(msg)
		if err != nil {
			p.logger.Errorf("处理消息失败： %v", err)
		}
	}
}

func (p *Peer) Process(msg *pb.RaftMessage) (err error) {
	defer func() {
		if reason := recover(); reason != nil {
			err = fmt.Errorf("处理消息 %s 失败:%v", msg.String(), reason)
		}
	}()

	return p.node.Process(context.Background(), msg)
}
```
定义raft server，继承grpc定义raft service
```go
type RaftServer struct {
	pb.RaftServer

	id            uint64
	name          string
	peerAddress   string
	raftServer    *grpc.Server
	peers         map[uint64]*Peer
	node          *raft.RaftNode
	close         bool
	stopc         chan struct{}
	logger        *zap.SugaredLogger
}
```
实现grpc service方法，接收流，并将流保存到map
- 读取第一条消息，解析来源raft编号
- 将流设置到集群节点map中来源raft编号对应位置，如来源raft编号不在集群则不做处理
- 启动协程读取流，处理接收的raft消息
```go
func (s *RaftServer) Consensus(stream pb.Raft_ConsensusServer) error {
	msg, err := stream.Recv()
	if err == io.EOF {
		s.logger.Debugf("流读取结束")
		return nil
	}
	if err != nil {
		s.logger.Debugf("流读取异常: %v", err)
		return err
	}
	return s.addServerPeer(stream, msg)
}

func (s *RaftServer) addServerPeer(stream pb.Raft_ConsensusServer, msg *pb.RaftMessage) error {

	p, isMember := s.peers[msg.From]
	if !isMember {
		s.logger.Debugf("收到非集群节点 %s 消息 %s", strconv.FormatUint(msg.From, 16), msg.String())
		return fmt.Errorf("非集群节点")
	}

	s.logger.Debugf("添加 %s 读写流", strconv.FormatUint(msg.From, 16))
	if p.SetStream(stream) {
		p.Process(msg)
		p.Recv()
	}
	return nil
}
```
启动协程从raftNode发送通道读取待发送数据，从集群节点信息msp取到消息对应节点，通过grpc发送
```go
func (s *RaftServer) handle() {
	go func() {
		for {
			select {
			case <-s.stopc:
				return
			case msgs := <-s.node.SendChan():
				s.sendMsg(msgs)
			}
		}
	}()
}

func (s *RaftServer) sendMsg(msgs []*pb.RaftMessage) {
	msgMap := make(map[uint64][]*pb.RaftMessage, len(s.peers)-1)

	for _, msg := range msgs {
		if s.peers[msg.To] == nil {
			s.logger.Debugf("节点 %s 不在集群, 发送消息失败", strconv.FormatUint(msg.To, 16))
			continue
		} else {
			if msgMap[msg.To] == nil {
				msgMap[msg.To] = make([]*pb.RaftMessage, 0)
			}
			msgMap[msg.To] = append(msgMap[msg.To], msg)
		}
	}
	for k, v := range msgMap {
		if len(v) > 0 {
			s.peers[k].SendBatch(v)
		}
	}
}
```
实现grpc server启动方法
```go
func (s *RaftServer) Start() {

	lis, err := net.Listen("tcp", s.peerAddress)
	if err != nil {
		s.logger.Errorf("对等节点服务器失败: %v", err)
	}
	var opts []grpc.ServerOption
	s.raftServer = grpc.NewServer(opts...)

	s.logger.Infof("对等节点服务器启动成功 %s", s.peerAddress)

	pb.RegisterRaftServer(s.raftServer, s)

	s.handle()

	err = s.raftServer.Serve(lis)
	if err != nil {
		s.logger.Errorf("Raft内部服务器关闭: %v", err)
	}
}
```
添加入口函数,调用该函数，传入配置，初始化raft server，再调用start启动服务
```go

type Config struct {
	Name          string
	PeerAddress   string
	Peers         map[string]string
	Logger        *zap.SugaredLogger
}

func GenerateNodeId(name string) uint64 {
	hash := sha1.Sum([]byte(name))
	return binary.BigEndian.Uint64(hash[:8])
}

func Bootstrap(conf *Config) *RaftServer {

	var nodeId uint64
	var node *raft.RaftNode
	servers := make(map[uint64]*Peer, len(conf.Peers))

	if len(peers) != 0 {
		nodeId = GenerateNodeId(conf.Name)
		node = raft.NewRaftNode(nodeId, storage, peers, conf.Logger)
	} else {
		peers = make(map[uint64]string, len(conf.Peers))
		// 遍历节点配置，生成各节点id
		for name, address := range conf.Peers {
			id := GenerateNodeId(name)
			peers[id] = address
			if name == conf.Name {
				nodeId = id
			}
		}
		node = raft.NewRaftNode(nodeId, storage, peers, conf.Logger)
		node.InitMember(peers)
	}

	// 初始化远端节点配置
	for id, address := range peers {
		conf.Logger.Infof("集群成员 %s 地址 %s", strconv.FormatUint(id, 16), address)
		if id == nodeId {
			continue
		}
		servers[id] = NewPeer(id, address, node, metric, conf.Logger)
	}

	server := &RaftServer{
		logger:        conf.Logger,
		id:            nodeId,
		name:          conf.Name,
		peerAddress:   conf.PeerAddress,
		peers:         servers,
		node:          node,
		stopc:         make(chan struct{}),
	}

	return server
}
```

[完整代码](https://github.com/nananatsu/simple-raft)

参考：
- <https://github.com/etcd-io/etcd>