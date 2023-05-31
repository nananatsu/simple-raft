## 用go实现Raft(4) - 成员变更篇
---

一个分布式集群在运行过程中不可避免的会出现节点故障，从而需要能够变更集群节点。
 - 一种方法是关闭集群以新的配置启动集群，而这会导致集群在成员变更期间不可用；
 - 也可以使新节点获得旧节点的IP地址，从而取代节点，这需要保证被替代的节点永远不会再启动；

上述两种办法都有缺陷，人工操作也会有操作错误产生，在raft中加入了成员变更自动化将其作为算法的一部分。

为保证安全性，在成员变更期间不能出现在同一个任期选举两个leader，在一次新增或移除多个节点时，集群中不可能一次性切换所有节点到新状态，过度期间集群可能分裂(下图在有3个节点的集群添加两个新节点，在某个时间$C_{new}$、$C_{old}$可能选取出不同leader)，我们需要额外的机制来处理这个问题。

<img src=./add_multiple_server.png width=60% />

Raft原始论文采用两阶段更新配置，让节点在不同配置间过度，不影响安全使用，并在过度期间提供服务：
- 首先进入一个过度阶段，称为联合共识，联合共识阶段集群中同时存在两种配置；
	- 日志会被复制到两种配置的节点
	- 新旧配置的节点都能作为leader
	- 共识（选举、日志）需要同时由两种配置的大多数节点达成
- 当联合共识被提交，集群已过度到新配置；

Raft集群配置使用特殊的日志来保存和通信，这样可以使用已存在的共识机制使得新配置在集群达成共识
- leader收到成员变更请求（将集群配置从$C_{old}$更新到$C_{new}$），leader将存储配置用于联合共识$C_{old,new}$，并将其作为日志同步到集群；
- follower收到$C_{old,new}$日志后，节点将应用配置$C_{old,new}$无需等待日志提交，日志提交/leader选举需得到$C_{old,new}$中大多数同意，在这期间$C_{new}$不能独立决策；
	- 如leader崩溃，新的leader将在$C_{old}$、$C_{old,new}$中选出；
- 当$C_{old,new}$日志提交后，$C_{old}$、$C_{new}$都能独立进行决策，添加一条新日志$C_{new}$；
	- 此时leader崩溃也能选举出含有$C_{old,new}$日志的新leader，故leader可以安全的添加$C_{new}$日志；
- follower收到$C_{new}$日志同样立即生效；
- 当$C_{new}$日志提交后，不在新集群的节点关机；

<img src=./imgs/raft/raft_change_timeline.png width=60% />

在新的论文中提出了一种更简单的办法，限制raft变化类型，每次只允许添加/删除一个节点，更复杂的变更由一系列单节点变更构成，当每次仅改变一个节点时，旧集群与新集群的大多数是重叠的，这种重叠可以防止集群分裂。

<img src=./imgs/raft/add_single_server.png width=60% />

单节点变更过程如下:
- eader收到成员变更请求（将集群配置从$C_{old}$更新到$C_{new}$），leader将存储配置$C_{new}$，并将其作为日志同步到集群；
- follower收到$C_{new}$日志后，节点将应用配置$C_{new}$无需等待日志提交，$C_{new}$的大多数用于决定$C_{new}$日志的提交；
	- 如需要日志提交后才应用配置，这会导致leader难以得知集群节点是否已应用新配置，需要额外的机制去追踪节点变更状况，这样才能继续后续的变更，而节点收到$C_{new}$日志直接采用时，leader可以$C_{new}$日志提交时判断集群大多数已切换到新状态，可以安全的进行下一次变更。
- 当$C_{new}$提交后成员变更完成，此时可以继续下述内容：
	- leader确认成员变更成功完成；
	- 一个节点从集群中移除，该节点可以关闭；
	- 可以开始下一次的成员变更，同时处理多条成员变更日志等价与一次变更多个节点，会导致集群不安全；

raft中达成共识使用调用者的配置，接收方处理RPC请求时不关心来源是否在接收方的集群配置中。
- 新节点加入集群时，无任何日志，无法从日志中取得集群配置，如不处理集群外RPC请求，将导致该节点无法处理任何RPC请求，永远无法加入集群；
- 节点也需要投票给不属于集群的canidate（canidate日志、任期足够新时），以在某些情况下保证集群可用；

在变更成员时，还有三个问题需要解决：
- 新节点可能没有存储任何日志，集群需要一定时间将日志同步到新节点，在此期间节点如新节点能够在日志同步中投票，可能导致集群在这段时间无法提交新的日志。
	- 故新节点在加入集群时不参与日志投票，直到节点已同步到最新日志
- 当前leader可能不是新集群的一部分。
	- 原始论文中leader需要在$C_{new}$日志提交后下台成为follower，此时出现了非集群节点的leader暂时管理集群；
	- 新论文中中加入了leader转移，在成员变更之前，将leader转移到集群中其他节点；
- 被移除的节点在没有收到心跳后可能开始选取，扰乱集群运行。
	- 节点在认为领导者存在时需要无视RequestVote请求（即节点在得到当前leader消息后的最短选举时间内收到RequestVote的请求）；


### 联合共识实现

首先添加新的日志类型，用来表示成员变更，成员变更分为：添加节点、移除节点，变更只需要知道节点的编号和地址。
```protobuf
enum EntryType {
  NORMAL = 0;
  MEMBER_CHNAGE = 1;
}

enum MemberChangeType {
  ADD_NODE = 0;
  REMOVE_NODE = 1;
}

message MemberChange {
  MemberChangeType type = 1;
  uint64 id = 2;
  string address = 3;
}

message MemberChangeCol { repeated MemberChange changes = 1; }
```

当前的实现中raft分为三层：
- raft server，提供与客户端、其他节点的RPC通信
- raft node，连接raft与raft server，通过通道将数据读写顺序化，避免产生数据竞争
- raft，实现协议逻辑
在raft、raft server中都保存着集群信息，raft server中保存集群各节点完整信息，raft中保存集群各节点编号，于是成员变更实际按以下过程来完成：
- 接收到$C_{old,new}$ -> raft&raft server中加入新集群配置
- $C_{old,new}$提交 -> leader发送$C_{new}$
- 接收到$C_{new}$ -> raft&raft server中移除旧集群配置
- $C_{new}$提交 -> 关闭旧节点

将第一阶段leader发送日志$C_{old,new}$定义为含有内容的变更日志，第二阶段leader发送日志$C_{new}$定义为内容为空的变更日志。

实现raft中成员变更：
- 将raft中集群信息分为新旧两个部分（incoming为当前集群，outcoming为集群），进行联合共识时会同时存在两种配置，集群共识需要两种配置的集群都达到共识；
- 第一阶段时先将当前集群信息incoming复制到旧集群信息outcoming，再将成员变更内容更新到当前集群信息incoming；
- 第二阶段时清除旧集群信息outcoming，如某个节点被移除同时清理对应同步记录；
```go
type Cluster struct {
	incoming           map[uint64]struct{} // 当前/新集群节点
	outcoming          map[uint64]struct{} // 旧集群节点
	pendingChangeIndex uint64              // 未完成变更日志
	inJoint            bool                // 是否正在进行联合共识
	...
}

func (c *Cluster) ApplyChange(changes []*pb.MemberChange) error {
	 if len(changes) == 0 {
		// 成员变更数为0,当前变更为阶段2,清空旧集群数据
		c.outcoming = make(map[uint64]struct{})
		for k := range c.progress {
			_, exsit := c.incoming[k]
			if !exsit {
				delete(c.progress, k)
			}
		}
		c.inJoint = false
		c.logger.Debugf("清理旧集群信息完成, 当前集群成员数量: %d", len(c.incoming))
		return nil
	}
	// 转移集群数据到outcoming
	for k, v := range c.incoming {
		c.outcoming[k] = v
	}

	// 按变更更新成员
	for _, change := range changes {
		if change.Type == pb.MemberChangeType_ADD_NODE {
			c.progress[change.Id] = &ReplicaProgress{
				MatchIndex: 0,
				NextIndex:  1,
			}
			c.incoming[change.Id] = struct{}{}
			c.logger.Debugf("添加集群成员: %s ,新集群成员数量: %d", strconv.FormatUint(change.Id, 16), len(c.incoming))
		} else if change.Type == pb.MemberChangeType_REMOVE_NODE {
			delete(c.incoming, change.Id)
			c.logger.Debugf("移除集群成员: %s ,新集群成员数量: %d", strconv.FormatUint(change.Id, 16), len(c.incoming))
		}
	}
	return nil
}
```
在raft node中添加应用配置方法。
- 添加一个新的通道用来应用配置，raft实现中无锁，变更配置需要与其他的处理一起顺序执行，防止数据竞争；
- 在主循环中加入成员变更通道等待，从通道取得变更内容后，调用raft中应用变更方法执行变更；
```go
type RaftNode struct {
	···
	changec    chan []*pb.MemberChange // 变更接收通道
	···
}

func (n *RaftNode) ApplyChange(changes []*pb.MemberChange) {
	n.changec <- changes
}

func (n *RaftNode) Start() {
	go func() {
		...
		for {
			...
			select {
			...
			case changes := <-n.changec:
				n.raft.ApplyChange(changes)
			case <-n.stopc:
				return
			}
		}
	}()
}
```
实现raft server中成员变更：
- 检查当前配置与变更请求的差异，判断是否需要进行变更，如当前集群配置与变更后一致，则无需后续处理；
- 依据日志内容判断为变更的哪个阶段：
	- 在第一阶段时，先更新raft server中集群信息，再更新raft中集群信息；
		- 当变更为新增节点时，加入节点到配置，建立与节点的连接；
		- 当变更为移除节点时，标记节点关闭；
	- 在第二阶段时，更新raft中集群信息,raft server中无需处理，新增的节点已在上阶段配置，删除的节点的移除在$C_{new}$日志提交后；
```go
func (s *RaftServer) applyChange(changes []*pb.MemberChange) {
	changeCount := len(changes)
	diffCount := 0
	for _, mc := range changes {
		p := s.peers[mc.Id]
		if mc.Type == pb.MemberChangeType_ADD_NODE {
			if (p == nil && s.id != mc.Id) || (p != nil && p.remote.address != mc.Address) {
				diffCount++
			}
		} else if mc.Type == pb.MemberChangeType_REMOVE_NODE {
			if p != nil || s.id == mc.Id {
				diffCount++
			}
		}
	}
	if diffCount == 0 && changeCount > 0 {
		return
	}
	if changeCount > 0 {
		for _, mc := range changes {
			if mc.Type == pb.MemberChangeType_ADD_NODE {
				if mc.Id != s.id {
					_, exsit := s.peers[mc.Id]
					if !exsit {
						peer := NewPeer(mc.Id, mc.Address, s.node, s.metric, s.logger)
						s.peers[mc.Id] = peer
					}
				}
			} else {
				if mc.Id != s.id {
					s.peers[mc.Id].close = true
				} else {
					s.close = true
				}
			}
		}
		// 先启动相关连接，再更新集群信息，防止无法发送消息
		s.node.ApplyChange(changes)
	} else {
		// 更新集群信息
		s.node.ApplyChange(changes)
	}
}

// 关闭连接
func (s *RaftServer) applyRemove() {
	for k, p := range s.peers {
		if p.close {
			delete(s.peers, k)
			p.Stop()
		}
	}

	if s.close {
		s.Stop()
	}
}
```
在raft server添加成员变更提案方法，将变更的成员、类型封装为成员变更消息进行提案
- 变更前检查是否能够进行变更（前一次变更已完成），上次变更未完成则拒绝请求；
- 按变更更新raft server、raft中的集群信息；
- 向集群进行变更提案；
```go
func (s *RaftServer) changeMember(peers map[string]string, changeType pb.MemberChangeType) error {

	changes := make([]*pb.MemberChange, 0, len(peers))
	for name, address := range peers {
		id := GenerateNodeId(name)
		change := &pb.MemberChange{
			Type:    changeType,
			Id:      id,
			Address: address,
		}
		changes = append(changes, change)
	}

	if !s.node.CanChange(changes) {
		return fmt.Errorf("前次变更未完成")
	}

	s.applyChange(changes)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return s.node.ChangeMember(ctx, changes)
}
```
在raft node添加方法将变更包装为日志提案。
```go
func (n *RaftNode) ChangeMember(ctx context.Context, changes []*pb.MemberChange) error {
	changeCol := &pb.MemberChangeCol{Changes: changes}
	data, err := proto.Marshal(changeCol)
	if err != nil {
		n.logger.Errorf("序列化变更成员信息失败: %v", err)
		return err
	}
	return n.Propose(ctx, []*pb.LogEntry{{Type: pb.EntryType_MEMBER_CHNAGE, Data: data}})
}
```
当leader追加日志时，检查是否为成员变更请求，当在进行成员变更时记录该日志编号，标记联合共识开始。
```go
func (r *Raft) AppendEntry(entries []*pb.LogEntry) {
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	for i, entry := range entries {
		if entry.Type == pb.EntryType_MEMBER_CHNAGE {
			r.cluster.pendingChangeIndex = entry.Index
			r.cluster.inJoint = true
		}
		entry.Index = lastLogIndex + 1 + uint64(i)
		entry.Term = r.currentTerm
	}
	...
}
```
当follower收到$C_{old,new}$日志时，立即调用相关方法执行变更。
- 过滤成员变更类型的日志，解码为成员变更信息，调用raft server应用变更方法执行变更。
```go
func (s *RaftServer) process(msg *pb.RaftMessage) (err error) {
	defer func() {
		if reason := recover(); reason != nil {
			err = fmt.Errorf("处理消息 %s 失败:%v", msg.String(), reason)
		}
	}()
	if msg.MsgType == pb.MessageType_APPEND_ENTRY {
		for _, entry := range msg.GetEntry() {
			if entry.Type == pb.EntryType_MEMBER_CHNAGE {
				var changeCol pb.MemberChangeCol
				err := proto.Unmarshal(entry.Data, &changeCol)
				if err != nil {
					s.logger.Warnf("解析成员变更日志失败: %v", err)
				}
				s.applyChange(changeCol.Changes)
			}
		}
	}
	return s.node.Process(context.Background(), msg)
}
```
leader在处理日志响应时，检查联合共识是否已经完成进行第二阶段。
- 在提交日志后，检查本次提交是否包含了成员变更日志（提交前编号小于成员变更编号，提交后编号大于成员变更编号）；
- 如提交含成员变更日志则该进入成员变更的第二阶段，发送一条空内容的变更日志；
```go
func (r *Raft) ReciveAppendEntriesResult(from, term, lastLogIndex uint64, success bool) {
	leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	if success {
		r.cluster.AppendEntryResp(from, lastLogIndex)
		if lastLogIndex > r.raftlog.commitIndex {
			// 取已同步索引更新到lastcommit
			if r.cluster.CheckCommit(lastLogIndex) {
				...
				// 检查联合共识是否完成
				if r.cluster.inJoint && prevApplied < r.cluster.pendingChangeIndex && lastLogIndex >= r.cluster.pendingChangeIndex {
					r.AppendEntry([]*pb.LogEntry{{Type: pb.EntryType_MEMBER_CHNAGE}})
					lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
					r.cluster.pendingChangeIndex = lastIndex
				}
			}
		}
		...
	} 
	...
}
```
在日志提交后添加通道通知raft server变更日志提交
- 当$C_{new}$日志提交后，关闭连接，如当前节点已移除则关闭节点进程；
```go
type RaftStorage struct {
	...
	notifyc             chan []*pb.MemberChange // 变更提交通知通道
}

func (s *RaftServer) handle() {
	go func() {
		for {
			select {
			case <-s.stopc:
				return
			case msgs := <-s.node.SendChan():
				s.sendMsg(msgs)
			case msg := <-s.incomingChan:
				s.process(msg)
			case changes := <-s.storage.NotifyChan():
				if len(changes) == 0 {
					go s.applyRemove()
				}
			}
		}
	}()
}
```

[完整代码](https://github.com/nananatsu/simple-raft)

参考：
- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [CONSENSUS: BRIDGING THEORY AND PRACTICE](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [etcd/raft](https://github.com/etcd-io/etcd)