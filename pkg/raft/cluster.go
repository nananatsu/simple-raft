package raft

import (
	"fmt"
	pb "kvdb/pkg/raftpb"
	"strconv"

	"go.uber.org/zap"
)

type VoteResult int

const (
	Voting VoteResult = iota
	VoteWon
	VoteLost
)

type Cluster struct {
	incoming           map[uint64]struct{}
	outcoming          map[uint64]struct{}
	pendingChangeIndex uint64
	voteResp           map[uint64]bool
	progress           map[uint64]*ReplicaProgress
	logger             *zap.SugaredLogger
}

func (c *Cluster) CheckVoteResult() VoteResult {
	granted := 0
	reject := 0
	for _, v := range c.voteResp {
		if v {
			granted++
		} else {
			reject++
		}
	}
	most := len(c.progress)/2 + 1

	if granted >= most {
		return VoteWon
	} else if reject >= most {
		return VoteLost
	}
	return Voting
}

func (c *Cluster) ResetVoteResult() {
	c.voteResp = make(map[uint64]bool)
}

func (c *Cluster) Vote(id uint64, granted bool) {
	c.voteResp[id] = granted
}

func (c *Cluster) ChangeMember(changes []*pb.MemberChange) error {

	if len(c.outcoming) > 0 && len(changes) > 0 {
		return fmt.Errorf("前次变更未完成")
	} else if len(changes) == 0 {
		c.outcoming = make(map[uint64]struct{})
		for k := range c.progress {
			_, exsit := c.incoming[k]
			if !exsit {
				c.logger.Debugf("清理节点 %s", strconv.FormatUint(k, 16))
				delete(c.progress, k)
			}
		}
		c.logger.Debugf("清理旧集群信息完成, 当前集群成员数量: %d", len(c.incoming))
		return nil
	}

	for k, v := range c.incoming {
		c.outcoming[k] = v
	}

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

func (c *Cluster) IsPause(id uint64) bool {
	return c.progress[id].IsPause()
}

func (c *Cluster) UpdateLogIndex(id uint64, lastIndex uint64) {
	c.progress[id].NextIndex = lastIndex
	c.progress[id].MatchIndex = lastIndex + 1
}

func (c *Cluster) ResetLogIndex(id uint64, lastIndex uint64) {
	c.progress[id].Reset(lastIndex)
}

func (c *Cluster) AppendEntry(id uint64, lastIndex uint64) {
	c.progress[id].AppendEntry(lastIndex)
}

func (c *Cluster) AppendEntryResp(id uint64, lastIndex uint64) {
	c.progress[id].AppendEntryResp(lastIndex)
}

func (c *Cluster) GetNextIndex(id uint64) uint64 {
	return c.progress[id].NextIndex
}

func (c *Cluster) CheckCommit(index uint64) bool {

	incomingLogged := 0
	for id := range c.incoming {
		if index <= c.progress[id].MatchIndex {
			incomingLogged++
		}
	}
	incomingCommit := incomingLogged >= len(c.incoming)/2+1

	if len(c.outcoming) > 0 {
		outcomingLogged := 0
		for id := range c.outcoming {
			if index <= c.progress[id].MatchIndex {
				outcomingLogged++
			}
		}
		return incomingCommit && (outcomingLogged >= len(c.outcoming)/2+1)
	}

	return incomingCommit
}

func (c *Cluster) Foreach(f func(id uint64, p *ReplicaProgress)) {
	for id, p := range c.progress {
		f(id, p)
	}
}

func NewCluster(peers []uint64, lastIndex uint64, logger *zap.SugaredLogger) *Cluster {

	incoming := make(map[uint64]struct{})
	progress := make(map[uint64]*ReplicaProgress)
	for _, id := range peers {
		progress[id] = &ReplicaProgress{
			NextIndex:  lastIndex + 1,
			MatchIndex: lastIndex,
		}
		incoming[id] = struct{}{}
	}

	return &Cluster{
		incoming:  incoming,
		outcoming: make(map[uint64]struct{}),
		voteResp:  make(map[uint64]bool),
		progress:  progress,
		logger:    logger,
	}

}
