package raft

type Snapshot struct {
	Offset int64
	Data   []byte
	Done   bool
}

// type RaftMsgType int

// // 消息类型
// const (
// 	MSG_VOTE RaftMsgType = iota
// 	MSG_VOTE_RES
// 	MSG_HEARTBEAT
// 	MSG_HEARTBEAT_RESP
// 	MSG_APPEND_ENTRY
// 	MSG_APPEND_ENTRY_RESP
// 	MSG_PROPOSE
// 	MSG_PROPOSE_RESP
// )

// func (mt RaftMsgType) string() string {
// 	switch mt {
// 	case MSG_VOTE:
// 		return "VOTE"
// 	case MSG_VOTE_RES:
// 		return "VOTE_RESP"
// 	case MSG_HEARTBEAT:
// 		return "HEARTBEAT"
// 	case MSG_HEARTBEAT_RESP:
// 		return "HEARTBEAT_RESP"
// 	case MSG_APPEND_ENTRY:
// 		return "APPEND_ENTRY"
// 	case MSG_APPEND_ENTRY_RESP:
// 		return "APPEND_ENTRY_RESP"
// 	case MSG_PROPOSE:
// 		return "PROPOSE"
// 	case MSG_PROPOSE_RESP:
// 		return "PROPOSE_RESP"
// 	default:
// 		return ""
// 	}
// }

// type Message struct {
// 	MsgType      RaftMsgType
// 	Term         uint64
// 	From         uint64
// 	To           uint64
// 	LastLogIndex uint64
// 	LastLogTerm  uint64
// 	LastCommit   uint64
// 	Entry        []*LogEntry
// 	Snapshot     *Snapshot
// 	Success      bool
// }

// func (m *Message) String() string {

// 	return fmt.Sprintf("类型: %s ,任期: %d ,来源: %d,目的: %d ,上次日志索引: %d ,上次日志任期: %d ,最后提交: %d ,日志: %v ,快照: %v, 结果: %t",
// 		m.MsgType.string(), m.Term, m.From, m.To, m.LastLogIndex, m.LastLogTerm, m.LastCommit, m.Entry, m.Snapshot, m.Success)
// }
