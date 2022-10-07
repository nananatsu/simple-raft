package utils

import "time"

const seqLen = 8
const nodeIdLen = 16
const timestampShift = seqLen + nodeIdLen

var seq uint64
var prevMillis uint64

//  |timestamp|nodeid|seq|
func NextId(nodeId uint64) uint64 {

	now := uint64(time.Now().UnixMilli())
	if now != prevMillis {
		seq = 0
	}
	seq++
	return now<<timestampShift | nodeId<<seqLen | seq
}
