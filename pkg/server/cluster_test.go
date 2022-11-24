package server

import (
	"context"
	"fmt"
	"kvdb/pkg/raftpb"
	"kvdb/pkg/utils"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func InitServer(clusterNumber int) (map[string]string, []*RaftServer, *RaftServer) {
	peers := make(map[string]string, clusterNumber)
	servers := make([]*Config, clusterNumber)

	dir := "../../build/"
	for i := 0; i < clusterNumber; i++ {
		name := fmt.Sprintf("raft_%d", i+1)
		logger := utils.GetLogger(dir + name)
		sugar := logger.Sugar()

		peerAddress := fmt.Sprintf("localhost:%d", 9123+i)
		peers[name] = peerAddress

		servers[i] = &Config{
			Dir:           dir,
			Name:          name,
			PeerAddress:   peerAddress,
			ServerAddress: fmt.Sprintf("localhost:%d", 9223+i),
			Peers:         peers,
			Logger:        sugar,
		}
	}

	rafts := make([]*RaftServer, clusterNumber)
	for i, conf := range servers {
		s := Bootstrap(conf)
		go s.Start()
		rafts[i] = s
	}

	var leader *RaftServer

	for {
		for _, s := range rafts {
			time.Sleep(3 * time.Second)
			if s.Ready() && s.node.IsLeader() {
				leader = s
				break
			}
		}
		if leader != nil {
			break
		}
	}

	return peers, rafts, leader
}

func TestClusterVote(t *testing.T) {
	InitServer(3)
}

func TestClusterPropose(t *testing.T) {

	_, _, leader := InitServer(3)
	for i := 0; i < 10000000; i++ {
		key := utils.RandStringBytesRmndr(rand.Intn(10) + 10)
		value := utils.RandStringBytesRmndr(20)
		leader.put(key, value)
	}
}

func TestSql(t *testing.T) {

	_, _, leader := InitServer(3)
	err := leader.ExecSql(context.Background(), `CREATE TABLE `+"`user`"+` (
		`+"`user_id`"+` INT NOT NULL,
		`+"`special_role`"+` VARCHAR DEFAULT NULL,
		`+"`usr_biz_type`"+` VARCHAR DEFAULT NULL,
		`+"`user_code`"+` VARCHAR DEFAULT NULL,
		`+"`nickname`"+` VARCHAR DEFAULT NULL,
		`+"`avatar`"+` VARCHAR DEFAULT NULL,
		`+"`sex`"+` INT DEFAULT NULL,
		`+"`division_code`"+` VARCHAR DEFAULT NULL,
		`+"`detailed_address`"+` VARCHAR DEFAULT NULL ,
		`+"`is_enabled`"+` INT NOT NULL DEFAULT '1',
		PRIMARY KEY (`+"`user_id`"+`),
		UNIQUE KEY user_code_UNIQUE (`+"`user_code`"+`)
	  );`)

	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("创建表完成")
}

func TestClusterMemberChange(t *testing.T) {

	dir := "../../build/"
	peers, _, leader := InitServer(3)

	name := "raft_4"
	peerAddress := fmt.Sprintf("localhost:%d", 9123+3)
	logger := utils.GetLogger(dir + name)
	sugar := logger.Sugar()

	newConf := &Config{
		Dir:           dir,
		Name:          name,
		PeerAddress:   peerAddress,
		ServerAddress: fmt.Sprintf("localhost:%d", 9223+3),
		Peers:         peers,
		Logger:        sugar,
	}

	nodes := make(map[string]string)
	nodes[name] = peerAddress
	peers[name] = peerAddress

	leader.changeMember(nodes, raftpb.MemberChangeType_ADD_NODE)

	sn := Bootstrap(newConf)
	go sn.Start()

	for {
		time.Sleep(5 * time.Second)
		if leader.node.Ready() {
			// leader.changeMember(nodes, raftpb.MemberChangeType_REMOVE_NODE)
			break
		}
	}

	// for i := 0; i < 10000; i++ {
	// 	key := utils.RandStringBytesRmndr(rand.Intn(10) + 10)
	// 	value := utils.RandStringBytesRmndr(20)
	// 	leader.put(key, value)
	// }

}

func TestReadIndex(t *testing.T) {
	_, _, leader := InitServer(3)

	idx, err := leader.readIndex()
	t.Logf("节点最新提交 %d %v", idx, err)
}

func TestGet(t *testing.T) {

	_, _, leader := InitServer(3)

	k := []byte("hello")
	v := []byte("world")

	err := leader.put(k, v)
	if err != nil {
		t.Errorf("写入失败  %v", err)
		return
	}

	for {
		ret, err := leader.get(k)

		t.Logf(" %s 查询结果 %s %v", strconv.FormatUint(leader.id, 16), string(ret), err)
		if ret != nil {
			break
		}
		time.Sleep(3 * time.Second)
	}
}
