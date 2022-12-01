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
	_, err := leader.ExecSql(context.Background(), `CREATE TABLE `+"`user`"+` (
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
	<-time.After(16 * time.Second)
}

func BenchmarkInsert(b *testing.B) {

	_, _, leader := InitServer(3)
	_, err := leader.ExecSql(context.Background(), `CREATE TABLE `+"`user`"+` (
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
		b.Error(err)
		return
	}

	b.Logf("创建表完成")
	// <-time.After(16 * time.Second)

	for i := 0; i < 100000; i++ {
		sqlstr := `INSERT INTO user
		(user_id, special_role, usr_biz_type, user_code, nickname, avatar, sex, division_code, detailed_address, is_enabled) VALUES`
		for j := 1; j <= 10; j++ {
			endRune := ","
			if j == 10 {
				endRune = ";"
			}
			sqlstr += fmt.Sprintf("(%d, %s, %s, %s, %s, %s, %d, %s, %s, %d)%s",
				int64(i*10+j), utils.RandStringBytesRmndr(10), utils.RandStringBytesRmndr(8), utils.RandStringBytesRmndr(8), utils.RandStringBytesRmndr(10),
				utils.RandStringBytesRmndr(16), j%2, utils.RandStringBytesRmndr(8), utils.RandStringBytesRmndr(8), rand.Intn(10)%2, endRune)
		}

		_, err := leader.ExecSql(context.Background(), sqlstr)
		if err != nil {
			b.Error(err)
			return
		}
	}

	<-time.After(16 * time.Second)
}

func TestSqlInsert(t *testing.T) {

	_, _, leader := InitServer(3)

	_, err := leader.ExecSql(context.Background(), `INSERT INTO user
	(user_id, special_role, usr_biz_type, user_code, nickname, avatar, sex, division_code, detailed_address, is_enabled) VALUES
	(1344453118192123905, 'NULL', 'User', 'dev02', '测试帐号', 'useravatar/1344453118192123905.png', 0, 'NULL', 'NULL', 1),
	(1387292243975573506, 'NULL', 'NULL', 'llx', 'llx', 'useravatar/1382886374490771458.jpg', 0, 'NULL', 'NULL', 1),
	(1387928161757532162, 'NULL', 'NULL', 'all', 'all', 'useravatar/1387928161757532162.png', 0, '12', '12121', 1),
	(1538692496857780225, 'NULL', 'NULL', 'orchidaceae', '兰科', 'NULL', 1, '53', '1', 1),
	(1538694217348698113, 'NULL', 'Admin', 'oncidium', '文心兰属', 'NULL', 1, '54', '2', 1),
	(1538695477411401729, 'NULL', 'User', 'cattleya', '嘉德丽雅兰属', 'NULL', 1, '54', '3', 1),
	(1538695877573169153, 'NULL', 'Admin', 'thrixspermum', '风铃兰属', 'NULL', 1, '54', '7', 1),
	(1538696329983381506, 'NULL', 'Admin', 'acampe', '脆兰属', 'NULL', 1, '54', '6', 1),
	(1538696715012100098, 'NULL', 'User', 'amitostigma', '雏兰属', 'NULL', 1, '54', '5', 1),
	(1538697116184694786, 'NULL', 'User', 'dendrobium', '石斛属', 'NULL', 1, '54', '9', 1),
	(1538697561175183362, 'NULL', 'User', 'freesia', '小苍兰', 'useravatar/1538697561175183362.jpeg', 1, '54', '日咯泽', 1),
	(6734283068724477978, 'NULL', 'User', 'dev04', 'dev04', 'NULL', 0, 'NULL', 'NULL', 1),
	(6734283068724477980, 'NULL', 'User', 'lhc', 'lhc', 'NULL', 0, 'NULL', 'NULL', 1);`)

	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("插入数据完成")

	<-time.After(16 * time.Second)
}

func TestSqlSelect(t *testing.T) {

	_, _, leader := InitServer(3)

	rets, err := leader.ExecSql(context.Background(), `select user_id FROM user where user_id = '12345' order BY user_id ;`)

	if err != nil {
		t.Error(err)
	}

	if rets[0].SelecResult[0][0] != "12345" {
		t.Error("查询结果不一致")
	}

}

func TestSqlSelectCount(t *testing.T) {

	_, _, leader := InitServer(3)

	rets, err := leader.ExecSql(context.Background(), `select COUNT(*),MAX(user_id),MIN(user_id) FROM user where sex=1 or sex=0 order BY user_id;`)

	if err != nil {
		t.Error(err)
	}

	for _, v := range rets[0].SelecResult {
		t.Log(v)
	}

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

	idx, err := leader.readIndex(context.Background())
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
