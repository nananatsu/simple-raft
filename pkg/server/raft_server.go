package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"kvdb/pkg/clientpb"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
	"kvdb/pkg/utils"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"time"

	_ "net/http/pprof"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"
	"github.com/shirou/gopsutil/v3/process"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type RaftServer struct {
	pb.RaftServer
	clientpb.KvdbServer

	dir           string
	id            uint64
	name          string
	peerAddress   string
	serverAddress string
	raftServer    *grpc.Server
	kvServer      *grpc.Server
	peers         map[uint64]*Peer
	encoding      raft.Encoding
	node          *raft.RaftNode
	storage       *raft.RaftStorage
	cache         map[string]interface{}
	leaderLease   int64 // leader 有效期
	close         bool
	stopc         chan struct{}
	metric        chan pb.MessageType
	logger        *zap.SugaredLogger
}

// 接受节点双向流，用以发送消息
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

// 添加对等节点-双向流
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

func (s *RaftServer) get(key []byte) ([]byte, error) {

	var commitIndex uint64
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if s.node.IsLeader() {
		start := time.Now().UnixNano()
		if start < s.leaderLease {
			commitIndex = s.node.GetLastLogIndex()
		} else {
			commitIndex, err = s.readIndex(ctx)
			if err == nil {
				s.leaderLease = start + int64(s.node.GetElectionTime())*1000000000
			}
		}
	} else {
		commitIndex, err = s.readIndex(ctx)
	}

	if err != nil {
		return nil, err
	}

	err = s.node.WaitIndexApply(ctx, commitIndex)
	if err != nil {
		return nil, err
	}

	return s.storage.GetValue(s.encoding.DefaultPrefix(key)), nil

}

func (s *RaftServer) readIndex(ctx context.Context) (uint64, error) {
	req := make([]byte, 8)
	reqId := utils.NextId(s.id)
	binary.BigEndian.PutUint64(req, reqId)

	err := s.node.ReadIndex(ctx, req)
	if err != nil {
		return 0, err
	}

	for {
		select {
		case resp := <-s.node.ReadIndexNotifyChan():
			if bytes.Equal(req, resp.Req) {
				return resp.Index, nil
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// 添加键值对
func (s *RaftServer) put(key, value []byte) error {

	s.metric <- pb.MessageType_PROPOSE

	data := s.encoding.EncodeLogEntryData(s.encoding.DefaultPrefix(key), value)
	// if err != nil {
	// 	s.logger.Errorf("序列化键值 key: %s ,value: %s 对失败: %v", string(key), string(value), err)
	// 	return err
	// }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return s.node.Propose(ctx, []*pb.LogEntry{{Data: data}})
}

// 变更成员
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return s.node.ChangeMember(ctx, changes)
}

// 通过双向流发送消息
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

// 执行成员变更,两阶段变更，阶段1 同时存在新旧集群，阶段2 清除旧集群
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
		// 先更新集群信息，再关闭连接，防止消息无法处理
		s.node.ApplyChange(changes)
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
}

// 处理消息发送、成员变更
func (s *RaftServer) handle() {
	go func() {
		for {
			select {
			case <-s.stopc:
				return
			case msgs := <-s.node.SendChan():
				s.sendMsg(msgs)
			case changes := <-s.node.MemberChangeNotifyChan():
				go s.applyChange(changes)
			}
		}
	}()
}

// 停止服务
func (s *RaftServer) Stop() {

	s.logger.Infof("关闭服务")

	for {
		select {
		case s.stopc <- struct{}{}:
		case <-time.After(time.Second):
			for _, p := range s.peers {
				if p != nil {
					p.Stop()
				}
			}
			s.node.Close()
			s.raftServer.Stop()
			s.kvServer.Stop()

			close(s.metric)
			close(s.stopc)
			return
		}
	}

}

// 启动服务
func (s *RaftServer) Start() {

	lis, err := net.Listen("tcp", s.peerAddress)
	if err != nil {
		s.logger.Errorf("对等节点服务器失败: %v", err)
	}
	var opts []grpc.ServerOption
	s.raftServer = grpc.NewServer(opts...)

	s.logger.Infof("对等节点服务器启动成功 %s", s.peerAddress)

	pb.RegisterRaftServer(s.raftServer, s)

	s.showMetrics()

	s.handle()

	go s.StartKvServer()

	err = s.raftServer.Serve(lis)
	if err != nil {
		s.logger.Errorf("Raft内部服务器关闭: %v", err)
	}
}

func (s *RaftServer) Ready() bool {
	return s.node.Ready()
}

// 显示监控信息
func (s *RaftServer) showMetrics() {

	appEntryCount := 0
	appEntryResCount := 0
	propCount := 0
	prevAppEntryCount := 0
	prevAppEntryResCount := 0
	prevPropCount := 0
	var prevCommit uint64

	xAxis := make([]string, 0)
	appEntryData := make([]opts.LineData, 0)
	appEntryResData := make([]opts.LineData, 0)
	propData := make([]opts.LineData, 0)
	applyData := make([]opts.LineData, 0)
	pendingData := make([]opts.LineData, 0)

	cpuData := make([]opts.LineData, 0)
	memData := make([]opts.LineData, 0)
	goroutineData := make([]opts.LineData, 0)

	p, _ := process.NewProcess(int32(os.Getpid()))

	render := func() *components.Page {

		srLegend := []string{"AppEntry", "AppEntryResp", "Propose", "Applied", "Pending"}
		nodeLegend := []string{"CPU", "Mem"}
		goroutineLegend := []string{"goroutine"}
		var name string
		if s.node.IsLeader() {
			name = "Leader"
		} else {
			name = "Follower"
		}

		srLine := charts.NewLine()
		srLine.SetGlobalOptions(
			charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeWesteros}),
			charts.WithTitleOpts(opts.Title{Title: fmt.Sprintf("%s接收发送", name)}),
			charts.WithLegendOpts(opts.Legend{Show: true, Data: srLegend}),
		)

		srLine.SetXAxis(xAxis).
			AddSeries("AppEntry", appEntryData).
			AddSeries("AppEntryResp", appEntryResData).
			AddSeries("Propose", propData).
			AddSeries("Applied", applyData).
			AddSeries("Pending", pendingData).
			SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: true}))

		nodeLine := charts.NewLine()
		nodeLine.SetGlobalOptions(
			charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeWesteros}),
			charts.WithTitleOpts(opts.Title{Title: fmt.Sprintf("%s性能", name)}),
			charts.WithLegendOpts(opts.Legend{Show: true, Data: nodeLegend}),
		)

		nodeLine.SetXAxis(xAxis).
			AddSeries("CPU", cpuData).
			AddSeries("Mem", memData).
			SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: true}))

		grLine := charts.NewLine()
		grLine.SetGlobalOptions(
			charts.WithInitializationOpts(opts.Initialization{Theme: types.ThemeWesteros}),
			charts.WithTitleOpts(opts.Title{Title: fmt.Sprintf("%s协程", name)}),
			charts.WithLegendOpts(opts.Legend{Show: true, Data: goroutineLegend}),
		)

		grLine.SetXAxis(xAxis).
			AddSeries("goroutine", goroutineData).
			SetSeriesOptions(charts.WithLineChartOpts(opts.LineChart{Smooth: true}))

		page := components.NewPage()
		page.AddCharts(srLine, nodeLine, grLine)

		return page
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		fd, err := os.OpenFile(path.Join(s.dir, "metric.html"), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			s.logger.Errorf("打开指标记录文件失败", err)
		}
		first := true
		for {

			select {
			case <-s.stopc:
				ticker.Stop()
				return
			case t := <-ticker.C:
				status := s.node.Status()
				if !first {
					xAxis = append(xAxis, t.Format("15:04:05"))
					appEntryData = append(appEntryData, opts.LineData{Value: appEntryCount - prevAppEntryCount})
					appEntryResData = append(appEntryResData, opts.LineData{Value: appEntryResCount - prevAppEntryResCount})
					propData = append(propData, opts.LineData{Value: propCount - prevPropCount})
					applyData = append(applyData, opts.LineData{Value: status.AppliedLogSize - prevCommit})
					pendingData = append(pendingData, opts.LineData{Value: status.PendingLogSize})

					cpuPercent, _ := p.CPUPercent()
					cpuData = append(cpuData, opts.LineData{Value: cpuPercent})

					mp, _ := p.MemoryPercent()
					memData = append(memData, opts.LineData{Value: mp})

					gNum := runtime.NumGoroutine()
					goroutineData = append(goroutineData, opts.LineData{Value: gNum})

					size := len(xAxis)
					if size > 180 {
						start := size - 180
						xAxis = xAxis[start:]
						appEntryData = appEntryData[start:]
						appEntryResData = appEntryResData[start:]
						propData = propData[start:]
						applyData = applyData[start:]
						pendingData = pendingData[start:]
						cpuData = cpuData[start:]
						memData = memData[start:]
						goroutineData = goroutineData[start:]
					}
				} else {
					first = false
				}

				if fd != nil {
					fd.Seek(0, 0)
					render().Render(io.MultiWriter(fd))
				}

				prevAppEntryCount = appEntryCount
				prevAppEntryResCount = appEntryResCount
				prevPropCount = propCount
				prevCommit = status.AppliedLogSize
			}
		}
	}()

	go func() {
		for {
			select {
			case <-s.stopc:
				return
			case t := <-s.metric:
				switch t {
				case pb.MessageType_APPEND_ENTRY:
					appEntryCount++
				case pb.MessageType_APPEND_ENTRY_RESP:
					appEntryResCount++
				case pb.MessageType_PROPOSE:
					propCount++
				}
			}
		}
	}()
}
