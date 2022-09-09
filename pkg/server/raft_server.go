package server

import (
	"fmt"
	"io"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
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
	"google.golang.org/protobuf/proto"
)

type RaftServer struct {
	pb.RaftInternalServer

	dir    string
	id     uint64
	name   string
	bind   string
	server *grpc.Server
	peer   map[uint64]*Peer
	node   *raft.RaftNode
	stroge raft.Storage
	close  bool
	stopc  chan struct{}
	metric chan pb.MessageType
	logger *zap.SugaredLogger
}

func (s *RaftServer) Send(stream pb.RaftInternal_SendServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		p := s.peer[msg.From]

		if p != nil {
			s.metric <- msg.MsgType
			p.Process(msg)
		} else {
			s.logger.Errorf("收到非集群节点 %s消息 %s", strconv.FormatUint(msg.From, 16), msg.String())
		}

	}
}

func (s *RaftServer) Put(key, value []byte) {
	s.node.Propose([]*pb.LogEntry{{Data: raft.Encode(key, value)}})
}

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

	changeCol := &pb.MemberChangeCollection{Changes: changes}

	data, err := proto.Marshal(changeCol)
	if err != nil {
		s.logger.Errorf("序列化变更成员信息失败: %v", err)
		return err
	}
	s.node.Propose([]*pb.LogEntry{{Type: pb.EntryType_MEMBER_CHNAGE, Data: data}})

	return nil
}

func (s *RaftServer) AddNode(peers map[string]string) {

	s.changeMember(peers, pb.MemberChangeType_ADD_NODE)
}

func (s *RaftServer) RemoveNode(peers map[string]string) {

	s.changeMember(peers, pb.MemberChangeType_REMOVE_NODE)
}

func (s *RaftServer) Ready() bool {
	return s.node.Ready()
}

func (s *RaftServer) IsLeader() bool {
	return s.node.IsLeader()
}

func (s *RaftServer) SendMsg(msgs []*pb.RaftMessage) {

	msgMap := make(map[uint64]*pb.RaftMessage, len(s.peer)-1)
	propMap := make(map[uint64]*pb.RaftMessage, len(s.peer)-1)
	otherMsg := make([]*pb.RaftMessage, 0)

	for i, msg := range msgs {
		if s.peer[msg.To] == nil {
			s.logger.Errorf("节点未连接 %s, 发送消息失败", strconv.FormatUint(msg.To, 16))
			continue
		}
		if msg.MsgType == pb.MessageType_APPEND_ENTRY {
			if msgMap[msg.To] == nil {
				msgMap[msg.To] = msg
			} else {
				entry := msgMap[msg.To].Entry
				if entry[len(entry)-1].Index+1 == msg.Entry[0].Index {
					msgMap[msg.To].LastCommit = msg.LastCommit
					msgMap[msg.To].Entry = append(msgMap[msg.To].Entry, msg.Entry...)
					msgs[i] = nil
				} else if entry[0].Index >= msg.Entry[0].Index {
					msgMap[msg.To] = msg
				}
			}
		} else if msg.MsgType == pb.MessageType_PROPOSE {
			if propMap[msg.To] == nil {
				propMap[msg.To] = msg
			} else {
				propMap[msg.To].Entry = append(propMap[msg.To].Entry, msg.Entry...)
				msgs[i] = nil
			}
		} else {
			otherMsg = append(otherMsg, msg)
		}
	}

	for _, msg := range otherMsg {
		s.metric <- msg.MsgType
		s.peer[msg.To].Send(msg)
	}
	for _, msg := range msgMap {
		s.metric <- msg.MsgType
		s.peer[msg.To].Send(msg)
	}
	for _, msg := range propMap {
		s.metric <- msg.MsgType
		s.peer[msg.To].Send(msg)
	}
}

func (s *RaftServer) pollMsg() {
	for {
		select {
		case <-s.stopc:
			return
		case msgs := <-s.node.Poll():
			s.SendMsg(msgs)
		case changes := <-s.stroge.Notify():
			s.node.ChangeMember(changes)
			if len(changes) > 0 {
				for _, mc := range changes {
					if mc.Type == pb.MemberChangeType_ADD_NODE {
						s.peer[mc.Id] = NewPeer(mc.Id, mc.Address, s.node, s.logger)
					} else {
						if mc.Id != s.id {
							s.peer[mc.Id].close = true
						} else {
							s.close = true
						}
					}
				}
				if s.node.IsLeader() {
					s.logger.Debugf("新集群配置已提交到集群")
					s.node.Propose([]*pb.LogEntry{{Type: pb.EntryType_MEMBER_CHNAGE}})
				}
			} else {
				for k, p := range s.peer {
					if p.close {
						p.Stop()
						delete(s.peer, k)
					}
				}
				if s.close {
					s.Stop()
				}
			}
		}
	}
}

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
		if s.IsLeader() {
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

	// go func() {
	// 	mux := http.NewServeMux()
	// 	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
	// 		render().Render(w)
	// 	})

	// 	srv := http.Server{
	// 		Addr:    fmt.Sprintf(":920%d", s.id),
	// 		Handler: mux,
	// 	}
	// 	srv.ListenAndServe()
	// }()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		fd, err := os.OpenFile(path.Join(s.dir, "metric.html"), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			s.logger.Errorf("打开指标记录文件失败", err)
		}
		first := true
		for {
			t := <-ticker.C
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

			// if s.node.IsLeader() {
			// 	s.logger.Infof("发送: %6d, 接收:(响应: %6d, 提议:%6d), 未响应: %6d, 提交: %6d, 已提交: %6d, 未提交： %6d, 接收通道: %6d, 提议通道: %6d, 发送通道: %6d",
			// 		appEntryResCount-prevAppEntryResCount, propCount-prevPropCount, appEntryCount-prevAppEntryCount, appEntryCount-appEntryResCount,
			// 		status.AppliedLogSize-prevCommit, status.AppliedLogSize, status.PendingLogSize,
			// 		status.RecvcSize, status.PropcSize, status.SendcSize)
			// } else {
			// 	s.logger.Infof("接收: %6d, 发送:(响应: %6d, 提议:%6d), 未响应: %6d, 提交: %6d, 已提交: %6d, 未提交： %6d, 接收通道: %6d, 提议通道: %6d, 发送通道: %6d",
			// 		appEntryCount-prevAppEntryCount, appEntryResCount-prevAppEntryResCount, propCount-prevPropCount, appEntryCount-appEntryResCount,
			// 		status.AppliedLogSize-prevCommit, status.AppliedLogSize, status.PendingLogSize,
			// 		status.RecvcSize, status.PropcSize, status.SendcSize)
			// }
			if fd != nil {
				fd.Seek(0, 0)
				render().Render(io.MultiWriter(fd))
			}

			prevAppEntryCount = appEntryCount
			prevAppEntryResCount = appEntryResCount
			prevPropCount = propCount
			prevCommit = status.AppliedLogSize
		}
	}()

	go func() {
		for {
			t := <-s.metric
			switch t {
			case pb.MessageType_APPEND_ENTRY:
				appEntryCount++
			case pb.MessageType_APPEND_ENTRY_RESP:
				appEntryResCount++
			case pb.MessageType_PROPOSE:
				propCount++
			}
		}
	}()
}

func (s *RaftServer) Stop() {

	s.logger.Infof("关闭服务")
	s.server.Stop()
	s.stopc <- struct{}{}

	for _, p := range s.peer {
		if p != nil {
			p.Stop()
		}
	}
	close(s.metric)
	close(s.stopc)
	s.node.Close()
	s.stroge.Close()
}

func (s *RaftServer) Start() {

	// flag.Parse()
	lis, err := net.Listen("tcp", s.bind)
	if err != nil {
		s.logger.Errorf("启动服务器失败: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	s.logger.Infof("服务器启动成功 %s", s.bind)

	pb.RegisterRaftInternalServer(grpcServer, s)

	go grpcServer.Serve(lis)
	go s.pollMsg()
}
