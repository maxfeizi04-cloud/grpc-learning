package main

import (
	"context"
	"fmt"
	pb "grpc_learning/proto"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ──────────────────────────────────────
// 服务结构体
// ──────────────────────────────────────

type streamDemoServer struct {
	pb.UnimplementedStreamDemoServer

	// 聊天室相关
	mu      sync.RWMutex
	clients map[string]chan *pb.ChatResponse // 用户名 -> 消息通道
}

func newServer() *streamDemoServer {
	return &streamDemoServer{
		clients: make(map[string]chan *pb.ChatResponse),
	}
}

// 1. 一元调用
// 最简单的模式: 收到一个请求,返回一个响应

func (s *streamDemoServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Printf("[Unary] 收到 Ping: %q", req.GetMessage())

	// 检查 context 是否已取消(客户端可能断开)
	select {
	case <-ctx.Done():
		return nil, status.Error(codes.Canceled, "客户端已取消")
	default:
	}

	return &pb.PingResponse{
		Reply:      fmt.Sprintf("Pong: %s", req.GetMessage()),
		ServerTime: time.Now().Format("2006-01-02 15:04:05.000"),
	}, nil
}

// 2. 服务端流
// 客户端发一个请求,服务端持续返回多个响应

func (s *streamDemoServer) CountDown(req *pb.CountRequest, stream pb.StreamDemo_CountDownServer) error {
	start := int(req.GetStart())
	end := int(req.GetEnd())

	log.Printf("[ServerStream] 倒计数请求: %d -> %d", start, end)

	// 参数校验
	if start < end {
		return status.Errorf(codes.InvalidArgument, "起始值 %d 应大于结束值 %d", start, end)
	}

	for i := start; i >= end; i-- {
		// 检查客户端是否断开
		select {
		case <-stream.Context().Done():
			log.Printf("[ServerStream] 客户端断开,停止发送")
			return status.Error(codes.Canceled, "客户端断开")
		default:
		}

		// 发送一个响应
		resp := &pb.CountResponse{
			Current:   int32(i),
			Remaining: int32(i - end),
			Timestamp: time.Now().Format("15:04:05.000"),
		}
		if err := stream.Send(resp); err != nil {
			// Send 失败通常是网络问题
			log.Printf("[ServerStream] Send 失败: %v", err)
			return err
		}

		log.Printf("[ServerStream] 发送: %d (剩余 %d)", i, i-end)

		// 模拟延迟(不要在最后一个数后等)
		if i > end {
			time.Sleep(500 * time.Millisecond)
		}
	}
	log.Printf("[ServerStream] 倒计数完成")
	return nil // 返回 nil 表示流正常结束
}

// 3. 客户端流
// 客户端持续发送多个请求,服务端接收完毕后返回一个响应
// 关键点:
//	1. 用 stream.Recv() 循环接收请求
//	2. 当 err == io.EOF 时,表示客户端发送完毕
//	3. stream.SendAndClose() 发送最终响应并关闭流

func (s *streamDemoServer) ComputeAverage(stream pb.StreamDemo_ComputeAverageServer) error {
	var sum float64
	var count int32

	log.Printf("[ClientStream] 开始接收数字...")

	for {
		// 接收客户端发送来的消息
		req, err := stream.Recv()
		if err == io.EOF {
			// 客户端正常发送完毕
			average := float64(0)
			if count > 0 {
				average = sum / float64(count)
			}

			log.Printf("[ClientStream] 接收完毕: 共 %d 个数字,总和 %.2f,平均值 %.2f", count, sum, average)

			// SendAndClose: 发送响应并关闭流
			return stream.SendAndClose(&pb.AverageResponse{
				Average: math.Round(average*100) / 100,
				Count:   count,
				Sum:     math.Round(sum*100) / 100,
			})
		}
		if err != nil {
			// 其他错误
			log.Printf("[ClientStream] Recv 错误: %v", err)
			return status.Errorf(codes.Internal, "接收失败: %v", err)
		}

		// 累加
		sum += req.GetNumber()
		count++
		log.Printf("[ClientStream] 收到: %.2f (累计 %d 个,总和 %.2f)", req.GetNumber(), count, sum)
	}
}

// 4. 双向流 Bidirectional RPC
// 双方同时发生和接收,完成独立,完全异步
// 关键点:
// 	1. 通常用 goroutine 分离发送和接收
//	2. 服务端可以向所有客户端广播
//	3. 客户端关闭发送后,服务端应优雅退出

func (s *streamDemoServer) LiveChat(stream pb.StreamDemo_LiveChatServer) error {
	// 第一条消息作为 "加入聊天室"
	firstMsg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "必须先发送用户名: %v", err)
	}

	username := firstMsg.GetUsername()
	if username == "" {
		return status.Error(codes.InvalidArgument, "用户名不能为空")
	}

	log.Printf("[BidiStream] %s 加入聊天室", username)

	// 注册客户端
	msgChan := make(chan *pb.ChatResponse, 64)
	s.mu.Lock()
	s.clients[username] = msgChan
	onlineCount := len(s.clients)
	s.mu.Unlock()

	// 通知所有人
	s.broadcast(&pb.ChatResponse{
		Username:    "System",
		Content:     fmt.Sprintf("%s 加入了聊天室", username),
		ServerTime:  time.Now().Format("15:04:05"),
		OnlineCount: int32(onlineCount),
	}, username)

	// 确保退出时清理
	defer func() {
		s.mu.Lock()
		delete(s.clients, username)
		onlineCount := len(s.clients)
		s.mu.Unlock()
		close(msgChan)

		s.broadcast(&pb.ChatResponse{
			Username:    "Sysyem",
			Content:     fmt.Sprintf("%s 离开了聊天室", username),
			ServerTime:  time.Now().Format("15:04:05"),
			OnlineCount: int32(onlineCount),
		}, username)
	}()

	// 启动 goroutine: 从消息通道读取消息,发送给客户端
	// 这是 "服务端 -> 客户端" 方向
	errChan := make(chan error, 1)
	go func() {
		for resp := range msgChan {
			if err := stream.Send(resp); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// 主循环: 接收客户端消息
	// 这是 "客户端 -> 服务端" 方向
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// 客户端关闭了发送
			log.Printf("[BidiStream] %s 关闭了发送", username)
			return nil
		}
		if err != nil {
			log.Printf("[BidiStream] %s Recv 错误: %v", username, err)
			return err
		}

		// 检查发送 goroutine 是否有错误
		select {
		case sendErr := <-errChan:
			return sendErr
		default:
		}

		log.Printf("[BidiStream] %s 说 %s", username, req.GetContent())

		// 广播给所有人
		s.broadcast(&pb.ChatResponse{
			Username:   username,
			Content:    req.GetContent(),
			ServerTime: time.Now().Format("15:04:05"),
		}, "")
	}
}

// 5. broadcast 广播消息给所有客户端

func (s *streamDemoServer) broadcast(msg *pb.ChatResponse, excludeUser string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for user, ch := range s.clients {
		if user == excludeUser {
			continue
		}
		// 非阻塞发送, 防止慢客户端阻塞整个广播
		select {
		case ch <- msg:
		default:
			log.Printf("[广播] %s 的消息队列已满", user)
		}
	}
}

// 主函数

func main() {
	lis, err := net.Listen("tcp", "127.0.0.1:50051")
	if err != nil {
		log.Fatalf("监听失败: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterStreamDemoServer(grpcServer, newServer())

	log.Println("=======================================")
	log.Println("  StreamDemo gRPC 服务启动")
	log.Println("  端口: :50051")
	log.Println("=======================================")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
