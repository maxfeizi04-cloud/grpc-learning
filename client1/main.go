package main

import (
	"context"
	"fmt"
	pb "grpc_learning/proto"
	"io"
	"log"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 辅助函数
func separator(title string) {
	fmt.Println()
	log.Println("═══════════════════════════════════════")
	log.Printf("  %s", title)
	log.Println("═══════════════════════════════════════")
}

func main() {
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	client := pb.NewStreamDemoClient(conn)
	ctx := context.Background()

	// 1. Unary RPC 演示
	demoUnary(ctx, client)

	// 2. Server Streaming 演示
	demoServerStreaming(ctx, client)

	// 3. Client Streaming 演示
	demoClientStreaming(ctx, client)

	// 4. Bidirectional Streaming 演示
	demoBidirectional(ctx, client)

}

// Unary RPC
// 最基础的调用方式,和 HTTP 请求类似

func demoUnary(ctx context.Context, client pb.StreamDemoClient) {
	separator("1. Unary RPC - 一问一答")

	// 连发 3 次 Ping
	messages := []string{"Hello", "你好 gRPC", "第一次调用"}
	for _, msg := range messages {
		resp, err := client.Ping(ctx, &pb.PingRequest{
			Message: msg,
		})
		if err != nil {
			log.Printf("  Ping 失败: %v", err)
			continue
		}
		log.Printf("  发送: %q", msg)
		log.Printf("  收到: %q (服务端时间: %s)", resp.GetReply(), resp.GetServerTime())
		fmt.Println()
	}
}

// Server Streaming RPC
// 客户端发一个请求，然后持续接收多个响应
func demoServerStreaming(ctx context.Context, client pb.StreamDemoClient) {
	separator("2. Server Streaming - 服务端倒计数")

	// 1. 发起请求,获取流对象
	stream, err := client.CountDown(ctx, &pb.CountRequest{
		Start: 10,
		End:   5,
	})
	if err != nil {
		log.Printf("  发起失败: %v", err)
		return
	}

	log.Printf("  开始倒计数: 10 -> 5")
	fmt.Println()

	// 2. 循环接收
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			// 流程正常结束
			log.Printf("  倒计数完成!")
		}
		if err != nil {
			log.Printf("  接收出错: %v", err)
			return
		}

		log.Printf("  收到: %d | 剩余: %d | 时间: %s", resp.GetCurrent(), resp.GetRemaining(), resp.GetTimestamp())

	}
}

// 3. Client Streaming RPC
// 客户端持续发送多个请求，最后接收一个响应

func demoClientStreaming(ctx context.Context, client pb.StreamDemoClient) {
	separator("3. Client Streaming - 计算平均值")

	// 1. 发起流
	stream, err := client.ComputeAverage(ctx)
	if err != nil {
		log.Printf("  发起失败: %v", err)
		return
	}

	// 2. 发送一串数字
	numbers := []float64{3.5, 8.0, 2.7, 9.3, 6.1, 4.8, 7.2}
	log.Printf("  发送数字: %v", numbers)
	fmt.Println()

	for i, num := range numbers {
		err := stream.Send(&pb.NumberRequest{Number: num})
		if err != nil {
			log.Printf("  Send 失败 (地 %d): %v", i+1, err)
			return
		}
		log.Printf("  已发送: %.2f", num)
		time.Sleep(200 * time.Millisecond) // 模拟间隔
	}

	// 3. 关闭发送,接收响应
	// CloseAnaRecv = CloseSend + Recv
	// 它会先通知服务端"我发完了"，然后等待服务端的最终响应
	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Printf("  CloseAndRecv 失败: %v", err)
		return
	}

	fmt.Println()
	log.Printf("  计算结果")
	fmt.Printf("  数字个数: %d |", resp.GetCount())
	fmt.Printf("  总   和: %.2f |", resp.GetSum())
	fmt.Printf("  平 均 数: %.2f ", resp.GetAverage())

}

// 4. Bidirectional Streaming RPC
// 双方同时发送和接收，完全独立
// 客户端关键操作：
//   1. 调用 RPC 方法，获得一个 stream 对象
//   2. 启动 goroutine 负责接收（stream.Recv）
//   3. 主 goroutine 负责发送（stream.Send）
//   4. 发送完毕后调用 stream.CloseSend() 通知服务端
//
// 为什么要用 goroutine？
//   因为 Recv 是阻塞的，Send 也是阻塞的
//   如果在同一个 goroutine 里先 Recv 再 Send，就变成了一问一答
//   用两个 goroutine 可以实现真正的异步双向通信

func demoBidirectional(ctx context.Context, client pb.StreamDemoClient) {
	separator("4. Bidirectional Streaming — 实时聊天")

	// 1. 发起双向流
	stream, err := client.LiveChat(ctx)
	if err != nil {
		log.Printf("  发起失败: %v", err)
		return
	}

	// 2. 发送第一条消息(作为"加入聊天室")
	username := fmt.Sprintf("用户%d", rand.Intn(1000))
	err = stream.Send(&pb.ChatRequest{
		Username: username,
		Content:  "我来了!",
	})
	if err != nil {
		log.Printf("  发送失败: %v", err)
		return
	}
	log.Printf(" [%s] 加入聊天室", username)

	// 3. 启动 goroutine 接收消息
	doneChan := make(chan struct{})
	go func() {
		defer close(doneChan)
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				log.Printf("  服务端关闭了连接")
				return
			}
			if err != nil {
				log.Printf("  接收错误: %v", err)
				return
			}
			log.Printf("  [%s] %s (在线: %d)", resp.GetUsername(), resp.GetContent(), resp.GetOnlineCount())
		}
	}()

	// 4. 主 goroutine 发送消息
	message := []string{
		"大家好!我是肥子.",
		"gRPC 好用吗",
		"双向流泰酷辣!",
		"再见!",
	}
	for _, msg := range message {
		time.Sleep(800 * time.Millisecond)

		err := stream.Send(&pb.ChatRequest{
			Username: username,
			Content:  msg,
		})
		if err != nil {
			log.Printf("  发送失败: %v", err)
			break
		}
		log.Printf("  [%s] -> %s", username, msg)
	}

	// 5. 关闭发送方向
	// CloseSend 只关闭发送，不影响接收
	// 服务端会收到 io.EOF，知道客户端不再发了
	stream.CloseSend()

	// 等待接收 goroutine 结束
	<-doneChan
	log.Printf("  聊天结束")
}
