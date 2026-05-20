package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryLoggingInterceptor 是一个 UnaryServerInterceptor
// 它记录每个 RPC 调用的请求、响应、耗时和错误信息
func UnaryLoggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	// === 前置: 记录请求开始 ===
	start := time.Now()
	log.Printf("[>>>>>] 收到请求: %s", info.FullMethod)

	// === 调用下一层(可能是下一个拦截器,也可能是真正的 RPC 方法 ===
	resp, err := handler(ctx, req)

	// === 后置: 记录请求完成 ===
	duration := time.Since(start)
	if err != nil {
		// 从 error 中 提取 gRPC 状态码
		st := status.Convert(err)
		log.Printf("[<<<<<] %s 失败 | code=%s | 耗时=%v | 错误=%s",
			info.FullMethod,
			st.Code(),
			duration,
			st.Message())
	} else {
		log.Printf("[<<<<<] %s 成功 | 耗时=%v",
			info.FullMethod,
			duration,
		)
	}
	return resp, err
}
