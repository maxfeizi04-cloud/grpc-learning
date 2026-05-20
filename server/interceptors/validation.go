package main

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 定义一个校验接口，让请求消息自己实现
// Validate() error 返回 nil 表示通过，否则返回错误原因

type Validator interface {
	Validate() error
}

// UnaryValidationInterceptor 自动校验实现了 Validator 接口的请求
func UnaryValidationInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	// 前置: 如果请求实现了 Validator 接口，自动调用校验
	if v, ok := req.(Validator); ok {
		if err := v.Validate(); err != nil {
			// 参数校验不通过 → 直接返回 InvalidArgument 错误
			// 不会进入后面的拦截器和业务方法
			return nil, status.Errorf(codes.InvalidArgument, "参数校验失败: %v", err)
		}
	}

	// 校验通过，继续执行
	return handler(ctx, req)
}
