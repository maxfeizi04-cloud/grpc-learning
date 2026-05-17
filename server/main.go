package main

import (
	"context"
	"fmt"
	pb "grpc_learning/proto"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ----- 服务实现 -----
type bookStoreServer struct {
	pb.UnimplementedBookstoreServer
	mu     sync.RWMutex
	books  map[string]*pb.Book
	nextID int
}

func newServer() *bookStoreServer {
	return &bookStoreServer{
		books:  make(map[string]*pb.Book),
		nextID: 1,
	}
}

// CreateBook 创建书籍
func (s *bookStoreServer) CreateBook(ctx context.Context, req *pb.CreateBookRequest) (*pb.CreateBookResponse, error) {
	// 参数校验
	if req.GetTitle() == "" {
		return nil, status.Error(codes.InvalidArgument, "书名不能为空")
	}
	if req.GetAuthor() == "" {
		return nil, status.Error(codes.InvalidArgument, "作者不能为空")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	id := fmt.Sprintf("BOOK-%04d", s.nextID)
	s.nextID++
	book := &pb.Book{
		Id:          id,
		Title:       req.GetTitle(),
		Author:      req.GetAuthor(),
		Category:    req.GetCategory(),
		PriceCents:  req.GetPriceCents(),
		Stock:       req.GetStock(),
		Tags:        req.GetTags(),
		PublishedAt: timestamppb.Now(),
		Metadata:    make(map[string]string),
	}
	s.books[id] = book

	log.Printf("[CreateBook] 创建成功: %s - %s", id, req.GetTitle())

	return &pb.CreateBookResponse{
		Book:    book,
		Message: fmt.Sprintf("书籍创建成功"),
	}, nil
}

// GetBook 查询书籍
func (s *bookStoreServer) GetBook(ctx context.Context, req *pb.GetBookRequest) (*pb.GetBookResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "书籍 ID 不能为空")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	book, ok := s.books[req.GetId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "书籍不存在", req.GetId())
	}
	return &pb.GetBookResponse{Book: book}, nil
}

// ListBooks 列出书籍
func (s *bookStoreServer) ListBooks(ctx context.Context, req *pb.ListBooksRequest) (*pb.ListBooksResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	page := int(req.GetPage())
	pageSize := int(req.GetPageSize())
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}

	// 筛选
	var filtered []*pb.Book
	for _, book := range s.books {
		if req.GetCategory() != pb.BookCategory_BOOK_CATEGORY_UNSPECIFIED && book.GetCategory() != req.GetCategory() {
			continue
		}
		filtered = append(filtered, book)
	}

	// 分页
	total := len(filtered)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}

	return &pb.ListBooksResponse{
		Books: filtered[start:end],
		Total: int32(total),
	}, nil
}

// UpdateStock 更新库存
func (s *bookStoreServer) UpdateStock(ctx context.Context, req *pb.UpdateStockRequest) (*pb.UpdateStockResponse, error) {
	if req.GetBookId() == "" {
		return nil, status.Error(codes.InvalidArgument, "书籍 ID 不能为空")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	book, ok := s.books[req.GetBookId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "书籍 %s 不存在", req.GetBookId())
	}

	newStock := book.GetStock() + req.GetQuantityChange()
	if newStock < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "库存不足: 当前 %d, 出库 %d", book.GetStock(), -req.GetQuantityChange())
	}
	book.Stock = newStock

	action := "入库"
	if req.GetQuantityChange() < 0 {
		action = "出库"
	}
	msg := fmt.Sprintf("%s %d 本,当前库存 %d", action, abs(req.GetQuantityChange()), newStock)
	log.Printf("[UpdateStock %s: %s", req.GetBookId(), msg)

	return &pb.UpdateStockResponse{
		Book:    book,
		Message: msg,
	}, nil
}

// DeleteBook 删除书籍
func (s *bookStoreServer) DeleteBook(ctx context.Context, req *pb.DeleteBookRequest) (*emptypb.Empty, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "书籍 ID 不能为空")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.books[req.GetId()]; !ok {
		return nil, status.Errorf(codes.NotFound, "书籍 %s 不存在", req.GetId())
	}
	delete(s.books, req.GetId())
	log.Printf("[DeleteBook] 删除: %s", req.GetId())
	return &emptypb.Empty{}, nil
}

func abs(n int32) int32 {
	if n < 0 {
		return -n
	}
	return n
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("监听失败: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterBookstoreServer(grpcServer, newServer())

	// 注册反射服务(方便 grpcurl 调试)
	reflection.Register(grpcServer)

	log.Println("=== BookStore gRPC 服务启动 ===")
	log.Println("监听端口: :50051")
	log.Println("调试命令: grpcurl -plaintext localhost:50051 list")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
