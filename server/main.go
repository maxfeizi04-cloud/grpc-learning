package main

import (
	"context"
	"fmt"
	pb "grpc_learning/proto"
	"log"
	"net"
	"strings"
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

// SearchBooks 按书名模糊搜索
// ====================================================================
// 思路：遍历内存中所有书籍，用 strings.Contains 判断书名是否包含关键词
//
//	命中则加入结果切片，最后返回
//
// ====================================================================
func (s *bookStoreServer) SearchBooks(ctx context.Context, req *pb.SearchBooksRequest) (*pb.SearchBooksResponse, error) {
	// 参数校验
	if req.GetKeyword() == "" {
		return nil, status.Error(codes.InvalidArgument, "搜索关键字不能为空")
	}
	// 加读锁: 并发安全地读取 books map
	// 用 RLock 而不是 Lock, 因为搜索是"只读"操作,不修改数据
	// 多个搜索请求可以同时执行,互不阻塞
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 遍历所有书籍,逐个匹配
	var results []*pb.Book
	for _, book := range s.books {
		// strings.Contains(str, substr) 判断 str 中是否包含 substr
		// 这里判断 book 的 title 中是否包含用户输入的关键词
		// 注意：这里是子串匹配，不是正则；区分大小写
		if strings.Contains(book.GetTitle(), req.GetKeyword()) {
			results = append(results, book)
		}
	}

	log.Printf("[SearchBooks] 关键词: %q, 匹配到: %d 本", req.GetKeyword(), len(results))

	return &pb.SearchBooksResponse{
		Books: results,
		Total: int32(len(results)),
	}, nil

}

// ====================================================================
// 练习 2（进阶）：BorrowBook —— 借书（库存 -1 或 -quantity）
// ====================================================================
// 思路：
//   1. 校验参数（ID、数量必须 > 0）
//   2. 加写锁（会修改库存）
//   3. 查找书籍，检查库存是否充足
//   4. 库存 >= quantity → 扣减库存，返回成功
//   5. 库存 < quantity → 返回 FailedPrecondition 错误
// ====================================================================

func (s *bookStoreServer) BorrowBook(ctx context.Context, req *pb.BorrowBookRequest) (*pb.BorrowBookResponse, error) {
	// 参数校验
	if req.GetBookId() == "" {
		return nil, status.Error(codes.InvalidArgument, "书籍 ID 不能为空")
	}
	quantity := req.GetQuantity()
	if quantity <= 0 {
		quantity = 1
	}
	// 加写锁: 要修改库存,必须用 Lock (排他锁), 不能用 RLock
	s.mu.Lock()
	defer s.mu.Unlock()

	// 查找书籍
	book, ok := s.books[req.GetBookId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "书籍 %s 不存在", req.GetBookId())
	}

	// 检查库存是否充足
	if book.GetStock() < quantity {
		// codes.FailedPrecondition = 前置条件不满足
		// 适合"操作本身合法，但当前状态不允许"的场景
		// 比如：借书是合法操作，但库存不够这个前置条件没满足
		return nil, status.Errorf(codes.FailedPrecondition, "库存不足: 当前库存 %d, 想借 %d", book.GetStock(), quantity)
	}

	// 库存减扣
	book.Stock = book.GetStock() - quantity

	// 拼接提示信息
	msg := fmt.Sprintf("借出 %d 本, 当前剩余库存 %d", quantity, book.GetStock())
	log.Printf("[BorrowBook] %s: %s", req.GetBookId(), msg)

	// 返回更新后的书籍消息
	return &pb.BorrowBookResponse{
		Book:    book,
		Message: msg,
	}, nil
}

// ====================================================================
// 练习 2（进阶）：ReturnBook —— 还书（库存 +1 或 +quantity）
// ====================================================================
// 思路：
//   1. 校验参数
//   2. 加写锁
//   3. 查找书籍
//   4. 增加库存（还书没有上限检查，除非你想设最大库存）
// ====================================================================

func (s *bookStoreServer) ReturnBook(ctx context.Context, req *pb.ReturnBookRequest) (*pb.ReturnBookResponse, error) {
	// 参数校验
	if req.GetBookId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "书籍 ID 不能为空")
	}

	// 还书数量,默认为 0
	quantity := req.GetQuantity()
	if quantity < 0 {
		quantity = 0
	}

	// 加写锁
	s.mu.Lock()
	defer s.mu.Unlock()

	// 查找书籍
	book, ok := s.books[req.GetBookId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "书籍 %s 不存在", req.GetBookId())
	}

	// 增加库存
	book.Stock = quantity + book.GetStock()

	msg := fmt.Sprintf("归还 %d 本, 当亲库存 %d", quantity, book.GetStock())
	log.Printf("[ReturnBook] %s: %s", req.GetBookId(), msg)

	return &pb.ReturnBookResponse{
		Book:    book,
		Message: msg,
	}, nil
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
