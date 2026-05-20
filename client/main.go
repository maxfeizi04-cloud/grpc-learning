package main

import (
	"context"
	pb "grpc_learning/proto"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. 建立连接
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	// 2. 创建客户端
	client := pb.NewBookstoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 3. 创建书籍
	log.Println("========== 创建书籍 ===========")

	books := []*pb.CreateBookRequest{
		{
			Title:      "Go 语言高级编程",
			Author:     "柴树杉、曹春晖",
			Category:   pb.BookCategory_BOOK_CATEGORY_TECHNICAL,
			PriceCents: 5900, // 59.00 元
			Stock:      100,
			Tags:       []string{"Go", "编程", "后端"},
		},
		{
			Title:      "三体",
			Author:     "刘慈欣",
			Category:   pb.BookCategory_BOOK_CATEGORY_FICTION,
			PriceCents: 3900,
			Stock:      200,
			Tags:       []string{"科幻", "小说"},
		},
		{
			Title:      "深度学习",
			Author:     "Ian Goodfellow",
			Category:   pb.BookCategory_BOOK_CATEGORY_SCIENCE,
			PriceCents: 12800,
			Stock:      50,
			Tags:       []string{"AI", "深度学习", "机器学习"},
		},
	}

	var bookIDs []string
	for _, req := range books {
		resp, err := client.CreateBook(ctx, req)
		if err != nil {
			log.Printf("创建失败: %v", err)
			continue
		}
		bookIDs = append(bookIDs, resp.GetBook().GetId())
		log.Printf("创建成功: [%s] %s, 价格: %.2f 元,库存: %d",
			resp.GetBook().GetId(),
			resp.GetBook().GetTitle(),
			float64(resp.GetBook().GetPriceCents())/100,
			resp.GetBook().GetStock(),
		)
	}

	// 4. 查询书籍
	log.Println("\n========== 查询书籍 ==========")

	if len(bookIDs) > 0 {
		resp, err := client.GetBook(ctx, &pb.GetBookRequest{Id: bookIDs[0]})
		if err != nil {
			log.Printf("查询失败: %v", err)
		} else {
			book := resp.GetBook()
			log.Printf("  书名: %s", book.GetTitle())
			log.Printf("  作者: %s", book.GetAuthor())
			log.Printf("  分类: %s", book.GetCategory())
			log.Printf("  标签: %v", book.GetTags())
			log.Printf("  出版时间: %v", book.GetPublishedAt().AsTime().Format("2006-01-02 15:04:05"))
		}
	}

	// 5. 列出所有书籍
	log.Println("\n========== 列出书籍 ==========")
	listResp, err := client.ListBooks(ctx, &pb.ListBooksRequest{
		Page:     1,
		PageSize: 10,
	})
	if err != nil {
		log.Printf("列出失败: %v", err)
	} else {
		log.Printf("  共 %d 本书", listResp.GetTotal())
		for i, book := range listResp.GetBooks() {
			log.Printf("  %d. [%s] %s - %s (%.2f元,库存:%d)",
				i+1,
				book.GetId(),
				book.GetTitle(),
				book.GetAuthor(),
				float64(book.GetPriceCents())/100,
				book.GetStock(),
			)
		}
	}

	// 6. 更新库存
	log.Println("\n========== 更新库存 ==========")

	if len(bookIDs) > 0 {
		// 出库 10 本
		resp, err := client.UpdateStock(ctx, &pb.UpdateStockRequest{
			BookId:         bookIDs[0],
			QuantityChange: -10,
		})
		if err != nil {
			log.Printf("更新失败: %v", err)
		} else {
			log.Printf("  %s", resp.GetMessage())
			log.Printf("  当前库存: %d", resp.GetBook().GetStock())
		}
	}

	// 7. 分类筛选
	log.Println("\n========== 按分类筛选: 技术类 ==========")

	filterResp, err := client.ListBooks(ctx, &pb.ListBooksRequest{
		Page:     1,
		PageSize: 10,
	})
	if err != nil {
		log.Printf("筛选失败: %v", err)
	} else {
		for _, book := range filterResp.GetBooks() {
			log.Printf("  [%s] %s", book.GetId(), book.GetTitle())
		}
	}

	// 8. 删除书籍
	log.Println("\n========== 删除书籍 ==========")
	if len(bookIDs) > 0 {
		_, err := client.DeleteBook(ctx, &pb.DeleteBookRequest{
			Id: bookIDs[len(bookIDs)-1],
		})
		if err != nil {
			log.Printf("删除失败: %v", err)
		} else {
			log.Printf("  删除成功: %s", bookIDs[len(bookIDs)-1])
		}
	}

	// 9. 查询不存在的书籍(测试错误处理)
	log.Println("\n========== 测试错误处理 ==========")

	_, err = client.GetBook(ctx, &pb.GetBookRequest{Id: "NOT-EXIST"})
	if err != nil {
		log.Printf("  预期错误: %v", err)
	}

	log.Println("\n========== 测试完成 ==========")

	log.Println("\n========== 练习1: 模糊搜索 ==========")

	// 搜索包含 "Go" 的书籍
	searchResp, err := client.SearchBooks(ctx, &pb.SearchBooksRequest{
		Keyword: "Go",
	})
	if err != nil {
		log.Printf("搜索失败: %v", err)
	} else {
		log.Printf("  搜索 \"Go\" 共 %d 个结果", searchResp.GetTotal())
		for _, book := range searchResp.GetBooks() {
			// 匹配到的数据
			log.Printf("  [%s] %s", book.GetId(), book.GetTitle())
		}
	}
	// 搜索包含 "三体" 的书籍
	searchResp2, err := client.SearchBooks(ctx, &pb.SearchBooksRequest{
		Keyword: "三体",
	})
	if err != nil {
		log.Printf("搜索失败: %v", err)
	} else {
		log.Printf("  搜索 \"学习\" 共 %d 个结果:", searchResp2.GetTotal())
		for _, book := range searchResp2.GetBooks() {
			// 应该匹配到 "深度学习"
			log.Printf("    [%s] %s", book.GetId(), book.GetTitle())
		}
	}

	// ====================================================================
	// 练习 2：测试借书 / 还书
	// ====================================================================
	log.Println("\n========== 练习2: 借书 / 还书 ==========")

	if len(bookIDs) > 0 {
		targetID := bookIDs[0] //用第一本书测试

		// 借 5 本
		borrowResp, err := client.BorrowBook(ctx, &pb.BorrowBookRequest{
			BookId:   targetID,
			Quantity: 5,
		})
		if err != nil {
			log.Printf("  借书失败: %v", err)
		} else {
			log.Printf("  借书成功: %s", borrowResp.GetMessage())
			log.Printf("  剩余库存: %d", borrowResp.GetBook().GetStock())
		}

		// 借太多 (故意测试库存不足的错误)
		_, err = client.BorrowBook(ctx, &pb.BorrowBookRequest{
			BookId:   targetID,
			Quantity: 9999,
		})
		if err != nil {
			// 预期会收到 FailedPrecondition 错误
			log.Printf("  预期错误: %v", err)
		}

		// 还 3 本
		returnResp, err := client.ReturnBook(ctx, &pb.ReturnBookRequest{
			BookId:   targetID,
			Quantity: 3,
		})
		if err != nil {
			log.Printf("  还书失败: %v", err)
		} else {
			log.Printf("  还书成功: %s", returnResp.GetMessage())
			log.Printf("  当前库存: %d", returnResp.GetBook().GetStock())
		}
	}

}
