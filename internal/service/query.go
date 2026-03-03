package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"qsystem/internal/pb"
	"sync"
	"time"

	"github.com/google/uuid"
)

type QueryServiceServer struct {
	pb.UnimplementedQueryServiceServer

	// TODO 生产环境替换为 radis
	store sync.Map
}

type StoreItem struct {
	Status string // pending processing succeed failed notFound
	Result string
}

// NewQueryServiceServer 构造函数
func NewQueryServiceServer() *QueryServiceServer {
	return &QueryServiceServer{}
}

// SubmitQuery 业务逻辑
func (s *QueryServiceServer) SubmitQuery(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if req.Output == "" {
		return &pb.QueryResponse{}, errors.New("输入不能为空")
	}

	queryId := uuid.New().String()
	s.store.Store(queryId, &StoreItem{Status: "PENDING", Result: ""})

	// 启动异步携程执行查询
	contextBg := context.Background()
	go s.executeQuery(contextBg, queryId, req.Output)

	return &pb.QueryResponse{
		QueryId: queryId,
	}, nil
}

// GetQueryStatus 查询执行状态
func (s *QueryServiceServer) GetQueryStatus(ctx context.Context, req *pb.QueryStatusRequest) (*pb.QueryStatusResponse, error) {
	queryId := req.QueryId
	val, exists := s.store.Load(queryId)
	if !exists {
		return &pb.QueryStatusResponse{
			Status: "NotFound",
		}, nil
	}

	item := val.(*StoreItem)

	return &pb.QueryStatusResponse{
		Status: item.Status,
		Result: item.Result,
	}, nil
}

// 真正执行查询的任务
func (s *QueryServiceServer) executeQuery(ctx context.Context, queryId string, output string) {
	// TODO 先模拟查询
	time.Sleep(40 * time.Second)

	result := &StoreItem{
		Status: "PROCESSING",
		Result: fmt.Sprintf(`"{ Link: %s }""`, output),
	}

	s.store.Store(queryId, result)

	log.Printf("%s 查询完成", queryId)
}
