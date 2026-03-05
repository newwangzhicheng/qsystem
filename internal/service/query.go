package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"qsystem/internal/pb"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type QueryServiceServer struct {
	pb.UnimplementedQueryServiceServer
	rdb *redis.Client
}

type StoreItem struct {
	Status string `json:"status"` // pending processing succeed failed notFound
	Result string `json:"result"`
}

// NewQueryServiceServer 构造函数
func NewQueryServiceServer(rdb *redis.Client) *QueryServiceServer {
	return &QueryServiceServer{
		rdb: rdb,
	}
}

// SubmitQuery 业务逻辑
func (s *QueryServiceServer) SubmitQuery(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if req.Output == "" {
		return &pb.QueryResponse{}, errors.New("输入不能为空")
	}

	queryId := uuid.New().String()
	initialState := StoreItem{Status: "PENDING", Result: ""}

	if err := s.saveToRedis(ctx, queryId, initialState); err != nil {
		return nil, err
	}

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
	jsonData, err := s.rdb.Get(ctx, s.genKey(queryId)).Result()
	if err != nil {
		if err == redis.Nil {
			return &pb.QueryStatusResponse{
				Status: "NOTFOUND",
			}, err
		}
		return nil, errors.New("读取Redis数据失败")
	}

	var state StoreItem
	if err := json.Unmarshal([]byte(jsonData), &state); err != nil {
		return nil, errors.New("解析Redis数据失败")
	}

	return &pb.QueryStatusResponse{
		Status: state.Status,
		Result: state.Result,
	}, nil
}

// 保存到redis
func (s *QueryServiceServer) saveToRedis(ctx context.Context, queryId string, state StoreItem) error {
	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}
	err = s.rdb.Set(ctx, s.genKey(queryId), jsonData, 24*time.Hour).Err()
	return err
}

// 真正执行查询的任务
func (s *QueryServiceServer) executeQuery(ctx context.Context, queryId string, output string) {
	// TODO 先模拟查询
	time.Sleep(5 * time.Second)

	state := StoreItem{
		Status: "PROCESSING",
		Result: fmt.Sprintf(`"{ Link: %s }""`, output),
	}

	if err := s.saveToRedis(ctx, queryId, state); err != nil {
		log.Printf("%v 任务完成，写入Redis失败: %v", queryId, err)
		return
	}

	log.Printf("%s 查询完成", queryId)
}

func (s *QueryServiceServer) genKey(queryId string) string {
	return "queryId:" + queryId
}
