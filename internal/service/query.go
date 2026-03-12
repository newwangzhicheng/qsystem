package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"qsystem/internal/model"
	"qsystem/internal/pb"
	"qsystem/internal/repository"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type QueryServiceServer struct {
	pb.UnimplementedQueryServiceServer
	repo        *repository.TaskRepository
	kafkaWriter *kafka.Writer // 依赖注入
}

// NewQueryServiceServer 构造函数
func NewQueryServiceServer(repo *repository.TaskRepository, kw *kafka.Writer) *QueryServiceServer {
	return &QueryServiceServer{
		repo:        repo,
		kafkaWriter: kw,
	}
}

// SubmitQuery 业务逻辑
func (s *QueryServiceServer) SubmitQuery(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if req.Output == "" {
		return &pb.QueryResponse{}, errors.New("输入不能为空")
	}

	queryId := uuid.New().String()
	initialState := model.StoreItem{Status: "PENDING", Result: ""}

	if err := s.repo.SaveAdnBroadcast(ctx, queryId, initialState); err != nil {
		return nil, err
	}

	// 启动异步携程执行查询
	//contextBg := context.Background()
	//go s.executeQuery(contextBg, queryId, req.Output)

	/** 把任务丢进Kafka */
	taskMsg := model.KafkaTaskMessage{
		QueryId: queryId,
		Output:  req.Output,
	}
	// 序列化
	taskMsgBytes, err := json.Marshal(taskMsg)
	if err != nil {
		return nil, fmt.Errorf("投递任务消息序列化失败；%w", err)
	}
	// 将消息丢进去
	err = s.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(queryId),
		Value: taskMsgBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("投递任务到消息队列失败: %w", err)
	}

	return &pb.QueryResponse{
		QueryId: queryId,
	}, nil
}

// GetQueryStatus 查询执行状态
func (s *QueryServiceServer) GetQueryStatus(ctx context.Context, req *pb.QueryStatusRequest) (*pb.QueryStatusResponse, error) {
	queryId := req.QueryId
	state, err := s.repo.GetState(ctx, queryId)
	if err != nil {
		log.Printf("查询失败，%w\n", err)
		return nil, fmt.Errorf("查询失败，%w", err)
	}

	return &pb.QueryStatusResponse{
		Status: state.Status,
		Result: state.Result,
	}, nil
}

// 真正执行查询的任务
func (s *QueryServiceServer) executeQuery(ctx context.Context, queryId string, output string) {
	// TODO 先模拟查询
	time.Sleep(5 * time.Second)

	state := model.StoreItem{
		Status: "PROCESSING",
		Result: fmt.Sprintf(`"{ Link: %s }""`, output),
	}

	if err := s.saveToRedis(ctx, queryId, state); err != nil {
		log.Printf("%v 任务完成，写入Redis失败: %v", queryId, err)
		return
	}

	log.Printf("%s 查询完成", queryId)
}
