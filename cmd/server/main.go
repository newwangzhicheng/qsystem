package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"qsystem/api"
	"qsystem/api/handler"
	"qsystem/internal/pb"
	"qsystem/internal/service"
	"qsystem/internal/worker"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	/** Redis 服务 */
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	// ping 确认连上，给2秒确认时间
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("Redis 服务连接不上")
	}
	log.Printf("成功连接到 Redis 服务器")
	defer func() {
		log.Printf("关闭 Redis 连接")
		rdb.Close()
	}()

	/** Kafka 服务 */
	// 生产者
	kafkaBroker := []string{"localhost:9092"}
	topicName := "query-tasks"
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker[0]),
		Topic:                  topicName,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer kafkaWriter.Close()
	// 消费者
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaBroker,
		GroupID:  "query-worker-group",
		Topic:    topicName,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer kafkaReader.Close()
	// 装配，后台启动worker
	queryWorker := worker.NewQueryWorker(rdb, kafkaReader)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	// 启动worker死循环
	go queryWorker.Start(workerCtx)

	/** 启动 gRPC */
	queryService := service.NewQueryServiceServer(rdb, kafkaWriter)

	// 创建 gRPC 引擎
	grpcServer := grpc.NewServer()
	pb.RegisterQueryServiceServer(grpcServer, queryService)

	// 50051 监听 gRPC 通信
	grpcListener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("gRPC 端口监听失败 %v", err)
	}
	// 启动 gRPC 服务
	go func() {
		log.Printf("启动 gRPC 服务")
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("gRPC 运行崩溃 %v", err)
		}
	}()

	/** 连接 gRPC */
	grpcConn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("无法连接内部 gRPC 服务", err)
	}
	// 程序关闭，关闭连接
	defer func() {
		log.Printf("正在关闭 gRPC 服务")
		err := grpcConn.Close()
		if err != nil {
			log.Printf("关闭 gRPC 服务时失败 %v", err)
		}
		log.Printf("关闭 gRPC 服务成功")
	}()
	// 实例化 gRPC 客户端桩
	grpcClient := pb.NewQueryServiceClient(grpcConn)
	// 依赖注入
	queryHandler := handler.NewQueryHandler(grpcClient)
	router := api.SetupRouter(queryHandler)

	// 服务
	serv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go start(serv)

	// 遇到关闭信息，安全的关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// 这里阻塞，直到收到信号
	<-quit
	log.Printf("正在关闭服务器...")

	// 给服务器5秒关闭
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := serv.Shutdown(ctx); err != nil {
		log.Printf("服务器强制关闭")
	}

	log.Printf("服务器安全关闭")
}

// 协程启动
func start(serv *http.Server) {
	log.Printf("启动服务，地址localhost%s...", serv.Addr)
	if err := serv.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
		log.Printf("服务器启动失败")
	}
}
