package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"qsystem/internal/model"
	"qsystem/internal/repository"
	"time"

	"github.com/segmentio/kafka-go"
)

type QueryWorker struct {
	repo        *repository.TaskRepository
	kafkaReader *kafka.Reader
}

func NewQueryWorker(repo *repository.TaskRepository, kr *kafka.Reader) *QueryWorker {
	return &QueryWorker{
		repo:        repo,
		kafkaReader: kr,
	}
}

func (w *QueryWorker) Start(ctx context.Context) {
	log.Printf("[worker] 任务消费者启动，正在监听 Kafka 队列")
	for {
		msg, err := w.kafkaReader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("[worker] 收到停止信号，退出监听")
				return
			}
			log.Printf("[worker] 读取 Kafka 失败：%s\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		// 解析数据
		var taskMsg model.KafkaTaskMessage
		if err := json.Unmarshal(msg.Value, &taskMsg); err != nil {
			log.Printf("[worker] 解析 Kafka 消息失败，废弃的消息：%s", string(msg.Value))
			err = w.kafkaReader.CommitMessages(ctx, msg) // 确认掉垃圾消息
			if err != nil {
				log.Printf("确认垃圾消息失败：%w\n", err)
			}
			continue
		}
		// 更新任务
		err = w.repo.SaveAdnBroadcast(ctx, taskMsg.QueryId, model.StoreItem{
			Status: "PROCESSING",
			Result: "",
		})
		if err != nil {
			log.Printf("[worker] 保存任务状态失败：%w", err)
		}
		// 执行具体任务
		result := w.executeQuery(ctx, taskMsg.QueryId, taskMsg.Output)

		// 更新任务
		err = w.repo.SaveAdnBroadcast(ctx, taskMsg.QueryId, model.StoreItem{
			Status: "COMPLETED",
			Result: result,
		})
		if err != nil {
			log.Printf("[worker] 保存任务状态失败：%w", err)
		}

		// 发送 ACK 确认
		if err = w.kafkaReader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[worker] 发送 ACK 确认消息失败：%w", err)
		} else {
			log.Printf("[worker] 任务 %s 完成", taskMsg.QueryId)
		}
	}
}

// 真正执行查询的任务
func (w *QueryWorker) executeQuery(ctx context.Context, queryId string, output string) string {
	// TODO 先模拟查询
	time.Sleep(5 * time.Second)

	return fmt.Sprintf(`"{ Link: %s }""`, output)
}
