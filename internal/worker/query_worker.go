package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// KafkaTaskMessage Kafka信息
type KafkaTaskMessage struct {
	QueryId string `json:"query_id"`
	Output  string `json:"output"`
}

type StoreItem struct {
	Status string `json:"status"` // pending processing succeed failed notFound
	Result string `json:"result"`
}

type QueryWorker struct {
	rdb         *redis.Client
	kafkaReader *kafka.Reader
}

func NewQueryWorker(rdb *redis.Client, kr *kafka.Reader) *QueryWorker {
	return &QueryWorker{
		rdb:         rdb,
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
		var taskMsg KafkaTaskMessage
		if err := json.Unmarshal(msg.Value, &taskMsg); err != nil {
			log.Printf("[worker] 解析 Kafka 消息失败，废弃的消息：%s", string(msg.Value))
			err = w.kafkaReader.CommitMessages(ctx, msg) // 确认掉垃圾消息
			if err != nil {
				log.Printf("确认垃圾消息失败：%w\n", err)
			}
			continue
		}
		// 更新任务
		err = w.saveToRedis(ctx, taskMsg.QueryId, StoreItem{
			Status: "PROCESSING",
			Result: "",
		})
		if err != nil {
			log.Printf("[worker] 保存任务状态失败：%w", err)
		}
		// 执行具体任务
		result := w.executeQuery(ctx, taskMsg.QueryId, taskMsg.Output)

		// 更新任务
		err = w.saveToRedis(ctx, taskMsg.QueryId, StoreItem{
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

// 保存到redis
func (w *QueryWorker) saveToRedis(ctx context.Context, queryId string, state StoreItem) error {
	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}
	err = w.rdb.Set(ctx, w.genKey(queryId), jsonData, 24*time.Hour).Err()
	return err
}

func (w *QueryWorker) genKey(queryId string) string {
	return "queryId:" + queryId
}

// 真正执行查询的任务
func (w *QueryWorker) executeQuery(ctx context.Context, queryId string, output string) string {
	// TODO 先模拟查询
	time.Sleep(5 * time.Second)

	return fmt.Sprintf(`"{ Link: %s }""`, output)
}
