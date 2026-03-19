package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"qsystem/internal/model"
	"time"

	"github.com/redis/go-redis/v9"
)

type TaskRepository struct {
	rdb *redis.Client
}

func NewTaskRepository(rdb *redis.Client) *TaskRepository {
	return &TaskRepository{
		rdb: rdb,
	}
}

func (r *TaskRepository) genKey(queryId string) string {
	return "queryId:" + queryId
}

func (r *TaskRepository) genChannel(key string) string {
	return "channelId:" + key
}

// SaveAdnBroadcast 保存并广播任务状态
func (r *TaskRepository) SaveAdnBroadcast(ctx context.Context, queryId string, state model.StoreItem) error {
	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}

	redisKey := r.genKey(queryId)
	err = r.rdb.Set(ctx, redisKey, jsonData, 24*time.Hour).Err()

	// 发送广播
	err = r.rdb.Publish(ctx, r.genChannel(redisKey), jsonData).Err()
	if err != nil {
		log.Printf("发送 redis 广播失败，%s\n", err)
	}

	return err
}

// GetState 获取任务状态
func (r *TaskRepository) GetState(ctx context.Context, queryId string) (*model.StoreItem, error) {
	redisKey := r.genKey(queryId)

	jsonData, err := r.rdb.Get(ctx, redisKey).Result()
	if err != nil {
		if err == redis.Nil {
			return &model.StoreItem{
				Status: "NOTFOUND",
				Result: "",
			}, nil // 查询不到结果
		}
		return nil, fmt.Errorf("读取不到 redis：%w", err)
	}

	var state model.StoreItem
	if err = json.Unmarshal([]byte(jsonData), &state); err != nil {
		return nil, fmt.Errorf("解析 redis 数据失败：%w", err)
	}

	return &state, nil
}

// SubscribeStatus 订阅任务状态，返回一个通道
func (r *TaskRepository) SubscribeStatus(ctx context.Context, queryId string) (<-chan model.StoreItem, error) {
	// 创建一个通道
	ch := make(chan model.StoreItem)

	// 订阅channel
	channel := r.genChannel(r.genKey(queryId))
	pubsub := r.rdb.Subscribe(ctx, channel)

	// 开启一个协程，接收到消息就放在通道中
	go func() {
		// 关闭通道，关闭订阅
		defer pubsub.Close()
		defer close(ch)

		// 开启死循环，接受消息的时候放在通道里
		for {
			select {
			case <-ctx.Done():
				log.Printf("监听到上下文取消，停止订阅 redis %s", channel)
				return
			case msg := <-pubsub.Channel():
				var item model.StoreItem
				if err := json.Unmarshal([]byte(msg.Payload), &item); err != nil {
					log.Printf("订阅的消息解析失败，%v", err)
					// 忽略脏数据
					continue
				}
				select {
				case ch <- item:
				case <-ctx.Done(): // 防止瞬间上下文取消导致死锁
					return
				}
			}
		}
	}()

	return ch, nil

}
