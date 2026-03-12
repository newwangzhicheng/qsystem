package repository

import (
	"context"
	"encoding/json"
	"fmt"
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

func (r *TaskRepository) SaveAdnBroadcast(ctx context.Context, queryId string, state model.StoreItem) error {
	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}
	err = r.rdb.Set(ctx, r.genKey(queryId), jsonData, 24*time.Hour).Err()
	return err
}

func (r *TaskRepository) GetState(ctx context.Context, queryId string) (*model.StoreItem, error) {
	redisKey := r.genKey(queryId)

	jsonData, err := r.rdb.Get(ctx, redisKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // 查询不到结果
		}
		return nil, fmt.Errorf("读取不到 redis：%w", err)
	}

	var state model.StoreItem
	if err = json.Unmarshal([]byte(jsonData), &state); err != nil {
		return nil, fmt.Errorf("解析 redis 数据失败：%w", err)
	}

	return &state, nil
}
