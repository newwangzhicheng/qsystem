package model

// KafkaTaskMessage Kafka信息
type KafkaTaskMessage struct {
	QueryId string `json:"query_id"`
	Output  string `json:"output"`
}

type StoreItem struct {
	Status string `json:"status"` // pending processing completed notFound
	Result string `json:"result"`
}
