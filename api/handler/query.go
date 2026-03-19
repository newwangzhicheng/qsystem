package handler

import (
	"context"
	"log"
	"net/http"
	"qsystem/internal/model"
	"qsystem/internal/pb"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type StatusScriber interface {
	SubscribeStatus(ctx context.Context, queryId string) (<-chan model.StoreItem, error)
}

type QueryHandler struct {
	grpcClient pb.QueryServiceClient
	subscriber StatusScriber
}

func NewQueryHandler(client pb.QueryServiceClient, sub StatusScriber) *QueryHandler {
	return &QueryHandler{
		grpcClient: client,
		subscriber: sub,
	}
}

// Query 根据output查询，生成queryId
func (h *QueryHandler) Query(c *gin.Context) {
	// 解析
	output := c.Query("output")

	if output == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "查询参数 output 不能为空"})
	}

	// 处理
	result, err := h.grpcClient.SubmitQuery(c.Request.Context(), &pb.QueryRequest{Output: output})

	if err != nil {
		// 错误返回400
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "查询服务不可用",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "success",
		"data": gin.H{
			"queryId":  result.QueryId,
			"queryUrl": "/api/v1/query/status?query_id=" + result.QueryId,
		},
	})
}

// GetQueryStatus 根据queryId获取状态
func (h *QueryHandler) GetQueryStatus(c *gin.Context) {
	// 解析
	queryId := c.Query("query_id")

	if queryId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "查询参数 query_id 不能为空"})
	}

	// 处理
	result, err := h.grpcClient.GetQueryStatus(c.Request.Context(), &pb.QueryStatusRequest{QueryId: queryId})

	if err != nil {
		// 错误返回400
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "查询服务不可用",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "success",
		"data": gin.H{
			"status": result.Status,
			"result": result.Result,
		},
	})
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// TODO 暂时都放行
		return true
	},
}

// WatchStatus ws连接监听状态
func (h *QueryHandler) WatchStatus(c *gin.Context) {
	// 解析
	queryId := c.Query("query_id")
	if queryId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "查询参数 query_id 不能为空"})
	}

	// 升级ws
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[websocket] 升级失败 %v", err)
		return
	}
	defer ws.Close()

	// 向底层申请状态流
	statusCh, err := h.subscriber.SubscribeStatus(c.Request.Context(), queryId)
	if err != nil {
		ws.WriteMessage(websocket.CloseMessage, []byte("内部订阅发生故障"))
		return
	}

	// 监听循环
	for {
		select {
		case state, ok := <-statusCh:
			if !ok {
				// 通道被关闭，订阅结束
				return
			}
			// 推送
			ws.WriteJSON(state)
			// 任务结束关闭
			if state.Status == "COMPLETED" {
				ws.WriteMessage(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "任务结束"))
			}
		case <-c.Request.Context().Done():
			log.Printf("用户主动断开 websocket 连接: %s", queryId)
			return
		}
	}
}
