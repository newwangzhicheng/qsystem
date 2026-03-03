package handler

import (
	"net/http"
	"qsystem/internal/pb"

	"github.com/gin-gonic/gin"
)

type QueryHandler struct {
	grpcClient pb.QueryServiceClient
}

func NewQueryHandler(client pb.QueryServiceClient) *QueryHandler {
	return &QueryHandler{
		grpcClient: client,
	}
}

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
