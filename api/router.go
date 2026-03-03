package api

import (
	"qsystem/api/handler"

	"github.com/gin-gonic/gin"
)

func SetupRouter(queryHandler *handler.QueryHandler) *gin.Engine {
	r := gin.Default()

	v1 := r.Group("/api/v1")

	{
		v1.GET("/query", queryHandler.Query)
		v1.GET("/query/status", queryHandler.GetQueryStatus)
	}

	return r
}
