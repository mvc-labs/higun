package blockindexer

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/metaid/utxo_indexer/api"
)

func SetRouter(server *api.Server) {
	server.Router.GET("/block/info", chainInfo)
	server.Router.GET("/block/:blockId", blockInfo)
	server.Router.GET("/block", blockList)
}
func chainInfo(c *gin.Context) {
	stats, err := client.GetChainStatus()
	if err != nil {
		c.JSON(500, map[string]interface{}{
			"error": "Failed to get chain status",
		})
		return
	}
	c.JSON(200, stats)
}
func blockInfo(c *gin.Context) {
	blockId := c.Param("blockId")
	if blockId == "" {
		c.JSON(400, map[string]interface{}{
			"error": "Block ID is required",
		})
		return
	}
	// 尝试将 blockId 解析为整数
	blockHeight, err := strconv.ParseInt(blockId, 10, 64)
	if err != nil {
		c.JSON(400, map[string]interface{}{
			"error": "Invalid block ID format",
		})
		return
	}
	blockInfo, err := GetBlockInfo(blockHeight)
	if err != nil {
		c.JSON(404, map[string]interface{}{
			"error": "Block not found",
		})
		return
	}
	c.JSON(200, blockInfo)
}

func blockList(c *gin.Context) {
	lastId := c.Query("last")
	if lastId == "" {
		c.JSON(400, map[string]interface{}{
			"error": "lastId  is required",
		})
		return
	}
	// 尝试将 blockId 解析为整数
	last, err := strconv.ParseInt(lastId, 10, 64)
	if err != nil {
		c.JSON(400, map[string]interface{}{
			"error": "Invalid lastId format",
		})
		return
	}
	// 获取区块列表
	blocks, err := GetBlockInfoList(last, 30)
	if err != nil {
		c.JSON(500, map[string]interface{}{
			"error": "Failed to get block list",
		})
		return
	}
	c.JSON(200, blocks)
}
