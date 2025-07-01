package blockindexer

import (
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/metaid/utxo_indexer/api"
)

func SetRouter(server *api.Server) {
	server.Router.GET("/block/info", chainInfo)
	server.Router.GET("/block/:blockId", blockInfo)
	server.Router.GET("/block", blockList)
	server.Router.GET("/block/tx/:height", blockTxList)
	server.Router.GET("/block/txall/:height", blockAllTxList)
}
func chainInfo(c *gin.Context) {
	if ChainStats == nil {
		c.JSON(500, map[string]interface{}{
			"error": "Failed to get chain status",
		})
		return
	}
	c.JSON(200, ChainStats)
}
func blockInfo(c *gin.Context) {
	blockId := c.Param("blockId")
	if blockId == "" {
		c.JSON(400, map[string]interface{}{
			"error": "Block ID is required",
		})
		return
	}
	// Try to parse blockId as integer
	blockHeight, err := ParseBlockHeightOrHash(blockId)
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
	last := int64(0)
	var err error
	lastId := c.Query("last")
	if lastId != "" {
		// Try to parse blockId as integer
		last, err = strconv.ParseInt(lastId, 10, 64)
		if err != nil {
			c.JSON(400, map[string]interface{}{
				"error": "Invalid lastId format",
			})
			return
		}
	}
	// Get block list
	blocks, err := GetBlockInfoList(last, 30)
	if err != nil {
		c.JSON(500, map[string]interface{}{
			"error": "Failed to get block list",
		})
		return
	}
	c.JSON(200, blocks)
}
func blockTxList(c *gin.Context) {
	blockHeightStr := c.Param("height")
	if blockHeightStr == "" {
		c.JSON(200, gin.H{
			"code": -1,
			"msg":  "Block height is required",
			"data": nil,
		})
		return
	}
	blockHeight, err := strconv.ParseInt(blockHeightStr, 10, 64)
	if err != nil {
		c.JSON(200, gin.H{
			"code": -1,
			"msg":  "Invalid block height format",
			"data": nil,
		})
		return
	}
	cursorStr := c.Query("cursor")
	cursor := 0
	if cursorStr != "" {
		cursor, err = strconv.Atoi(cursorStr)
		if err != nil {
			c.JSON(200, gin.H{
				"code": -1,
				"msg":  "Invalid cursor format",
				"data": nil,
			})
			return
		}
	}
	sizeStr := c.Query("size")
	size := 100 // default value
	if sizeStr != "" {
		size, err = strconv.Atoi(sizeStr)
		if err != nil || size <= 0 {
			c.JSON(200, gin.H{
				"code": -1,
				"msg":  "Invalid size format",
				"data": nil,
			})
			return
		}
	}
	txs, total, err := GetBlockTxList(blockHeight, cursor, size)
	if err != nil {
		c.JSON(200, gin.H{
			"code": -1,
			"msg":  "Block not found or no transactions",
			"data": nil,
		})
		return
	}
	if c.Query("fee") != "" {
		c.JSON(200, gin.H{
			"code": 0,
			"msg":  "ok",
			"data": gin.H{
				"num_tx": total,
				"tx":     txs,
			},
		})
	} else {
		var newList []string
		for _, tx := range txs {
			arr := strings.Split(tx, ":")
			if len(arr) > 1 {
				newList = append(newList, arr[0])
			} else {
				newList = append(newList, tx)
			}
		}
		c.JSON(200, gin.H{
			"code": 0,
			"msg":  "ok",
			"data": gin.H{
				"num_tx": total,
				"tx":     newList,
			},
		})
	}

}

func blockAllTxList(c *gin.Context) {
	blockHeightStr := c.Param("height")
	if blockHeightStr == "" {
		c.JSON(200, []string{})
		return
	}
	blockHeight, err := strconv.ParseInt(blockHeightStr, 10, 64)
	if err != nil {
		c.JSON(200, []string{})
		return
	}
	txs, err := GetBlockAllTxList(blockHeight)
	if err != nil {
		c.JSON(200, []string{})
		return
	}
	c.JSON(200, txs)
}
