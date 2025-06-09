package api

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/metaid/utxo_indexer/api/respond"
	"github.com/metaid/utxo_indexer/common"
	indexer "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"

	"github.com/gin-gonic/gin"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/mempool"
	"github.com/metaid/utxo_indexer/storage"
)

type FtServer struct {
	indexer     *indexer.ContractFtIndexer
	router      *gin.Engine
	mempoolMgr  *mempool.FtMempoolManager
	bcClient    *blockchain.FtClient
	metaStore   *storage.MetaStore
	stopCh      <-chan struct{}
	mempoolInit bool // 内存池是否已初始化
}

func NewFtServer(bcClient *blockchain.FtClient, indexer *indexer.ContractFtIndexer, metaStore *storage.MetaStore, stopCh <-chan struct{}) *FtServer {
	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = io.Discard
	server := &FtServer{
		indexer:     indexer,
		router:      gin.Default(),
		mempoolInit: false,
		metaStore:   metaStore,
		stopCh:      stopCh,
		bcClient:    bcClient,
	}

	server.setupRoutes()
	return server
}

// 设置内存池管理器和区块链客户端
func (s *FtServer) SetMempoolManager(mempoolMgr *mempool.FtMempoolManager, bcClient *blockchain.FtClient) {
	s.mempoolMgr = mempoolMgr
	s.bcClient = bcClient
}

func (s *FtServer) setupRoutes() {
	s.router.GET("/ft/balance", s.getFtBalance)
	s.router.GET("/ft/utxos", s.getFtUTXOs)

	s.router.GET("/db/ft/utxo", s.getDbFtUtxoByTx)
	s.router.GET("/db/ft/income", s.getDbFtIncomeByAddress)
	s.router.GET("/db/ft/income/valid", s.getDbAddressFtIncomeValid)
	s.router.GET("/db/ft/spend", s.getDbFtSpendByAddress)
	s.router.GET("/db/ft/all/income", s.getDbAllFtIncome)
	s.router.GET("/db/ft/all/spend", s.getDbAllFtSpend)
	s.router.GET("/db/ft/address/income", s.getDbAddressFtIncome)
	s.router.GET("/db/ft/address/spend", s.getDbAddressFtSpend)
	s.router.GET("/db/ft/info", s.getDbFtInfo)
	s.router.GET("/db/ft/uncheck/outpoint", s.getAllDbUncheckFtOutpoint)
	s.router.GET("/db/ft/uncheck/outpoint/all", s.getAllDbUncheckFtOutpoint)
	s.router.GET("/db/ft/genesis", s.getAllDbFtGenesis)
	s.router.GET("/db/ft/genesis/output", s.getAllDbFtGenesisOutput)
	s.router.GET("/db/ft/genesis/utxo", s.getAllDbFtGenesisUtxo)
	s.router.GET("/db/ft/used/income", s.getAllDbUsedFtIncome)
	s.router.GET("/db/ft/uncheck/outpoint/total", s.getUncheckFtOutpointTotal)

	s.router.GET("/ft/mempool/utxos", s.getFtMempoolUTXOs)

	// 添加启动内存池的API
	s.router.GET("/ft/mempool/start", s.startMempool)
	// 内存池重建API
	s.router.GET("/ft/mempool/rebuild", s.rebuildMempool)
	// 重新索引区块API
	s.router.GET("/ft/blocks/reindex", s.reindexBlocks)

	// 添加新的内存池查询接口
	s.router.GET("/db/ft/mempool/verify/tx", s.getMempoolVerifyTx)
	s.router.GET("/db/ft/mempool/uncheck/utxo", s.getMempoolUncheckFtUtxo)
	s.router.GET("/db/ft/mempool/spend", s.getMempoolAddressFtSpendMap)
	s.router.GET("/db/ft/mempool/unique/spend", s.getMempoolUniqueFtSpendMap)
	s.router.GET("/db/ft/mempool/address/income", s.getMempoolAddressFtIncomeMap)
	s.router.GET("/db/ft/mempool/address/income/valid", s.getMempoolAddressFtIncomeValidMap)
}

// 启动内存池API
func (s *FtServer) startMempool(c *gin.Context) {
	// 检查内存池管理器是否已配置
	if s.mempoolMgr == nil || s.bcClient == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "内存池管理器或区块链客户端未配置",
		})
		return
	}

	// 检查是否已经初始化
	if s.mempoolInit {
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "内存池已经启动",
			"status":  "running",
		})
		return
	}

	// 启动内存池
	log.Println("通过API启动ZMQ和内存池监听...")
	err := s.mempoolMgr.Start()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "内存池启动失败: " + err.Error(),
		})
		return
	}

	// 标记为已初始化
	s.mempoolInit = true
	log.Println("内存池管理器已通过API启动，监听新交易...")

	// 初始化内存池数据（加载现有内存池交易）
	go func() {
		log.Println("开始初始化FT内存池数据...")
		s.mempoolMgr.InitializeMempool(s.bcClient)
		log.Println("FT内存池数据初始化完成")
	}()

	// 获取当前索引高度作为清理起始高度
	lastIndexedHeightBytes, err := s.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
	if err == nil {
		// 将当前高度设置为清理起始高度，避免清理历史区块
		log.Println("将内存池清理起始高度设置为当前索引高度:", string(lastIndexedHeightBytes))
		err = s.metaStore.Set([]byte("last_mempool_clean_height"), lastIndexedHeightBytes)
		if err != nil {
			log.Printf("设置内存池清理起始高度失败: %v", err)
		}
	} else {
		log.Printf("获取当前索引高度失败: %v", err)
	}

	// 启动内存池清理协程
	go func() {
		// 内存池清理间隔时间
		cleanInterval := 10 * time.Second

		for {
			select {
			case <-s.stopCh:
				return
			case <-time.After(cleanInterval):
				// 1. 获取最后清理的高度
				lastCleanHeight := 0
				lastCleanHeightBytes, err := s.metaStore.Get([]byte("last_mempool_clean_height"))
				if err == nil {
					lastCleanHeight, _ = strconv.Atoi(string(lastCleanHeightBytes))
				}

				// 2. 获取最新索引高度
				lastIndexedHeight := 0
				lastIndexedHeightBytes, err := s.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
				if err == nil {
					lastIndexedHeight, _ = strconv.Atoi(string(lastIndexedHeightBytes))
				}

				// 3. 如果最新索引高度大于最后清理高度，执行清理
				if lastIndexedHeight > lastCleanHeight {
					log.Printf("执行内存池清理，从高度 %d 到 %d", lastCleanHeight+1, lastIndexedHeight)

					// 对每个新块执行清理
					for height := lastCleanHeight + 1; height <= lastIndexedHeight; height++ {
						err := s.mempoolMgr.CleanByHeight(height, s.bcClient)
						if err != nil {
							log.Printf("清理高度 %d 失败: %v", height, err)
						}
					}

					// 更新最后清理高度
					err := s.metaStore.Set([]byte("last_mempool_clean_height"), []byte(strconv.Itoa(lastIndexedHeight)))
					if err != nil {
						log.Printf("更新最后清理高度失败: %v", err)
					}
				}
			}
		}
	}()

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "内存池启动成功",
		"status":  "running",
	})
}

// 重建内存池API
func (s *FtServer) rebuildMempool(c *gin.Context) {
	// 检查内存池管理器是否已配置
	if s.mempoolMgr == nil || s.bcClient == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "内存池管理器或区块链客户端未配置",
		})
		return
	}

	// 检查内存池是否已启动
	if !s.mempoolInit {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "内存池尚未启动，请先使用 /ft/mempool/start 接口启动内存池",
		})
		return
	}

	log.Println("开始清理并重建内存池数据...")

	// 首先停止当前的ZMQ连接（如果有）
	s.mempoolMgr.Stop()
	log.Println("已停止现有ZMQ连接和内存池监听")

	// 清除所有内存池数据
	err := s.mempoolMgr.CleanAllMempool()
	if err != nil {
		log.Printf("清理内存池数据出错: %v，尝试重新创建内存池管理器", err)

		// 即使清理失败，也继续重新创建内存池管理器
		// 获取配置
		cfg, cfgErr := config.LoadConfig()
		if cfgErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "加载配置失败: " + cfgErr.Error(),
			})
			return
		}

		// 获取链参数
		chainCfg, cfgErr := cfg.GetChainParams()
		if cfgErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "获取链参数失败: " + cfgErr.Error(),
			})
			return
		}

		// 重新创建内存池管理器
		basePath := s.mempoolMgr.GetBasePath()
		zmqAddress := s.mempoolMgr.GetZmqAddress()

		// 重新创建内存池管理器
		newMempoolMgr := mempool.NewFtMempoolManager(basePath,
			s.indexer.GetContractFtUtxoStore(),
			s.indexer.GetContractFtInfoStore(),
			s.indexer.GetContractFtGenesisStore(),
			s.indexer.GetContractFtGenesisOutputStore(),
			s.indexer.GetContractFtGenesisUtxoStore(),
			chainCfg, zmqAddress)
		if newMempoolMgr == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"success": false,
				"error":   "重新创建内存池管理器失败",
			})
			return
		}

		// 更新管理器引用
		s.mempoolMgr = newMempoolMgr
		s.indexer.SetMempoolManager(newMempoolMgr)
	}

	// 获取当前索引高度作为清理起始高度
	lastIndexedHeightBytes, err := s.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
	if err == nil {
		// 将当前高度设置为清理起始高度，避免清理历史区块
		log.Println("将内存池清理起始高度设置为当前索引高度:", string(lastIndexedHeightBytes))
		err = s.metaStore.Set([]byte("last_mempool_clean_height"), lastIndexedHeightBytes)
		if err != nil {
			log.Printf("设置内存池清理起始高度失败: %v", err)
		}
	} else {
		log.Printf("获取当前索引高度失败: %v", err)
	}

	// 重新启动ZMQ连接
	log.Println("重新启动ZMQ连接...")
	err = s.mempoolMgr.Start()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "重启ZMQ连接失败: " + err.Error(),
		})
		return
	}

	// 记录重启成功消息
	log.Println("ZMQ连接重启成功，现在应该能够监听新交易")

	// 开始重新初始化内存池
	go func() {
		log.Println("开始重新初始化内存池数据...")
		s.mempoolMgr.InitializeMempool(s.bcClient)
		log.Println("内存池数据重新初始化完成，系统现在应该可以正常处理新交易")
	}()

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "内存池数据已清理，ZMQ已重新启动，内存池正在重建中",
	})
}

// reindexBlocks 重新索引指定范围的区块
func (s *FtServer) reindexBlocks(c *gin.Context) {

	// //配置区块链客户端
	// cfg, err := config.LoadConfig()
	// if err != nil {
	// 	c.JSON(http.StatusInternalServerError, gin.H{
	// 		"success": false,
	// 		"error":   "加载配置失败: " + err.Error(),
	// 	})
	// 	return
	// }
	// bcClient, err := blockchain.NewFtClient(cfg)
	// if err != nil {
	// 	c.JSON(http.StatusInternalServerError, gin.H{
	// 		"success": false,
	// 		"error":   "创建区块链客户端失败: " + err.Error(),
	// 	})
	// 	return
	// }
	// s.bcClient = bcClient

	// 检查区块链客户端是否已配置
	if s.bcClient == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "区块链客户端未配置",
		})
		return
	}

	// 解析请求参数
	startHeightStr := c.Query("start")
	endHeightStr := c.Query("end")

	if startHeightStr == "" || endHeightStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "start和end参数是必须的",
		})
		return
	}

	startHeight, err := strconv.Atoi(startHeightStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "start参数必须是有效的整数",
		})
		return
	}

	endHeight, err := strconv.Atoi(endHeightStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "end参数必须是有效的整数",
		})
		return
	}

	// 验证高度范围
	if startHeight < 0 || endHeight < startHeight {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "无效的高度范围，start必须大于等于0，end必须大于等于start",
		})
		return
	}

	// 检查当前最新区块高度
	currentHeight, err := s.bcClient.GetBlockCount()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "获取当前区块高度失败: " + err.Error(),
		})
		return
	}

	if endHeight > currentHeight {
		endHeight = currentHeight
	}

	// 立即返回响应，后台开始重新索引
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": fmt.Sprintf("开始重新索引区块，范围从 %d 到 %d", startHeight, endHeight),
	})

	// 在后台启动重新索引过程
	go func() {
		log.Printf("开始重新索引区块，范围从 %d 到 %d", startHeight, endHeight)

		// 设置进度条
		blocksToProcess := endHeight - startHeight + 1

		// 处理每个区块
		for height := startHeight; height <= endHeight; height++ {
			// 使用共用的区块处理函数
			if err := s.bcClient.ProcessBlock(s.indexer, height, false); err != nil {
				log.Printf("处理区块失败，高度 %d: %v", height, err)
				continue // 继续处理下一个区块而不是终止整个重索引过程
			}
		}

		log.Printf("重新索引完成，处理了 %d 个区块，从高度 %d 到 %d", blocksToProcess, startHeight, endHeight)
	}()
}

// getMempoolVerifyTx 获取内存池验证交易信息
func (s *FtServer) getMempoolVerifyTx(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	txId := c.Query("txId")

	// 检查内存池管理器是否已配置
	if s.mempoolMgr == nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(errors.New("内存池管理器未配置"), time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 获取分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 获取验证交易信息
	txs, total, err := s.mempoolMgr.GetVerifyTx(txId, page, pageSize)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 计算总页数
	totalPages := (total + pageSize - 1) / pageSize

	c.JSONP(http.StatusOK, respond.RespSuccess(gin.H{
		"txs": txs,
		"pagination": struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

// getMempoolUncheckFtUtxo 获取内存池未检查的FT UTXO信息
func (s *FtServer) getMempoolUncheckFtUtxo(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	outpoint := c.Query("outpoint")

	// 检查内存池管理器是否已配置
	if s.mempoolMgr == nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(errors.New("内存池管理器未配置"), time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 获取分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 获取未检查的FT UTXO列表
	utxoList, err := s.mempoolMgr.GetUncheckFtUtxo()
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 如果提供了outpoint，过滤结果
	if outpoint != "" {
		filteredList := make([]common.FtUtxo, 0)
		for _, utxo := range utxoList {
			if utxo.TxID+":"+utxo.Index == outpoint {
				filteredList = append(filteredList, utxo)
				break
			}
		}
		utxoList = filteredList
	}

	// 计算分页
	total := len(utxoList)
	totalPages := (total + pageSize - 1) / pageSize

	// 获取当前页的数据
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// 提取当前页的数据
	currentPageData := utxoList[start:end]

	c.JSONP(http.StatusOK, respond.RespSuccess(gin.H{
		"utxos": currentPageData,
		"pagination": struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) Start(addr string) error {
	// Start the server
	err := s.router.Run(addr)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
		return err
	}
	return nil
}
