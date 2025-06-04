package api

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

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

func NewFtServer(indexer *indexer.ContractFtIndexer, metaStore *storage.MetaStore, stopCh <-chan struct{}) *FtServer {
	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = io.Discard
	server := &FtServer{
		indexer:     indexer,
		router:      gin.Default(),
		mempoolInit: false,
		metaStore:   metaStore,
		stopCh:      stopCh,
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
	s.router.GET("/ft/utxo/db", s.getFtUtxoByTx)
	s.router.GET("/ft/income/db", s.getFtIncomeByAddress)
	s.router.GET("/ft/spend/db", s.getFtSpendByAddress)
	s.router.GET("/ft/mempool/utxos", s.getFtMempoolUTXOs)
	s.router.GET("/ft/all/income", s.getAllFtIncome)
	s.router.GET("/ft/all/spend", s.getAllFtSpend)
	s.router.GET("/ft/address/income", s.getAddressFtIncome)
	s.router.GET("/ft/address/spend", s.getAddressFtSpend)
	s.router.GET("/ft/info", s.getFtInfo)

	// 添加启动内存池的API
	s.router.GET("/ft/mempool/start", s.startMempool)
	// 内存池重建API
	s.router.GET("/ft/mempool/rebuild", s.rebuildMempool)
	// 重新索引区块API
	s.router.GET("/ft/blocks/reindex", s.reindexBlocks)
}

func (s *FtServer) getFtBalance(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address parameter is required"})
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	balances, err := s.indexer.GetFtBalance(address, codeHash, genesis)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, balances)
}

func (s *FtServer) getFtUTXOs(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address parameter is required"})
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	utxos, err := s.indexer.GetFtUTXOs(address, codeHash, genesis)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"address": address,
		"utxos":   utxos,
		"count":   len(utxos),
	})
}

func (s *FtServer) getFtUtxoByTx(c *gin.Context) {
	tx := c.Query("tx")
	if tx == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tx parameter is required"})
		return
	}

	utxos, err := s.indexer.GetDbFtUtxoByTx(tx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"utxos": string(utxos),
	})
}

func (s *FtServer) getFtIncomeByAddress(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address parameter is required"})
		return
	}

	income, err := s.indexer.GetDbAddressFtIncomeByTx(address)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"income": string(income),
	})
}

func (s *FtServer) getFtSpendByAddress(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address parameter is required"})
		return
	}

	spend, err := s.indexer.GetDbAddressFtSpendByTx(address)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"spend": string(spend),
	})
}

func (s *FtServer) getFtMempoolUTXOs(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address parameter is required"})
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	imcome, spend, err := s.indexer.GetMempoolFtUTXOs(address, codeHash, genesis)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"address": address,
		"imcome":  imcome,
		"spend":   spend,
		"count":   len(imcome) + len(spend),
	})
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
		log.Println("开始初始化内存池数据...")
		s.mempoolMgr.InitializeMempool(s.bcClient)
		log.Println("内存池数据初始化完成")
	}()

	// 获取当前索引高度作为清理起始高度
	lastIndexedHeightBytes, err := s.metaStore.Get([]byte("last_indexed_height"))
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
				lastIndexedHeightBytes, err := s.metaStore.Get([]byte("last_indexed_height"))
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
	lastIndexedHeightBytes, err := s.metaStore.Get([]byte("last_indexed_height"))
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

// getAllFtIncome 获取所有地址的 FT 收入数据
func (s *FtServer) getAllFtIncome(c *gin.Context) {
	// 获取分页参数
	pageStr := c.DefaultQuery("page", "1")
	pageSizeStr := c.DefaultQuery("page_size", "100")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的页码参数"})
		return
	}

	pageSize, err := strconv.Atoi(pageSizeStr)
	if err != nil || pageSize < 1 || pageSize > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的每页数量参数，范围应在1-1000之间"})
		return
	}

	incomeData, err := s.indexer.GetAllDbAddressFtIncome()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 计算分页
	total := len(incomeData)
	start := (page - 1) * pageSize
	end := start + pageSize

	if start >= total {
		c.JSON(http.StatusOK, gin.H{
			"income_data": map[string]string{},
			"pagination": gin.H{
				"current_page": page,
				"page_size":    pageSize,
				"total":        total,
				"total_pages":  (total + pageSize - 1) / pageSize,
			},
		})
		return
	}

	if end > total {
		end = total
	}

	// 创建分页后的数据
	pagedData := make(map[string]string)
	keys := make([]string, 0, len(incomeData))
	for k := range incomeData {
		keys = append(keys, k)
	}

	// 对键进行排序以确保分页的一致性
	sort.Strings(keys)

	// 获取当前页的数据
	for i := start; i < end; i++ {
		key := keys[i]
		pagedData[key] = incomeData[key]
	}

	c.JSON(http.StatusOK, gin.H{
		"income_data": pagedData,
		"pagination": gin.H{
			"current_page": page,
			"page_size":    pageSize,
			"total":        total,
			"total_pages":  (total + pageSize - 1) / pageSize,
		},
	})
}

// getAllFtSpend 获取所有地址的 FT 支出数据
func (s *FtServer) getAllFtSpend(c *gin.Context) {
	// 获取分页参数
	pageStr := c.DefaultQuery("page", "1")
	pageSizeStr := c.DefaultQuery("page_size", "100")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的页码参数"})
		return
	}

	pageSize, err := strconv.Atoi(pageSizeStr)
	if err != nil || pageSize < 1 || pageSize > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的每页数量参数，范围应在1-1000之间"})
		return
	}

	spendData, err := s.indexer.GetAllDbAddressFtSpend()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 计算分页
	total := len(spendData)
	start := (page - 1) * pageSize
	end := start + pageSize

	if start >= total {
		c.JSON(http.StatusOK, gin.H{
			"spend_data": map[string]string{},
			"pagination": gin.H{
				"current_page": page,
				"page_size":    pageSize,
				"total":        total,
				"total_pages":  (total + pageSize - 1) / pageSize,
			},
		})
		return
	}

	if end > total {
		end = total
	}

	// 创建分页后的数据
	pagedData := make(map[string]string)
	keys := make([]string, 0, len(spendData))
	for k := range spendData {
		keys = append(keys, k)
	}

	// 对键进行排序以确保分页的一致性
	sort.Strings(keys)

	// 获取当前页的数据
	for i := start; i < end; i++ {
		key := keys[i]
		pagedData[key] = spendData[key]
	}

	c.JSON(http.StatusOK, gin.H{
		"spend_data": pagedData,
		"pagination": gin.H{
			"current_page": page,
			"page_size":    pageSize,
			"total":        total,
			"total_pages":  (total + pageSize - 1) / pageSize,
		},
	})
}

// getAddressFtIncome 获取指定地址的 FT 收入数据
func (s *FtServer) getAddressFtIncome(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address 参数是必须的"})
		return
	}

	// 获取分页参数
	pageStr := c.DefaultQuery("page", "1")
	pageSizeStr := c.DefaultQuery("page_size", "100")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的页码参数"})
		return
	}

	pageSize, err := strconv.Atoi(pageSizeStr)
	if err != nil || pageSize < 1 || pageSize > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的每页数量参数，范围应在1-1000之间"})
		return
	}

	// 获取地址的收入数据
	incomeData, err := s.indexer.GetDbAddressFtIncomeByTx(address)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSON(http.StatusOK, gin.H{
				"address":     address,
				"income_data": "",
				"pagination": gin.H{
					"current_page": page,
					"page_size":    pageSize,
					"total":        0,
					"total_pages":  0,
				},
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 将收入数据按逗号分割成数组
	incomeItems := strings.Split(string(incomeData), ",")
	if len(incomeItems) == 0 || (len(incomeItems) == 1 && incomeItems[0] == "") {
		c.JSON(http.StatusOK, gin.H{
			"address":     address,
			"income_data": "",
			"pagination": gin.H{
				"current_page": page,
				"page_size":    pageSize,
				"total":        0,
				"total_pages":  0,
			},
		})
		return
	}

	// 计算分页
	total := len(incomeItems)
	start := (page - 1) * pageSize
	end := start + pageSize

	if start >= total {
		c.JSON(http.StatusOK, gin.H{
			"address":     address,
			"income_data": "",
			"pagination": gin.H{
				"current_page": page,
				"page_size":    pageSize,
				"total":        total,
				"total_pages":  (total + pageSize - 1) / pageSize,
			},
		})
		return
	}

	if end > total {
		end = total
	}

	// 获取当前页的数据
	pagedItems := incomeItems[start:end]

	c.JSON(http.StatusOK, gin.H{
		"address":     address,
		"income_data": strings.Join(pagedItems, ","),
		"pagination": gin.H{
			"current_page": page,
			"page_size":    pageSize,
			"total":        total,
			"total_pages":  (total + pageSize - 1) / pageSize,
		},
	})
}

// getAddressFtSpend 获取指定地址的 FT 支出数据
func (s *FtServer) getAddressFtSpend(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address 参数是必须的"})
		return
	}

	// 获取分页参数
	pageStr := c.DefaultQuery("page", "1")
	pageSizeStr := c.DefaultQuery("page_size", "100")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的页码参数"})
		return
	}

	pageSize, err := strconv.Atoi(pageSizeStr)
	if err != nil || pageSize < 1 || pageSize > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的每页数量参数，范围应在1-1000之间"})
		return
	}

	// 获取地址的支出数据
	spendData, err := s.indexer.GetDbAddressFtSpendByTx(address)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSON(http.StatusOK, gin.H{
				"address":    address,
				"spend_data": "",
				"pagination": gin.H{
					"current_page": page,
					"page_size":    pageSize,
					"total":        0,
					"total_pages":  0,
				},
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 将支出数据按逗号分割成数组
	spendItems := strings.Split(string(spendData), ",")
	if len(spendItems) == 0 || (len(spendItems) == 1 && spendItems[0] == "") {
		c.JSON(http.StatusOK, gin.H{
			"address":    address,
			"spend_data": "",
			"pagination": gin.H{
				"current_page": page,
				"page_size":    pageSize,
				"total":        0,
				"total_pages":  0,
			},
		})
		return
	}

	// 计算分页
	total := len(spendItems)
	start := (page - 1) * pageSize
	end := start + pageSize

	if start >= total {
		c.JSON(http.StatusOK, gin.H{
			"address":    address,
			"spend_data": "",
			"pagination": gin.H{
				"current_page": page,
				"page_size":    pageSize,
				"total":        total,
				"total_pages":  (total + pageSize - 1) / pageSize,
			},
		})
		return
	}

	if end > total {
		end = total
	}

	// 获取当前页的数据
	pagedItems := spendItems[start:end]

	c.JSON(http.StatusOK, gin.H{
		"address":    address,
		"spend_data": strings.Join(pagedItems, ","),
		"pagination": gin.H{
			"current_page": page,
			"page_size":    pageSize,
			"total":        total,
			"total_pages":  (total + pageSize - 1) / pageSize,
		},
	})
}

// getFtInfo 获取 FT 信息
func (s *FtServer) getFtInfo(c *gin.Context) {
	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	if codeHash == "" || genesis == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "codeHash 和 genesis 参数都是必须的"})
		return
	}

	// 构建查询键
	key := codeHash + "@" + genesis

	// 获取 FT 信息
	ftInfo, err := s.indexer.GetFtInfo(key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSON(http.StatusOK, gin.H{
				"codeHash": codeHash,
				"genesis":  genesis,
				"error":    "未找到该 FT 信息",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"codeHash":   ftInfo.CodeHash,
		"genesis":    ftInfo.Genesis,
		"sensibleId": ftInfo.SensibleId,
		"name":       ftInfo.Name,
		"symbol":     ftInfo.Symbol,
		"decimal":    ftInfo.Decimal,
	})
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
