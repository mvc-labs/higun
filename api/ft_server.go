package api

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/metaid/utxo_indexer/api/respond"
	"github.com/metaid/utxo_indexer/common"
	indexer "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"

	"github.com/gin-gonic/gin"
	"github.com/metaid/utxo_indexer/blockchain"
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
	mempoolInit bool // Whether mempool is initialized
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

// Set mempool manager and blockchain client
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
	s.router.GET("/db/ft/invalid/outpoint", s.getDbInvalidFtOutpoint)

	s.router.GET("/ft/mempool/utxos", s.getFtMempoolUTXOs)

	// Add mempool start API
	s.router.GET("/ft/mempool/start", s.startMempool)
	// Mempool rebuild API
	s.router.GET("/ft/mempool/rebuild", s.rebuildMempool)
	// Reindex blocks API
	s.router.GET("/ft/blocks/reindex", s.reindexBlocks)

	// Add new mempool query interfaces
	s.router.GET("/db/ft/mempool/verify/tx", s.getMempoolVerifyTx)
	s.router.GET("/db/ft/mempool/uncheck/utxo", s.getMempoolUncheckFtUtxo)
	s.router.GET("/db/ft/mempool/spend", s.getMempoolAddressFtSpendMap)
	s.router.GET("/db/ft/mempool/unique/spend", s.getMempoolUniqueFtSpendMap)
	s.router.GET("/db/ft/mempool/address/income", s.getMempoolAddressFtIncomeMap)
	s.router.GET("/db/ft/mempool/address/income/valid", s.getMempoolAddressFtIncomeValidMap)
}

// Start mempool API
func (s *FtServer) StartMempoolCore() error {
	// Check if mempool manager is configured
	if s.mempoolMgr == nil || s.bcClient == nil {
		return fmt.Errorf("mempool manager or blockchain client not configured")
	}

	// Check if already initialized
	if s.mempoolInit {
		return nil
	}

	// Start mempool
	log.Println("Starting ZMQ and mempool monitoring via API...")
	err := s.mempoolMgr.Start()
	if err != nil {
		return fmt.Errorf("mempool startup failed: %w", err)
	}

	// Mark as initialized
	s.mempoolInit = true
	log.Println("Mempool manager started via API, monitoring new transactions...")

	// Initialize mempool data (load existing mempool transactions)
	go func() {
		log.Println("Starting FT mempool data initialization...")
		s.mempoolMgr.InitializeMempool(s.bcClient)
		log.Println("FT mempool data initialization completed")
	}()

	// Get current index height as cleanup start height
	lastIndexedHeightBytes, err := s.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
	if err == nil {
		// Set current height as cleanup start height to avoid cleaning historical blocks
		log.Println("Setting mempool cleanup start height to current index height:", string(lastIndexedHeightBytes))
		err = s.metaStore.Set([]byte(common.MetaStoreKeyLastFtMempoolCleanHeight), lastIndexedHeightBytes)
		if err != nil {
			log.Printf("Failed to set mempool cleanup start height: %v", err)
		}
	} else {
		log.Printf("Failed to get current index height: %v", err)
	}

	go s.startMempoolCleaner()
	return nil
}

func (s *FtServer) startMempoolCleaner() {
	// Mempool cleanup interval
	cleanInterval := 10 * time.Second

	for {
		select {
		case <-s.stopCh:
			return
		case <-time.After(cleanInterval):
			// 1. Get last cleaned height
			lastCleanHeight := 0
			lastCleanHeightBytes, err := s.metaStore.Get([]byte(common.MetaStoreKeyLastFtMempoolCleanHeight))
			if err == nil {
				lastCleanHeight, _ = strconv.Atoi(string(lastCleanHeightBytes))
			}

			// 2. Get latest indexed height
			lastIndexedHeight := 0
			lastIndexedHeightBytes, err := s.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
			if err == nil {
				lastIndexedHeight, _ = strconv.Atoi(string(lastIndexedHeightBytes))
			}

			// 3. If latest indexed height is greater than last cleaned height, perform cleanup
			if lastIndexedHeight > lastCleanHeight {
				log.Printf("Performing mempool cleanup from height %d to %d", lastCleanHeight+1, lastIndexedHeight)

				// Perform cleanup for each new block
				for height := lastCleanHeight + 1; height <= lastIndexedHeight; height++ {
					err := s.mempoolMgr.CleanByHeight(height, s.bcClient)
					if err != nil {
						log.Printf("Failed to clean height %d: %v", height, err)
					}
				}

				// Update last cleaned height
				err := s.metaStore.Set([]byte(common.MetaStoreKeyLastFtMempoolCleanHeight), []byte(strconv.Itoa(lastIndexedHeight)))
				if err != nil {
					log.Printf("Failed to update last cleaned height: %v", err)
				}
			}
		}
	}
}

func (s *FtServer) startMempool(c *gin.Context) {
	err := s.StartMempoolCore()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Mempool started successfully",
		"status":  "running",
	})
}

func (s *FtServer) RebuildMempool() error {
	return s.mempoolMgr.CleanAllMempool()
}

// Rebuild mempool API
func (s *FtServer) rebuildMempool(c *gin.Context) {
	// Check if mempool manager is configured
	err := s.RebuildMempool()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	err = s.StartMempoolCore()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Mempool started successfully",
		"status":  "running",
	})

	// /

	// // Check if mempool is started
	// if !s.mempoolInit {
	// 	c.JSON(http.StatusBadRequest, gin.H{
	// 		"success": false,
	// 		"error":   "Mempool not started, please use /ft/mempool/start interface to start mempool first",
	// 	})
	// 	return
	// }

	// log.Println("Starting to clean and rebuild mempool data...")

	// // First stop current ZMQ connection (if any)
	// s.mempoolMgr.Stop()
	// log.Println("Stopped existing ZMQ connection and mempool monitoring")

	// // Clear all mempool data
	// err := s.mempoolMgr.CleanAllMempool()
	// if err != nil {
	// 	log.Printf("Error cleaning mempool data: %v, trying to recreate mempool manager", err)

	// 	// Even if cleanup fails, continue to recreate mempool manager
	// 	// Get configuration
	// 	cfg, cfgErr := config.LoadConfig()
	// 	if cfgErr != nil {
	// 		c.JSON(http.StatusInternalServerError, gin.H{
	// 			"success": false,
	// 			"error":   "Failed to load configuration: " + cfgErr.Error(),
	// 		})
	// 		return
	// 	}

	// 	// Get chain parameters
	// 	chainCfg, cfgErr := cfg.GetChainParams()
	// 	if cfgErr != nil {
	// 		c.JSON(http.StatusInternalServerError, gin.H{
	// 			"success": false,
	// 			"error":   "Failed to get chain parameters: " + cfgErr.Error(),
	// 		})
	// 		return
	// 	}

	// 	// Recreate mempool manager
	// 	basePath := s.mempoolMgr.GetBasePath()
	// 	zmqAddress := s.mempoolMgr.GetZmqAddress()

	// 	// Recreate mempool manager
	// 	newMempoolMgr := mempool.NewFtMempoolManager(basePath,
	// 		s.indexer.GetContractFtUtxoStore(),
	// 		s.indexer.GetContractFtInfoStore(),
	// 		s.indexer.GetContractFtGenesisStore(),
	// 		s.indexer.GetContractFtGenesisOutputStore(),
	// 		s.indexer.GetContractFtGenesisUtxoStore(),
	// 		chainCfg, zmqAddress)
	// 	if newMempoolMgr == nil {
	// 		c.JSON(http.StatusInternalServerError, gin.H{
	// 			"success": false,
	// 			"error":   "Failed to recreate mempool manager",
	// 		})
	// 		return
	// 	}

	// 	// Update manager reference
	// 	s.mempoolMgr = newMempoolMgr
	// 	s.indexer.SetMempoolManager(newMempoolMgr)
	// }

	// // Get current index height as cleanup start height
	// lastIndexedHeightBytes, err := s.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
	// if err == nil {
	// 	// Set current height as cleanup start height to avoid cleaning historical blocks
	// 	log.Println("Setting mempool cleanup start height to current index height:", string(lastIndexedHeightBytes))
	// 	err = s.metaStore.Set([]byte(common.MetaStoreKeyLastFtMempoolCleanHeight), lastIndexedHeightBytes)
	// 	if err != nil {
	// 		log.Printf("Failed to set mempool cleanup start height: %v", err)
	// 	}
	// } else {
	// 	log.Printf("Failed to get current index height: %v", err)
	// }

	// // Restart ZMQ connection
	// log.Println("Restarting ZMQ connection...")
	// err = s.mempoolMgr.Start()
	// if err != nil {
	// 	c.JSON(http.StatusInternalServerError, gin.H{
	// 		"success": false,
	// 		"error":   "Failed to restart ZMQ connection: " + err.Error(),
	// 	})
	// 	return
	// }

	// // Record restart success message
	// log.Println("ZMQ connection restarted successfully, should now be able to monitor new transactions")

	// // Start reinitializing mempool
	// go func() {
	// 	log.Println("Starting to reinitialize mempool data...")
	// 	s.mempoolMgr.InitializeMempool(s.bcClient)
	// 	log.Println("Mempool data reinitialization completed, system should now be able to process new transactions normally")
	// }()

	// c.JSON(http.StatusOK, gin.H{
	// 	"success": true,
	// 	"message": "Mempool data cleaned, ZMQ restarted, mempool is being rebuilt",
	// })
}

// reindexBlocks reindexes blocks in specified range
func (s *FtServer) reindexBlocks(c *gin.Context) {

	// //Configure blockchain client
	// cfg, err := config.LoadConfig()
	// if err != nil {
	// 	c.JSON(http.StatusInternalServerError, gin.H{
	// 		"success": false,
	// 		"error":   "Failed to load configuration: " + err.Error(),
	// 	})
	// 	return
	// }
	// bcClient, err := blockchain.NewFtClient(cfg)
	// if err != nil {
	// 	c.JSON(http.StatusInternalServerError, gin.H{
	// 		"success": false,
	// 		"error":   "Failed to create blockchain client: " + err.Error(),
	// 	})
	// 	return
	// }
	// s.bcClient = bcClient

	// Check if blockchain client is configured
	if s.bcClient == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Blockchain client not configured",
		})
		return
	}

	// Parse request parameters
	startHeightStr := c.Query("start")
	endHeightStr := c.Query("end")

	if startHeightStr == "" || endHeightStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "start and end parameters are required",
		})
		return
	}

	startHeight, err := strconv.Atoi(startHeightStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "start parameter must be a valid integer",
		})
		return
	}

	endHeight, err := strconv.Atoi(endHeightStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "end parameter must be a valid integer",
		})
		return
	}

	// Validate height range
	if startHeight < 0 || endHeight < startHeight {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid height range, start must be greater than or equal to 0, end must be greater than or equal to start",
		})
		return
	}

	// Check current latest block height
	currentHeight, err := s.bcClient.GetBlockCount()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to get current block height: " + err.Error(),
		})
		return
	}

	if endHeight > currentHeight {
		endHeight = currentHeight
	}

	// Return response immediately, start reindexing in background
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": fmt.Sprintf("Starting to reindex blocks, range from %d to %d", startHeight, endHeight),
	})

	// Start reindexing process in background
	go func() {
		log.Printf("Starting to reindex blocks, range from %d to %d", startHeight, endHeight)

		// Set progress bar
		blocksToProcess := endHeight - startHeight + 1

		// Process each block
		for height := startHeight; height <= endHeight; height++ {
			// Use shared block processing function
			if err := s.bcClient.ProcessBlock(s.indexer, height, false); err != nil {
				log.Printf("Failed to process block, height %d: %v", height, err)
				continue // Continue processing next block instead of terminating entire reindex process
			}
		}

		log.Printf("Reindexing completed, processed %d blocks, from height %d to %d", blocksToProcess, startHeight, endHeight)
	}()
}

// getMempoolVerifyTx gets mempool verification transaction information
func (s *FtServer) getMempoolVerifyTx(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	txId := c.Query("txId")

	// Check if mempool manager is configured
	if s.mempoolMgr == nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(errors.New("mempool manager not configured"), time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// Get pagination parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// Get verification transaction information
	txs, total, err := s.mempoolMgr.GetVerifyTx(txId, page, pageSize)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// Calculate total pages
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

// getMempoolUncheckFtUtxo gets unchecked FT UTXO information from mempool
func (s *FtServer) getMempoolUncheckFtUtxo(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	outpoint := c.Query("outpoint")

	// Check if mempool manager is configured
	if s.mempoolMgr == nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(errors.New("mempool manager not configured"), time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// Get pagination parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// Get unchecked FT UTXO list
	utxoList, err := s.mempoolMgr.GetUncheckFtUtxo()
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// If outpoint is provided, filter results
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

	// Calculate pagination
	total := len(utxoList)
	totalPages := (total + pageSize - 1) / pageSize

	// Get current page data
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// Extract current page data
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

// getDbInvalidFtOutpoint gets invalid FT contract UTXO data
func (s *FtServer) getDbInvalidFtOutpoint(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	outpoint := c.Query("outpoint")

	if outpoint == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("outpoint parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	// Query invalid FT contract UTXO data
	value, err := s.indexer.QueryInvalidFtOutpoint(outpoint)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	if value == "" {
		c.JSONP(http.StatusOK, respond.RespSuccess(gin.H{
			"outpoint": outpoint,
			"data":     nil,
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// Parse returned data
	// Format: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height@reason
	parts := strings.Split(value, "@")
	if len(parts) != 10 {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(errors.New("invalid data format"), time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(gin.H{
		"outpoint": outpoint,
		"data": gin.H{
			"ft_address":  parts[0],
			"code_hash":   parts[1],
			"genesis":     parts[2],
			"sensible_id": parts[3],
			"amount":      parts[4],
			"tx_id":       parts[5],
			"index":       parts[6],
			"value":       parts[7],
			"height":      parts[8],
			"reason":      parts[9],
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
