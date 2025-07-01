package api

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/indexer"
	"github.com/metaid/utxo_indexer/mempool"
	"github.com/metaid/utxo_indexer/storage"
)

type Server struct {
	indexer     *indexer.UTXOIndexer
	Router      *gin.Engine
	mempoolMgr  *mempool.MempoolManager
	bcClient    *blockchain.Client
	metaStore   *storage.MetaStore
	stopCh      <-chan struct{}
	mempoolInit bool // Whether the mempool has been initialized
}

func NewServer(indexer *indexer.UTXOIndexer, metaStore *storage.MetaStore, stopCh <-chan struct{}) *Server {
	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = io.Discard
	server := &Server{
		indexer:     indexer,
		Router:      gin.Default(),
		mempoolInit: false,
		metaStore:   metaStore,
		stopCh:      stopCh,
	}

	server.setupRoutes()
	return server
}

// Set the mempool manager and blockchain client
func (s *Server) SetMempoolManager(mempoolMgr *mempool.MempoolManager, bcClient *blockchain.Client) {
	s.mempoolMgr = mempoolMgr
	s.bcClient = bcClient
}

func (s *Server) setupRoutes() {
	s.Router.GET("/balance", s.getBalance)
	s.Router.GET("/utxos", s.getUTXOs)
	s.Router.GET("/utxos/spend", s.getSpendUTXOs)
	s.Router.GET("/utxo/db", s.getUtxoByTx)
	s.Router.GET("/mempool/utxos", s.getMempoolUTXOs)
	s.Router.GET("/cleanedHeight/get", s.getCleanedHeight)
	// Add API to start the mempool
	s.Router.GET("/mempool/start", s.startMempool)
	// Mempool rebuild API
	s.Router.GET("/mempool/rebuild", s.rebuildMempool)
	// Reindex blocks API
	s.Router.GET("/blocks/reindex", s.reindexBlocks)
}

func (s *Server) StartMempoolCore() error {
	if s.mempoolMgr == nil || s.bcClient == nil {
		return fmt.Errorf("Mempool manager or blockchain client not configured")
	}
	if s.mempoolInit {
		return nil // Already started
	}

	log.Println("Starting ZMQ and mempool listener via API...")
	if err := s.mempoolMgr.Start(); err != nil {
		return fmt.Errorf("Failed to start mempool: %w", err)
	}
	s.mempoolInit = true
	log.Println("Mempool manager started via API, listening for new transactions...")

	// Initialize mempool data (load existing mempool transactions)
	go func() {
		log.Println("Starting to initialize mempool data...")
		s.mempoolMgr.InitializeMempool(s.bcClient)
		log.Println("Mempool data initialization complete")
	}()

	// Get current index height as the cleaning start height
	// lastIndexedHeightBytes, err := s.metaStore.Get([]byte("last_indexed_height"))
	// if err == nil {
	// 	log.Println("Setting mempool cleaning start height to current index height:", string(lastIndexedHeightBytes))
	// 	err = s.metaStore.Set([]byte("last_mempool_clean_height"), lastIndexedHeightBytes)
	// 	if err != nil {
	// 		log.Printf("Failed to set mempool cleaning start height: %v", err)
	// 	}
	// } else {
	// 	log.Printf("Failed to get current index height: %v", err)
	// }

	// Start mempool cleaning goroutine
	//go s.startMempoolCleaner()

	return nil
}

// Mempool cleaning goroutine
func (s *Server) startMempoolCleaner() {
	cleanInterval := 10 * time.Second
	for {
		select {
		case <-s.stopCh:
			return
		case <-time.After(cleanInterval):
			lastCleanHeight := 0
			lastCleanHeightBytes, err := s.metaStore.Get([]byte("last_mempool_clean_height"))
			if err == nil {
				lastCleanHeight, _ = strconv.Atoi(string(lastCleanHeightBytes))
			}
			lastIndexedHeight := 0
			lastIndexedHeightBytes, err := s.metaStore.Get([]byte("last_indexed_height"))
			if err == nil {
				lastIndexedHeight, _ = strconv.Atoi(string(lastIndexedHeightBytes))
			}
			if lastIndexedHeight > lastCleanHeight {
				log.Printf("Performing mempool cleaning from height %d to %d", lastCleanHeight+1, lastIndexedHeight)
				for height := lastCleanHeight + 1; height <= lastIndexedHeight; height++ {
					err := s.mempoolMgr.CleanByHeight(height, s.bcClient)
					if err != nil {
						log.Printf("Failed to clean height %d: %v", height, err)
					}
				}
				err := s.metaStore.Set([]byte("last_mempool_clean_height"), []byte(strconv.Itoa(lastIndexedHeight)))
				if err != nil {
					log.Printf("Failed to update last cleaned height: %v", err)
				}
			}
		}
	}
}

// Start mempool API
func (s *Server) startMempool(c *gin.Context) {
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
	// // Check if mempool manager is configured
	// if s.mempoolMgr == nil || s.bcClient == nil {
	// 	c.JSON(http.StatusInternalServerError, gin.H{
	// 		"success": false,
	// 		"error":   "Mempool manager or blockchain client not configured",
	// 	})
	// 	return
	// }

	// // Check if already initialized
	// if s.mempoolInit {
	// 	c.JSON(http.StatusOK, gin.H{
	// 		"success": true,
	// 		"message": "Mempool already started",
	// 		"status":  "running",
	// 	})
	// 	return
	// }

	// // Start mempool
	// log.Println("Starting ZMQ and mempool monitoring via API...")
	// err := s.mempoolMgr.Start()
	// if err != nil {
	// 	c.JSON(http.StatusInternalServerError, gin.H{
	// 		"success": false,
	// 		"error":   "Mempool startup failed: " + err.Error(),
	// 	})
	// 	return
	// }

	// // Mark as initialized
	// s.mempoolInit = true
	// log.Println("Mempool manager started via API, monitoring new transactions...")

	// // Initialize mempool data (load existing mempool transactions)
	// go func() {
	// 	log.Println("Starting mempool data initialization...")
	// 	s.mempoolMgr.InitializeMempool(s.bcClient)
	// 	log.Println("Mempool data initialization completed")
	// }()

	// // Get current index height as cleaning start height
	// lastIndexedHeightBytes, err := s.metaStore.Get([]byte("last_indexed_height"))
	// if err == nil {
	// 	// Set current height as cleaning start height to avoid cleaning historical blocks
	// 	log.Println("Setting mempool cleaning start height to current index height:", string(lastIndexedHeightBytes))
	// 	err = s.metaStore.Set([]byte("last_mempool_clean_height"), lastIndexedHeightBytes)
	// 	if err != nil {
	// 		log.Printf("Failed to set mempool cleaning start height: %v", err)
	// 	}
	// } else {
	// 	log.Printf("Failed to get current index height: %v", err)
	// }

	// // Start mempool cleaning goroutine
	// go func() {
	// 	// Mempool cleaning interval
	// 	cleanInterval := 10 * time.Second

	// 	for {
	// 		select {
	// 		case <-s.stopCh:
	// 			return
	// 		case <-time.After(cleanInterval):
	// 			// 1. Get last cleaned height
	// 			lastCleanHeight := 0
	// 			lastCleanHeightBytes, err := s.metaStore.Get([]byte("last_mempool_clean_height"))
	// 			if err == nil {
	// 				lastCleanHeight, _ = strconv.Atoi(string(lastCleanHeightBytes))
	// 			}

	// 			// 2. Get latest index height
	// 			lastIndexedHeight := 0
	// 			lastIndexedHeightBytes, err := s.metaStore.Get([]byte("last_indexed_height"))
	// 			if err == nil {
	// 				lastIndexedHeight, _ = strconv.Atoi(string(lastIndexedHeightBytes))
	// 			}
	// 			//lastCleanHeight = lastIndexedHeight - 1
	// 			// 3. If latest index height is greater than last cleaned height, perform cleaning
	// 			if lastIndexedHeight > lastCleanHeight {
	// 				log.Printf("Performing mempool cleaning from height %d to %d", lastCleanHeight+1, lastIndexedHeight)

	// 				// Clean each new block
	// 				for height := lastCleanHeight + 1; height <= lastIndexedHeight; height++ {
	// 					err := s.mempoolMgr.CleanByHeight(height, s.bcClient)
	// 					if err != nil {
	// 						log.Printf("Failed to clean height %d: %v", height, err)
	// 					}
	// 				}

	// 				// Update last cleaned height
	// 				err := s.metaStore.Set([]byte("last_mempool_clean_height"), []byte(strconv.Itoa(lastIndexedHeight)))
	// 				if err != nil {
	// 					log.Printf("Failed to update last cleaned height: %v", err)
	// 				}
	// 			}
	// 		}
	// 	}
	// }()

	// c.JSON(http.StatusOK, gin.H{
	// 	"success": true,
	// 	"message": "Mempool started successfully",
	// 	"status":  "running",
	// })
}
func (s *Server) RebuildMempool() error {
	return s.mempoolMgr.RebuildMempool()
}

// Rebuild mempool API
func (s *Server) rebuildMempool(c *gin.Context) {
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
	// Check if mempool manager is configured
	// 	if s.mempoolMgr == nil || s.bcClient == nil {
	// 		c.JSON(http.StatusInternalServerError, gin.H{
	// 			"success": false,
	// 			"error":   "Mempool manager or blockchain client not configured",
	// 		})
	// 		return
	// 	}
	// 	log.Println("Starting to clean and rebuild mempool data...")
	// 	// Check if mempool is already started
	// 	if s.mempoolInit {
	// 		// First stop current ZMQ connection (if any)
	// 		s.mempoolMgr.Stop()
	// 		log.Println("Stopped existing ZMQ connection and mempool monitoring")
	// 	}
	// 	// Clear all mempool data
	// 	err := s.mempoolMgr.CleanAllMempool()
	// 	if err != nil {
	// 		log.Printf("Error cleaning mempool data: %v, attempting to recreate mempool manager", err)

	// 		// Even if cleaning fails, continue to recreate mempool manager
	// 		// Get configuration
	// 		cfg, cfgErr := config.LoadConfig()
	// 		if cfgErr != nil {
	// 			c.JSON(http.StatusInternalServerError, gin.H{
	// 				"success": false,
	// 				"error":   "Failed to load configuration: " + cfgErr.Error(),
	// 			})
	// 			return
	// 		}

	// 		// Get chain parameters
	// 		chainCfg, cfgErr := cfg.GetChainParams()
	// 		if cfgErr != nil {
	// 			c.JSON(http.StatusInternalServerError, gin.H{
	// 				"success": false,
	// 				"error":   "Failed to get chain parameters: " + cfgErr.Error(),
	// 			})
	// 			return
	// 		}

	// 		// Recreate mempool manager
	// 		basePath := s.mempoolMgr.GetBasePath()
	// 		zmqAddress := s.mempoolMgr.GetZmqAddress()

	// 		// Recreate mempool manager
	// 		newMempoolMgr := mempool.NewMempoolManager(basePath, s.indexer.GetUtxoStore(), chainCfg, zmqAddress)
	// 		if newMempoolMgr == nil {
	// 			c.JSON(http.StatusInternalServerError, gin.H{
	// 				"success": false,
	// 				"error":   "Failed to recreate mempool manager",
	// 			})
	// 			return
	// 		}

	// 		// Update manager reference
	// 		s.mempoolMgr = newMempoolMgr
	// 		s.indexer.SetMempoolManager(newMempoolMgr)
	// 	}

	// 	// Get current index height as cleaning start height
	// 	lastIndexedHeightBytes, err := s.metaStore.Get([]byte("last_indexed_height"))
	// 	if err == nil {
	// 		// Set current height as cleaning start height to avoid cleaning historical blocks
	// 		log.Println("Setting mempool cleaning start height to current index height:", string(lastIndexedHeightBytes))
	// 		err = s.metaStore.Set([]byte("last_mempool_clean_height"), lastIndexedHeightBytes)
	// 		if err != nil {
	// 			log.Printf("Failed to set mempool cleaning start height: %v", err)
	// 		}
	// 	} else {
	// 		log.Printf("Failed to get current index height: %v", err)
	// 	}

	// 	// Restart ZMQ connection
	// 	log.Println("Restarting ZMQ connection...")
	// 	err = s.mempoolMgr.Start()
	// 	if err != nil {
	// 		c.JSON(http.StatusInternalServerError, gin.H{
	// 			"success": false,
	// 			"error":   "Failed to restart ZMQ connection: " + err.Error(),
	// 		})
	// 		return
	// 	}

	// 	// Log restart success message
	// 	log.Println("ZMQ connection restarted successfully, should now be able to monitor new transactions")

	// 	// Start reinitializing mempool
	// 	go func() {
	// 		log.Println("Starting to reinitialize mempool data...")
	// 		s.mempoolMgr.InitializeMempool(s.bcClient)
	// 		log.Println("Mempool data reinitialization completed, system should now be able to handle new transactions normally")
	// 	}()

	// 	c.JSON(http.StatusOK, gin.H{
	// 		"success": true,
	// 		"message": "Mempool data cleaned, ZMQ restarted, mempool rebuilding in progress",
	// 	})
}

// reindexBlocks reindexes blocks in the specified range
func (s *Server) reindexBlocks(c *gin.Context) {
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
			"error":   "invalid height range, start must be greater than or equal to 0, end must be greater than or equal to start",
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
		//s.indexer.InitProgressBar(endHeight, startHeight-1)

		// Process each block
		for height := startHeight; height <= endHeight; height++ {
			// Use shared block processing function
			if err := s.bcClient.ProcessBlock(s.indexer, height, false); err != nil {
				log.Printf("Failed to process block at height %d: %v", height, err)
				continue // Continue processing next block instead of terminating entire reindexing process
			}
		}

		log.Printf("Reindexing completed, processed %d blocks, from height %d to %d", blocksToProcess, startHeight, endHeight)
	}()
}

func (s *Server) getBalance(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address parameter is required"})
		return
	}

	balance, err := s.indexer.GetBalance(address)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, balance)
}

func (s *Server) getUTXOs(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address parameter is required"})
		return
	}

	utxos, err := s.indexer.GetUTXOs(address)
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
func (s *Server) getSpendUTXOs(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address parameter is required"})
		return
	}

	utxos, err := s.indexer.GetSpendUTXOs(address)
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
func (s *Server) getUtxoByTx(c *gin.Context) {
	tx := c.Query("tx")
	if tx == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tx parameter is required"})
		return
	}

	utxos, err := s.indexer.GetDbUtxoByTx(tx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"utxos": string(utxos),
	})
}
func (s *Server) getCleanedHeight(c *gin.Context) {
	dbHeight, err := s.metaStore.Get([]byte("last_mempool_clean_height"))
	if err != nil {
		dbHeight = []byte("0")
	}
	c.JSON(http.StatusOK, gin.H{
		"CleanedHeight": indexer.CleanedHeight,
		"dbHeight":      string(dbHeight),
	})
}
func (s *Server) getMempoolUTXOs(c *gin.Context) {
	address := c.Query("address")
	if address == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "address parameter is required"})
		return
	}

	imcome, spend, err := s.indexer.GetMempoolUTXOs(address)
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

func (s *Server) Start(addr string) error {
	// Start the server
	err := s.Router.Run(addr)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
		return err
	}
	return nil
}
