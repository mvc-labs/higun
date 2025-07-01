package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/metaid/utxo_indexer/api"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/explorer/blockindexer"
	"github.com/metaid/utxo_indexer/indexer"
	"github.com/metaid/utxo_indexer/mempool"
	"github.com/metaid/utxo_indexer/storage"
)

var ApiServer *api.Server

func main() {
	// Load config
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	config.GlobalConfig = cfg
	config.GlobalNetwork, _ = cfg.GetChainParams()
	// Execute block info indexing
	fmt.Println("Initializing block info index...")
	blockindexer.IndexerInit("blockinfo_data", cfg)
	go blockindexer.DoBlockInfoIndex()
	go blockindexer.SaveBlockInfoData()
	log.Println("0.==============>")
	// Create auto configuration
	params := config.AutoConfigure(config.SystemResources{
		CPUCores:   cfg.CPUCores,
		MemoryGB:   cfg.MemoryGB,
		HighPerf:   cfg.HighPerf,
		ShardCount: cfg.ShardCount,
	})
	params.MaxTxPerBatch = config.GlobalConfig.MaxTxPerBatch
	common.InitBytePool(params.BytePoolSizeKB)
	log.Println("1.==============>")
	storage.DbInit(params)
	log.Println("2.==============>")
	// Initialize storage
	utxoStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUTXO, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize UTXO storage: %v", err)
	}
	defer utxoStore.Close()
	log.Println("3.==============>")
	addressStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize address storage: %v", err)
	}
	log.Println("4.==============>")
	defer addressStore.Close()

	spendStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeSpend, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize spend storage: %v", err)
	}
	defer spendStore.Close()
	log.Println("5.==============>")

	// Create blockchain client (create early for mempool cleanup use)
	bcClient, err := blockchain.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create blockchain client: %v", err)
	}
	defer bcClient.Shutdown()
	log.Println("6.==============>")
	// Create metadata storage (create early for mempool cleanup use)
	metaStore, err := storage.NewMetaStore(cfg.DataDir)
	if err != nil {
		log.Fatalf("Failed to create metadata storage: %v", err)
	}
	defer metaStore.Close()
	log.Println("7.==============>")
	// Verify last indexed height
	lastHeight, err := metaStore.Get([]byte("last_indexed_height"))
	if err == nil {
		log.Printf("Resuming index from height %s", lastHeight)
	} else if errors.Is(err, storage.ErrNotFound) {
		log.Println("Starting new index from genesis block")
	} else {
		log.Printf("Error reading last height: %v", err)
	}

	// Force sync meta store to ensure durability
	if err := metaStore.Sync(); err != nil {
		log.Printf("Failed to sync metadata storage: %v", err)
	}

	// Ensure last_mempool_clean_height has initial value
	_, err = metaStore.Get([]byte("last_mempool_clean_height"))
	if errors.Is(err, storage.ErrNotFound) {
		// First run, use start height from config file
		startHeight := strconv.Itoa(cfg.MemPoolCleanStartHeight)
		log.Printf("Initializing mempool cleanup height to: %s", startHeight)
		err = metaStore.Set([]byte("last_mempool_clean_height"), []byte(startHeight))
		if err != nil {
			log.Printf("Failed to initialize mempool cleanup height: %v", err)
		}
	}

	// Create stop signal channel
	stopCh := make(chan struct{})

	// Capture interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Handle interrupt signal
	go func() {
		<-sigCh
		log.Println("Received stop signal, preparing to shutdown...")
		close(stopCh)
	}()

	// Get chain parameters
	chainCfg, err := cfg.GetChainParams()
	if err != nil {
		log.Fatalf("Failed to get chain parameters: %v", err)
	}

	// Create mempool manager, but don't start
	log.Printf("Initializing mempool manager, ZMQ address: %s, network: %s", cfg.ZMQAddress, cfg.Network)
	mempoolMgr := mempool.NewMempoolManager(cfg.DataDir, utxoStore, chainCfg, cfg.ZMQAddress)
	if mempoolMgr == nil {
		log.Printf("Failed to create mempool manager")
	}

	idx := indexer.NewUTXOIndexer(params, utxoStore, addressStore, metaStore, spendStore)

	// Set mempool manager so indexer can query mempool UTXOs
	if mempoolMgr != nil {
		idx.SetMempoolManager(mempoolMgr)
	}
	// Pass mempool manager and blockchain client to API server
	ApiServer = api.NewServer(idx, metaStore, stopCh)
	ApiServer.SetMempoolManager(mempoolMgr, bcClient)
	log.Printf("Starting UTXO indexer API, port: %s", cfg.APIPort)
	blockindexer.SetRouter(ApiServer)
	go ApiServer.Start(fmt.Sprintf(":%s", cfg.APIPort))
	// Get current blockchain height
	var bestHeight int
	//for {
	bestHeight, err = bcClient.GetBlockCount()
	if err != nil {
		log.Printf("Failed to get block count: %v, retrying in 3 seconds...", err)
		//time.Sleep(3 * time.Second)
		//continue
	}
	lastCleanHeightInt := int64(0)
	lastCleanHeight, err := metaStore.Get([]byte("last_mempool_clean_height"))
	if err == nil {
		log.Printf("Resuming from mempool cleanup height %s", string(lastCleanHeight))
		lastCleanHeightInt, _ = strconv.ParseInt(string(lastCleanHeight), 10, 64)
	}
	if lastCleanHeightInt == 0 {
		lastCleanHeightInt = int64(bestHeight)
	}
	indexer.CleanedHeight = lastCleanHeightInt
	//	break
	//}

	lastHeightInt, err := strconv.Atoi(string(lastHeight))
	if err != nil {
		lastHeightInt = 0
		log.Printf("Failed to convert last height, starting from 0: %v", err)
	}
	//lastHeightInt = 121050
	// Initialize progress bar
	idx.InitProgressBar(bestHeight, lastHeightInt)

	// Interval to check for new blocks
	checkInterval := 10 * time.Second

	log.Println("Starting block synchronization...")
	//log.Println("Note: Mempool not automatically started, please use API '/mempool/start' to start mempool after block sync is complete")

	// Use goroutine to start block synchronization, no longer automatically start mempool
	go func() {
		if err := bcClient.SyncBlocks(idx, checkInterval, stopCh, firstSyncCompleted); err != nil {
			log.Printf("Block synchronization failed: %v, retrying in 3 seconds...", err)
			select {
			case <-stopCh:
				return
			case <-time.After(3 * time.Second):
				// Continue retry
			}
		} else {
			// Normal exit (usually won't reach here)
			return
		}
	}()

	// Wait for stop signal
	<-stopCh
	log.Println("Program is shutting down...")

	// Stop mempool (if started)
	if mempoolMgr != nil {
		mempoolMgr.Stop()
	}

	// Program won't execute here unless stop signal is received
	finalHeight, err := idx.GetLastIndexedHeight()
	if err != nil {
		log.Printf("Error getting final indexed height: %v", err)
	} else {
		log.Printf("Final indexed height: %d", finalHeight)
	}
}
func firstSyncCompleted() {
	//return
	log.Println("Initial sync completed, starting mempool")
	err := ApiServer.RebuildMempool()
	if err != nil {
		log.Printf("Failed to rebuild mempool: %v", err)
		return
	}
	err = ApiServer.StartMempoolCore()
	if err != nil {
		log.Printf("Failed to start mempool core: %v", err)
		return
	}
	log.Println("Mempool core started")
}
