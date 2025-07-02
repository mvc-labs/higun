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

	indexer "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"
	"github.com/metaid/utxo_indexer/mempool"

	"github.com/metaid/utxo_indexer/api"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/storage"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	fmt.Println("cfg", cfg)
	config.GlobalConfig = cfg
	config.GlobalNetwork, _ = cfg.GetChainParams()

	// Create auto configuration
	params := config.AutoConfigure(config.SystemResources{
		CPUCores:   cfg.CPUCores,
		MemoryGB:   cfg.MemoryGB,
		HighPerf:   cfg.HighPerf,
		ShardCount: cfg.ShardCount,
	})
	params.MaxTxPerBatch = cfg.MaxTxPerBatch
	common.InitBytePool(params.BytePoolSizeKB)
	storage.DbInit(params)

	// Initialize storage
	contractFtUtxoStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTUTXO, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize FT storage: %v", err)
	}
	defer contractFtUtxoStore.Close()

	addressFtIncomeStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeAddressFTIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize FT address storage: %v", err)
	}
	defer addressFtIncomeStore.Close()

	addressFtSpendStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeAddressFTSpend, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize FT spend storage: %v", err)
	}
	defer addressFtSpendStore.Close()

	contractFtInfoStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTInfo, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize FT info storage: %v", err)
	}
	defer contractFtInfoStore.Close()

	contractFtGenesisStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTGenesis, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize FT genesis storage: %v", err)
	}
	defer contractFtGenesisStore.Close()

	contractFtGenesisOutputStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTGenesisOutput, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize FT genesis output storage: %v", err)
	}
	defer contractFtGenesisOutputStore.Close()

	contractFtGenesisUtxoStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTGenesisUTXO, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize FT genesis UTXO storage: %v", err)
	}
	defer contractFtGenesisUtxoStore.Close()

	addressFtIncomeValidStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeAddressFTIncomeValid, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize valid address FT UTXO storage: %v", err)
	}
	defer addressFtIncomeValidStore.Close()

	uncheckFtOutpointStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUnCheckFtIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize FT verification storage: %v", err)
	}
	defer uncheckFtOutpointStore.Close()

	usedFtIncomeStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUsedFTIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize used FT contract UTXO storage: %v", err)
	}
	defer usedFtIncomeStore.Close()

	uniqueFtIncomeStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUniqueFTIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize unique contract UTXO storage: %v", err)
	}
	defer uniqueFtIncomeStore.Close()

	uniqueFtSpendStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUniqueFTSpend, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize unique contract UTXO storage: %v", err)
	}
	defer uniqueFtSpendStore.Close()

	invalidFtOutpointStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeInvalidFtOutpoint, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize invalid FT contract UTXO storage: %v", err)
	}
	defer invalidFtOutpointStore.Close()

	// Create blockchain client
	bcClient, err := blockchain.NewFtClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create blockchain client: %v", err)
	}
	defer bcClient.Shutdown()

	// Create metadata storage
	metaStore, err := storage.NewMetaStore(cfg.DataDir)
	if err != nil {
		log.Fatalf("Failed to create metadata storage: %v", err)
	}
	defer metaStore.Close()

	// Verify last indexed height
	lastHeight, err := metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
	if err == nil {
		log.Printf("Resuming FT indexing from height %s", lastHeight)
	} else if errors.Is(err, storage.ErrNotFound) {
		log.Println("Starting new FT indexing from genesis block")
	} else {
		log.Printf("Error reading last FT height: %v", err)
	}

	// Force sync metadata storage to ensure persistence
	if err := metaStore.Sync(); err != nil {
		log.Printf("Failed to sync metadata storage: %v", err)
	}

	// Create stop signal channel
	stopCh := make(chan struct{})

	// Capture interrupt signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Handle interrupt signals
	go func() {
		<-sigCh
		log.Println("Received stop signal, preparing to shutdown...")
		close(stopCh)
	}()

	// Create FT indexer
	idx := indexer.NewContractFtIndexer(params,
		contractFtUtxoStore,
		addressFtIncomeStore,
		addressFtSpendStore,
		contractFtInfoStore,
		contractFtGenesisStore,
		contractFtGenesisOutputStore,
		contractFtGenesisUtxoStore,
		addressFtIncomeValidStore,
		uncheckFtOutpointStore,
		usedFtIncomeStore,
		uniqueFtIncomeStore,
		uniqueFtSpendStore,
		invalidFtOutpointStore,
		metaStore)

	// Create and start FT verification manager
	verifyManager := indexer.NewFtVerifyManager(idx, 5*time.Second, 1000, params.WorkerCount)
	if err := verifyManager.Start(); err != nil {
		log.Printf("Failed to start FT verification manager: %v", err)
	} else {
		log.Println("FT verification manager started")
	}
	defer verifyManager.Stop()

	// Create mempool manager but don't start it
	log.Printf("Initializing mempool manager, ZMQ address: %s, network: %s", cfg.ZMQAddress, cfg.Network)
	mempoolMgr := mempool.NewFtMempoolManager(cfg.DataDir,
		contractFtUtxoStore,
		contractFtInfoStore,
		contractFtGenesisStore,
		contractFtGenesisOutputStore,
		contractFtGenesisUtxoStore,
		config.GlobalNetwork, cfg.ZMQAddress)
	if mempoolMgr == nil {
		log.Printf("Failed to create mempool manager")
	}
	// Set mempool manager, so indexer can query mempool UTXO
	if mempoolMgr != nil {
		idx.SetMempoolManager(mempoolMgr)
	}
	// Create and start FT verification manager
	mempoolVerifyManager := mempool.NewFtMempoolVerifier(mempoolMgr, 2*time.Second, 1000, params.WorkerCount)
	if err := mempoolVerifyManager.Start(); err != nil {
		log.Printf("Failed to start mempool FT verification manager: %v", err)
	} else {
		log.Println("mempool FT verification manager started")
	}
	defer mempoolVerifyManager.Stop()

	// Get current blockchain height
	bestHeight, err := bcClient.GetBlockCount()
	if err != nil {
		log.Fatalf("Failed to get block count: %v", err)
	}

	// Start API server
	server := api.NewFtServer(bcClient, idx, metaStore, stopCh)
	log.Printf("Starting FT-UTXO indexer API, port: %s", cfg.APIPort)
	server.SetMempoolManager(mempoolMgr, bcClient)
	go server.Start(fmt.Sprintf(":%s", cfg.APIPort))

	lastHeightInt, err := strconv.Atoi(string(lastHeight))
	if err != nil {
		lastHeightInt = 0
		log.Printf("Failed to convert last height, starting from 0: %v", err)
	}

	// Initialize progress bar
	idx.InitProgressBar(bestHeight, lastHeightInt)

	// Check interval for new blocks
	checkInterval := 10 * time.Second
	log.Printf("Syncing index to %d height\n", lastHeightInt)

	log.Println("Starting FT block sync...")

	firstSyncCompleted := func() {
		log.Println("Initial sync completed, starting mempool")
		err := server.RebuildMempool()
		if err != nil {
			log.Printf("Failed to rebuild mempool: %v", err)
			return
		}
		err = server.StartMempoolCore()
		if err != nil {
			log.Printf("Failed to start mempool core: %v", err)
			return
		}
		log.Println("mempool core started")
	}

	// Use goroutine to start block sync
	go func() {
		if err := bcClient.SyncBlocks(idx, checkInterval, stopCh, firstSyncCompleted); err != nil {
			log.Fatalf("Failed to sync FT blocks: %v", err)
		}
	}()

	// Wait for stop signal
	<-stopCh
	log.Println("Program is shutting down...")

	// Get final indexed height
	finalHeight, err := idx.GetLastIndexedHeight()
	if err != nil {
		log.Printf("Error getting final FT indexed height: %v", err)
	} else {
		log.Printf("Final FT indexed height: %d", finalHeight)
	}
}
