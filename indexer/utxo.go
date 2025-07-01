package indexer

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"

	"github.com/mattn/go-colorable"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/storage"
	"github.com/schollz/progressbar/v3"
)

type UTXOIndexer struct {
	utxoStore      *storage.PebbleStore
	addressStore   *storage.PebbleStore
	spendStore     *storage.PebbleStore
	metaStore      *storage.MetaStore
	mu             sync.RWMutex
	bar            *progressbar.ProgressBar
	params         config.IndexerParams
	mempoolManager MempoolManager // Use interface type instead of interface{}
}

var workers = 1

var batchSize = 1000
var CleanedHeight int64 // Used to record cleanup height
func NewUTXOIndexer(params config.IndexerParams, utxoStore, addressStore *storage.PebbleStore, metaStore *storage.MetaStore, spendStore *storage.PebbleStore) *UTXOIndexer {
	return &UTXOIndexer{
		params:       params,
		utxoStore:    utxoStore,
		addressStore: addressStore,
		metaStore:    metaStore,
		spendStore:   spendStore,
	}
}

// func (i *UTXOIndexer) optimizeConcurrency(dataSize int) int {
// 	// Dynamically adjust concurrency based on data size and system resources
// 	// Use fewer goroutines for small data, limit max concurrency for large data
// 	optimalWorkers := i.workers

// 	if dataSize < 1000 {
// 		// Use fewer goroutines for small data
// 		optimalWorkers = min(i.workers, 2)
// 	} else if dataSize > 10000 {
// 		// Limit max goroutines for large data
// 		maxWorkers := runtime.NumCPU() * 2 // Or set a fixed upper limit
// 		optimalWorkers = min(i.workers, maxWorkers)
// 	}

//		return optimalWorkers
//	}
func (i *UTXOIndexer) InitProgressBar(totalBlocks, startHeight int) {
	remainingBlocks := totalBlocks - startHeight
	if remainingBlocks <= 0 {
		remainingBlocks = 1 // Set to at least 1 to avoid errors
	}
	i.bar = progressbar.NewOptions(remainingBlocks,
		progressbar.OptionSetWriter(colorable.NewColorableStdout()),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSetDescription("Indexing blocks..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionSetRenderBlankState(false), // Don't show blank state
		progressbar.OptionShowCount(),                // Show count (e.g., 1/3)
		progressbar.OptionShowIts(),                  // Show iterations
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(colorable.NewColorableStdout(), "\nDone!\n")
		}),
		//progressbar.OptionSetFormat("%s %s %d/%d (%.2f ops/s)"), // Custom format: progress bar + progress + speed
	)
}
func (i *UTXOIndexer) SetMempoolCleanedHeight(height int64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	CleanedHeight = height
	i.metaStore.Set([]byte("last_mempool_clean_height"), []byte(strconv.FormatInt(height, 10)))
}
func (i *UTXOIndexer) IndexBlock(block *Block, updateHeight bool) error {
	if block == nil {
		return fmt.Errorf("cannot index nil block")
	}

	// Set global worker count and batch size
	workers = config.GlobalConfig.Workers
	batchSize = config.GlobalConfig.BatchSize

	// Validate block
	if err := block.Validate(); err != nil {
		return fmt.Errorf("invalid block: %w", err)
	}

	// Since batch processing is already done in the convertBlock stage, complex large block processing logic is no longer needed here
	// Directly process transactions in the current batch

	// Phase 1: Index all outputs
	if err := i.indexIncome(block); err != nil {
		return fmt.Errorf("failed to index outputs: %w", err)
	}
	// After phase 1 is complete, some memory can be released
	block.AddressIncome = nil

	// Phase 2: Process all inputs
	if err := i.processSpend(block); err != nil {
		return fmt.Errorf("failed to process inputs: %w", err)
	}
	// After phase 2 is complete, release transaction data
	block.Transactions = nil

	// If it's a partial batch of a large block, don't update the index height, wait for the last batch
	if !block.IsPartialBlock && updateHeight {
		// Only save height when processing the last batch of the entire block
		heightStr := strconv.Itoa(block.Height)
		if err := i.metaStore.Set([]byte("last_indexed_height"), []byte(heightStr)); err != nil {
			return err
		}

		if err := i.metaStore.Sync(); err != nil {
			log.Printf("Failed to sync meta store: %v", err)
			return err
		}

		// Update progress bar
		if i.bar != nil {
			i.bar.Add(1)
		}
	}

	// Finally release the block object
	block = nil
	runtime.GC()
	return nil
}

func (i *UTXOIndexer) indexIncome(block *Block) error {
	// Set reasonable batch size based on memory conditions
	//const batchSize = 1000
	workers = config.GlobalConfig.Workers
	batchSize = config.GlobalConfig.BatchSize
	// Calculate the number of batches to process
	txCount := len(block.Transactions)
	batchCount := (txCount + batchSize - 1) / batchSize
	blockHeight := int64(block.Height)
	// Process in batches
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		start := batchIndex * batchSize
		end := start + batchSize
		if end > txCount {
			end = txCount
		}

		// Create temporary maps for current batch
		//addressIncomeMap := make(map[string][]string)
		//txMap := make(map[string][]string)
		currBatchSize := end - start
		addressIncomeMap := make(map[string][]string, currBatchSize*3) // Assume each transaction has an average of 2 outputs
		txMap := make(map[string][]string, currBatchSize)

		// Only process transactions in current batch
		var mempoolIncomeKeys []string
		for i := start; i < end; i++ {
			tx := block.Transactions[i]
			for x, out := range tx.Outputs {
				//fmt.Println(tx.ID, out.Address, out.Amount)
				if out.Address == "" {
					out.Address = "errAddress"
				}
				if out.Amount == "" {
					out.Amount = "0"
				}
				txMap[tx.ID] = append(txMap[tx.ID], common.ConcatBytesOptimized([]string{out.Address, out.Amount}, "@"))
				// Use pre-allocated slices to reduce memory reallocation
				if _, exists := addressIncomeMap[out.Address]; !exists {
					// Pre-allocate a slice with reasonable capacity
					addressIncomeMap[out.Address] = make([]string, 0, 4) // Assume most addresses have less than 4 outputs
				}
				if out.Amount != "errAddress" {
					addressIncomeMap[out.Address] = append(addressIncomeMap[out.Address], common.ConcatBytesOptimized([]string{tx.ID, strconv.Itoa(x), out.Amount}, "@"))
				}
				// Whether to clean up mempool income records
				if blockHeight > CleanedHeight {
					// If it's a partial batch of a large block, record mempool income
					txPoint := common.ConcatBytesOptimized([]string{tx.ID, strconv.Itoa(x)}, ":")
					// if out.Address == "19egopKjkPDphD9THoj6qbqG13Pf5DcCnj" {
					// 	log.Printf("Found special address %s in block %d, txpoint %s", out.Address, blockHeight, txPoint)
					// }
					mempoolIncomeKeys = append(mempoolIncomeKeys, common.ConcatBytesOptimized([]string{out.Address, txPoint}, "_"))
				}
			}
		}

		// Process current batch
		//workers := 1
		if err := i.utxoStore.BulkMergeMapConcurrent(&txMap, workers); err != nil {
			return err
		}

		if err := i.addressStore.BulkMergeMapConcurrent(&addressIncomeMap, workers); err != nil {
			return err
		}
		if len(mempoolIncomeKeys) > 0 && i.mempoolManager != nil {
			log.Printf("Deleting %d mempool income records for block height %d,first key:%s", len(mempoolIncomeKeys), blockHeight, mempoolIncomeKeys[0])
			err := i.mempoolManager.BatchDeleteIncom(mempoolIncomeKeys)
			if err != nil {
				log.Printf("Failed to delete mempool income records: %v", err)
			}
			mempoolIncomeKeys = nil // Clean up memory
		}

		// Immediately clean up memory for current batch
		for k := range txMap {
			delete(txMap, k)
		}
		for k := range addressIncomeMap {
			delete(addressIncomeMap, k)
		}
		txMap = nil
		addressIncomeMap = nil

		// Optional: Force garbage collection
		// runtime.GC()
	}

	return nil
}

func (i *UTXOIndexer) processSpend(block *Block) error {
	workers = config.GlobalConfig.Workers
	batchSize = config.GlobalConfig.BatchSize
	blockHeight := int64(block.Height)
	// Collect all transaction points
	var allTxPoints []string
	for _, tx := range block.Transactions {
		for _, in := range tx.Inputs {
			allTxPoints = append(allTxPoints, in.TxPoint)
		}
	}

	// Calculate batches
	totalPoints := len(allTxPoints)
	batchCount := (totalPoints + batchSize - 1) / batchSize

	// Process in batches
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		start := batchIndex * batchSize
		end := start + batchSize
		if end > totalPoints {
			end = totalPoints
		}

		// Transaction points for current batch
		batchPoints := allTxPoints[start:end]

		// Query current batch
		addressResult, err := i.utxoStore.QueryUTXOAddresses(&batchPoints, workers)
		if err != nil {
			return err
		}

		// Process results for current batch
		//workers := 1
		if err := i.spendStore.BulkMergeMapConcurrent(&addressResult, workers); err != nil {
			return err
		}
		// Whether to clean up mempool spend records
		if blockHeight > CleanedHeight {
			log.Printf("Deleting %d mempool spend records for block height %d,first key:%s", len(batchPoints), blockHeight, batchPoints[0])
			err := i.mempoolManager.BatchDeleteSpend(batchPoints)
			if err != nil {
				log.Printf("Failed to delete mempool spend records: %v", err)
			}
		}
		// Clean up current batch
		for k := range addressResult {
			delete(addressResult, k)
		}
		addressResult = nil

		// This batch is complete, release memory
		batchPoints = nil
	}

	// Clean up all transaction points
	allTxPoints = nil

	return nil
}

func (i *UTXOIndexer) GetLastIndexedHeight() (int, error) {
	// Get last indexed height from meta store
	heightBytes, err := i.metaStore.Get([]byte("last_indexed_height"))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			log.Println("No previous height found, starting from genesis")
			return 0, nil
		}
		log.Printf("Error reading last height: %v", err)
		return 0, err
	}

	height, err := strconv.Atoi(string(heightBytes))
	if err != nil {
		log.Printf("Invalid height format: %s, error: %v", heightBytes, err)
		return 0, fmt.Errorf("invalid height format: %w", err)
	}

	//log.Printf("Successfully read last indexed height: %d", height)
	return height, nil
}

type Block struct {
	Height         int                  `json:"height"`
	Transactions   []*Transaction       `json:"transactions"`
	AddressIncome  map[string][]*Income `json:"address_income"`
	IsPartialBlock bool                 `json:"-"` // Mark whether it is a partial block for batch processing
}

func (b *Block) Validate() error {
	if b.Height < 0 {
		return fmt.Errorf("invalid block height: %d", b.Height)
	}
	return nil
}

type Transaction struct {
	ID      string
	Inputs  []*Input
	Outputs []*Output
}
type Income struct {
	TxID  string
	Index string
	Value string
}
type Input struct {
	TxPoint string
}

type Output struct {
	Address string
	Amount  string
}

// SetMempoolManager sets the mempool manager
func (i *UTXOIndexer) SetMempoolManager(mgr MempoolManager) {
	i.mempoolManager = mgr
}

// GetUtxoStore returns the UTXO storage object
func (i *UTXOIndexer) GetUtxoStore() *storage.PebbleStore {
	return i.utxoStore
}
