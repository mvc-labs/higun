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
	mempoolManager MempoolManager // 使用接口类型替代interface{}
}

var workers = 1

var batchSize = 1000
var CleanedHeight int64 // 用于记录清理高度
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
// 	// 根据数据大小和系统资源动态调整并发数
// 	// 小数据量用较少协程，大数据量时限制最大并发数
// 	optimalWorkers := i.workers

// 	if dataSize < 1000 {
// 		// 小数据量使用更少的协程
// 		optimalWorkers = min(i.workers, 2)
// 	} else if dataSize > 10000 {
// 		// 大数据量限制最大协程数
// 		maxWorkers := runtime.NumCPU() * 2 // 或设置一个固定上限
// 		optimalWorkers = min(i.workers, maxWorkers)
// 	}

//		return optimalWorkers
//	}
func (i *UTXOIndexer) InitProgressBar(totalBlocks, startHeight int) {
	remainingBlocks := totalBlocks - startHeight
	if remainingBlocks <= 0 {
		remainingBlocks = 1 // 至少设置为1以避免错误
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
		progressbar.OptionSetRenderBlankState(false), // 不显示空白状态
		progressbar.OptionShowCount(),                // 显示计数 (e.g., 1/3)
		progressbar.OptionShowIts(),                  // 显示迭代次数
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(colorable.NewColorableStdout(), "\nDone!\n")
		}),
		//progressbar.OptionSetFormat("%s %s %d/%d (%.2f ops/s)"), // 自定义格式：进度条 + 进度 + 速度
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

	// 设置全局工作线程数和批处理大小
	workers = config.GlobalConfig.Workers
	batchSize = config.GlobalConfig.BatchSize

	// Validate block
	if err := block.Validate(); err != nil {
		return fmt.Errorf("invalid block: %w", err)
	}

	// 由于在convertBlock阶段已分批处理，此处不再需要复杂的大区块处理逻辑
	// 直接处理当前批次的交易

	// Phase 1: Index all outputs
	if err := i.indexIncome(block); err != nil {
		return fmt.Errorf("failed to index outputs: %w", err)
	}
	// 第1阶段完成后即可释放部分内存
	block.AddressIncome = nil

	// Phase 2: Process all inputs
	if err := i.processSpend(block); err != nil {
		return fmt.Errorf("failed to process inputs: %w", err)
	}
	// 第2阶段完成后释放交易数据
	block.Transactions = nil

	// 如果是大区块的部分批次，不更新索引高度，等待最后一个批次
	if !block.IsPartialBlock && updateHeight {
		// 只有在处理完整个区块的最后一批次时才保存高度
		heightStr := strconv.Itoa(block.Height)
		if err := i.metaStore.Set([]byte("last_indexed_height"), []byte(heightStr)); err != nil {
			return err
		}

		if err := i.metaStore.Sync(); err != nil {
			log.Printf("Failed to sync meta store: %v", err)
			return err
		}

		// 更新进度条
		if i.bar != nil {
			i.bar.Add(1)
		}
	}

	// 最后释放区块对象
	block = nil
	runtime.GC()
	return nil
}

func (i *UTXOIndexer) indexIncome(block *Block) error {
	// 设置合理的批处理大小，根据内存情况调整
	//const batchSize = 1000
	workers = config.GlobalConfig.Workers
	batchSize = config.GlobalConfig.BatchSize
	// 计算需要处理的批次数
	txCount := len(block.Transactions)
	batchCount := (txCount + batchSize - 1) / batchSize
	blockHeight := int64(block.Height)
	// 分批处理
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		start := batchIndex * batchSize
		end := start + batchSize
		if end > txCount {
			end = txCount
		}

		// 为当前批次创建临时map
		//addressIncomeMap := make(map[string][]string)
		//txMap := make(map[string][]string)
		currBatchSize := end - start
		addressIncomeMap := make(map[string][]string, currBatchSize*3) // 假设每个交易平均有2个输出
		txMap := make(map[string][]string, currBatchSize)

		// 只处理当前批次的交易
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
				// 使用预分配的切片减少内存重分配
				if _, exists := addressIncomeMap[out.Address]; !exists {
					// 预分配一个合理容量的切片
					addressIncomeMap[out.Address] = make([]string, 0, 4) // 假设大多数地址有4个以下的输出
				}
				if out.Amount != "errAddress" {
					addressIncomeMap[out.Address] = append(addressIncomeMap[out.Address], common.ConcatBytesOptimized([]string{tx.ID, strconv.Itoa(x), out.Amount}, "@"))
				}
				//是否需要清理内存的收入记录
				if blockHeight > CleanedHeight {
					// 如果是大区块的部分批次，记录内存池的收入
					mempoolIncomeKeys = append(mempoolIncomeKeys, common.ConcatBytesOptimized([]string{out.Address, tx.ID}, "_"))
				}
			}
		}

		// 处理当前批次
		//workers := 1
		if err := i.utxoStore.BulkMergeMapConcurrent(&txMap, workers); err != nil {
			return err
		}

		if err := i.addressStore.BulkMergeMapConcurrent(&addressIncomeMap, workers); err != nil {
			return err
		}
		if len(mempoolIncomeKeys) > 0 && i.mempoolManager != nil {
			log.Printf("Deleting %d mempool income records for block height %d", len(mempoolIncomeKeys), blockHeight)
			err := i.mempoolManager.BatchDeleteIncom(mempoolIncomeKeys)
			if err != nil {
				log.Printf("Failed to delete mempool income records: %v", err)
			}
			mempoolIncomeKeys = nil // 清理内存
		}

		// 立即清理当前批次的内存
		for k := range txMap {
			delete(txMap, k)
		}
		for k := range addressIncomeMap {
			delete(addressIncomeMap, k)
		}
		txMap = nil
		addressIncomeMap = nil

		// 可选：强制垃圾回收
		// runtime.GC()
	}

	return nil
}

func (i *UTXOIndexer) processSpend(block *Block) error {
	workers = config.GlobalConfig.Workers
	batchSize = config.GlobalConfig.BatchSize
	blockHeight := int64(block.Height)
	// 收集所有交易点
	var allTxPoints []string
	for _, tx := range block.Transactions {
		for _, in := range tx.Inputs {
			allTxPoints = append(allTxPoints, in.TxPoint)
		}
	}

	// 计算批次
	totalPoints := len(allTxPoints)
	batchCount := (totalPoints + batchSize - 1) / batchSize

	// 分批处理
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		start := batchIndex * batchSize
		end := start + batchSize
		if end > totalPoints {
			end = totalPoints
		}

		// 当前批次的交易点
		batchPoints := allTxPoints[start:end]

		// 查询当前批次
		addressResult, err := i.utxoStore.QueryUTXOAddresses(&batchPoints, workers)
		if err != nil {
			return err
		}

		// 处理当前批次的结果
		//workers := 1
		if err := i.spendStore.BulkMergeMapConcurrent(&addressResult, workers); err != nil {
			return err
		}
		//是否需要清理内存的支出记录
		if blockHeight > CleanedHeight {
			log.Printf("Deleting %d mempool spend records for block height %d", len(batchPoints), blockHeight)
			err := i.mempoolManager.BatchDeleteSpend(batchPoints)
			if err != nil {
				log.Printf("Failed to delete mempool spend records: %v", err)
			}
		}
		// 清理当前批次
		for k := range addressResult {
			delete(addressResult, k)
		}
		addressResult = nil

		// 此批次处理完毕，释放内存
		batchPoints = nil
	}

	// 清理全部交易点
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
	IsPartialBlock bool                 `json:"-"` // 标记是否为分批处理的部分区块
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

// SetMempoolManager 设置内存池管理器
func (i *UTXOIndexer) SetMempoolManager(mgr MempoolManager) {
	i.mempoolManager = mgr
}

// GetUtxoStore 返回UTXO存储对象
func (i *UTXOIndexer) GetUtxoStore() *storage.PebbleStore {
	return i.utxoStore
}
