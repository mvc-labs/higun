package blockindexer

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble"
	lru "github.com/hashicorp/golang-lru"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
)

var (
	blockInfoDB   *pebble.DB
	blockTxDB     *pebble.DB
	dbOnce        sync.Once
	noopLogger    = &customLogger{}
	client        *blockchain.Client
	syncingBlocks int32 // 0: idle, 1: syncing
	ChainStats    *blockchain.ChainStatus
	txCache       *lru.Cache
)

// Custom logger - outputs nothing
type customLogger struct{}

func (l *customLogger) Infof(format string, args ...interface{})  {}
func (l *customLogger) Fatalf(format string, args ...interface{}) {}
func (l *customLogger) Errorf(format string, args ...interface{}) {}

type BlockInfo struct {
	BlockHash     string  `json:"blockHash"`
	Height        int     `json:"height"`
	Version       int     `json:"version"`
	PrevBlockHash string  `json:"prevBlockHash"`
	MerkleRoot    string  `json:"merkleRoot"`
	Timestamp     int64   `json:"timestamp"`
	MedianTime    int64   `json:"medianTime"`
	Reward        int64   `json:"reward"`
	Miner         string  `json:"miner"`
	MinerAddress  *string `json:"minerAddress"` // Can be null
	TxCount       int     `json:"txCount"`
	InputCount    int     `json:"inputCount"`
	OutputCount   int     `json:"outputCount"`
	Size          int     `json:"size"`
	Bits          int     `json:"bits"`
	Nonce         uint32  `json:"nonce"`
	Coinbase      *string `json:"coinbase"` // Can be null
}
type BlockData struct {
	BaseInfo map[string]interface{} `json:"baseInfo"`
	TotalFee float64                `json:"totalFee"`
	Miner    string                 `json:"miner"`
	Reward   int64                  `json:"reward"`
	Size     int                    `json:"size"`
}

// BlockIndexerInit initializes blockInfo and blockTx two Pebble databases (global singleton)
func IndexerInit(dataDir string, cfg *config.Config) error {
	var err error
	txCache, _ = lru.New(20000) // Cache 20,000 transaction details
	client, err = blockchain.NewClient(cfg)

	dbOptions := &pebble.Options{
		Logger: noopLogger,
		Levels: []pebble.LevelOptions{
			{
				Compression: pebble.NoCompression,
			},
		},
		//MemTableSize:                32 << 20, // Reduce to 32MB (default 64MB)
		//MemTableStopWritesThreshold: 2,        // Default 4
	}
	dbOnce.Do(func() {
		blockInfoPath := filepath.Join(dataDir, "blockInfo")
		blockTxPath := filepath.Join(dataDir, "blockTx")
		blockInfoDB, err = pebble.Open(blockInfoPath, dbOptions)
		if err != nil {
			return
		}
		blockTxDB, err = pebble.Open(blockTxPath, dbOptions)
	})
	return err
}
func SaveBlockInfoData() {
	for {
		stats, err := client.GetChainStatus()
		if err == nil && stats != nil {
			ChainStats = stats
		}
		time.Sleep(1 * time.Minute)
	}
}

// DoBlockInfoIndex gets the latest block height every 10 seconds, compares with local txt, prints and updates txt if there are new blocks
func DoBlockInfoIndex() {
	const fileName = "latest_block.txt"
	for {
		height, err := client.GetBlockCount()
		if err != nil {
			fmt.Printf("GetBlockCount error: %v\n", err)
			time.Sleep(10 * time.Second)
			continue
		}

		localHeight := int64(0)
		data, err := os.ReadFile(fileName)
		if err != nil {
			if os.IsNotExist(err) {
				_ = os.WriteFile(fileName, []byte("0"), 0644)
			} else {
				fmt.Printf("ReadFile error: %v\n", err)
			}
		} else {
			s := strings.TrimSpace(string(data))
			if s != "" {
				if v, err := strconv.ParseInt(s, 10, 64); err == nil {
					localHeight = v
				}
			}
		}

		if int64(height) > localHeight {
			SyncNewBlocks(localHeight, int64(height), fileName)
		}

		time.Sleep(10 * time.Second)
	}
}

// Sync new blocks and update txt one by one
func SyncNewBlocks(fromHeight, toHeight int64, fileName string) {
	total := toHeight - fromHeight
	if total <= 0 {
		return
	}
	barLen := 40 // Progress bar length
	for h := fromHeight + 1; h <= toHeight; h++ {
		blockHash, err := client.GetBlockHash(h)
		if err != nil {
			fmt.Printf("\nGetBlockHash(%d) error: %v\n", h, err)
			break
		}
		// block, err := client.GetBlock(blockHash)
		// if err != nil {
		// 	fmt.Printf("\nGetBlock(%d) error: %v\n", block.Height, err)
		// 	break
		// }
		// Progress bar
		progress := float64(h-fromHeight) / float64(total)
		done := int(progress * float64(barLen))
		fmt.Printf("\rSyncing blocks [%s%s] %d/%d (%.2f%%)",
			strings.Repeat("=", done),
			strings.Repeat(" ", barLen-done),
			h, toHeight, progress*100)
		// Here you can handle other business logic for the block
		err = SaveBlockInfo(h, blockHash)
		if err != nil {
			fmt.Printf("\nSaveBlockInfo(%d) error: %v\n", h, err)
			break
		}
		_ = os.WriteFile(fileName, []byte(strconv.FormatInt(h, 10)), 0644)
	}
	fmt.Println("\nSync completed")
}

func SaveBlockInfo(blockHeight int64, blockHash *chainhash.Hash) (err error) {
	var result map[string]interface{}
	result, err = client.GetBlockHeaderWithTimeout(blockHash.String(), 5*time.Second)
	if err != nil {
		return
	}
	var infoData BlockData
	infoData.BaseInfo = result
	var totalFee float64
	block, err := client.GetBlockOnlyTxId(blockHash)
	if err != nil {
		return fmt.Errorf("failed to get block transactions: %w", err)
	}
	if len(block.Tx) == 0 {
		// Block has no transactions
		return fmt.Errorf("block %d has no transactions", blockHeight)
	}
	// blockLen := len(block.Tx)
	// if blockLen > 100000 {
	// 	blockLen = 100000 // Limit maximum transaction count to avoid being too large
	// }
	// txIdList := block.Tx[0:blockLen]
	coinbaseTxid := block.Tx[0] // First transaction txid
	coinbaseTx, err := client.GetRawTransaction(coinbaseTxid)

	if err == nil && coinbaseTx != nil {
		infoData.Miner, infoData.Reward = GetMinerAndReward(coinbaseTx)
	}
	infoData.TotalFee = totalFee
	infoData.Size = int(block.Size)
	key := fmt.Sprintf("%08d", blockHeight)
	go SaveBlockTxWithFee(blockHeight, key, block.Tx)
	// blockValue, err := sonic.Marshal(txIdList)
	// if err != nil {
	// 	return
	// }
	infoValue, err := sonic.Marshal(infoData)
	if err != nil {
		return
	}
	// // Save block transaction list to blockTxDB
	// if err = blockTxDB.Set([]byte(key), blockValue, nil); err != nil {
	// 	return fmt.Errorf("failed to save block transactions: %w", err)
	// }
	// Save block info to blockInfoDB
	if err = blockInfoDB.Set([]byte(key), infoValue, nil); err != nil {
		return fmt.Errorf("failed to save block info: %w", err)
	}
	return
}
func SaveBlockTxWithFee(blockHeight int64, key string, txIdList []string) (err error) {
	if blockHeight <= 126000 {
		blockValue, err1 := sonic.Marshal(txIdList)
		if err1 != nil {
			return
		}
		// Save block transaction list to blockTxDB
		if err1 = blockTxDB.Set([]byte(key), blockValue, nil); err1 != nil {
			return fmt.Errorf("failed to save block transactions: %w", err)
		}
		return nil
	}
	txValueList, err := CountBlockTxFee(blockHeight, txIdList, 2, 1000)
	if err != nil {
		return
	}
	blockValue, err := sonic.Marshal(txValueList)
	if err != nil {
		return
	}
	// Save block transaction list to blockTxDB
	if err = blockTxDB.Set([]byte(key), blockValue, nil); err != nil {
		return fmt.Errorf("failed to save block transactions: %w", err)
	}
	return nil
}

// CountBlockTxFee calculates fees for each transaction, writes results to txValueList (format "txId:fee")
func CountBlockTxFee(blockHeight int64, txids []string, concurrency, batchSize int) ([]string, error) {
	if len(txids) <= 1 {
		return nil, nil // Only coinbase, no fees
	}
	txValueList := make([]string, len(txids))
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	// Skip coinbase, coinbase transaction fee is 0
	txValueList[0] = fmt.Sprintf("%s:0", txids[0])

	for i := 1; i < len(txids); i += batchSize {
		end := i + batchSize
		if end > len(txids) {
			end = len(txids)
		}
		batch := txids[i:end]
		for j, txid := range batch {
			wg.Add(1)
			sem <- struct{}{}
			go func(idx int, txid string) {
				defer wg.Done()
				defer func() { <-sem }()
				tx, err := GetTxWithCache(txid, txCache)
				if err != nil || tx == nil {
					txValueList[idx] = fmt.Sprintf("%s:0", txid)
					return
				}
				// Calculate input total
				var inputSum int64
				for _, vin := range tx.MsgTx().TxIn {
					// Check if it's a coinbase input
					if len(vin.SignatureScript) > 0 && vin.PreviousOutPoint.Index == 0xffffffff && vin.PreviousOutPoint.Hash == (chainhash.Hash{}) {
						continue // Skip coinbase input
					}
					prevTx, err := GetTxWithCache(vin.PreviousOutPoint.Hash.String(), txCache)
					if err != nil || prevTx == nil {
						continue
					}
					if int(vin.PreviousOutPoint.Index) < len(prevTx.MsgTx().TxOut) {
						inputSum += prevTx.MsgTx().TxOut[vin.PreviousOutPoint.Index].Value
					}
				}
				// Calculate output total
				var outputSum int64
				for _, vout := range tx.MsgTx().TxOut {
					outputSum += vout.Value
				}
				fee := inputSum - outputSum
				feeRate := fee / int64(tx.MsgTx().SerializeSize())
				//txValueList[idx] = fmt.Sprintf("%s:%.8f", txid, fee)
				txValueList[idx] = common.ConcatBytesOptimized([]string{txid, strconv.FormatInt(fee, 10), strconv.FormatInt(feeRate, 10)}, ":")
			}(i+j, txid)
		}
		log.Println("Processing block(", blockHeight, ") transactionsFee, current batch start index:", i, "end index:", end, "total transactions:", len(txids))
	}
	wg.Wait()
	return txValueList, nil
}

// GetTxWithCache queries transaction details, prioritizes LRU cache
func GetTxWithCache(txid string, cache *lru.Cache) (*btcutil.Tx, error) {
	if v, ok := cache.Get(txid); ok {
		return v.(*btcutil.Tx), nil
	}
	tx, err := client.GetRawTransaction(txid)
	if err != nil {
		return nil, err
	}
	cache.Add(txid, tx)
	return tx, nil
}
func GetMaxBlockHeight() (int64, error) {
	iter, err := blockInfoDB.NewIter(nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()
	if iter.Last() {
		key := string(iter.Key())
		height, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse height failed: %w", err)
		}
		return height, nil
	}
	return 0, fmt.Errorf("db is empty")
}
func GetBlockInfo(blockHeight int64) (*BlockInfo, error) {
	key := fmt.Sprintf("%08d", blockHeight)
	value, closer, err := blockInfoDB.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("failed to get block info: %w", err)
	}
	defer closer.Close()

	var info BlockData
	if err = sonic.Unmarshal(value, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block info: %w", err)
	}
	bitsStr := info.BaseInfo["bits"].(string)
	bitsInt, err := strconv.ParseInt(bitsStr, 16, 64)
	if err != nil {
		bitsInt = 0 // Or handle error according to your needs
	}
	result := &BlockInfo{
		BlockHash:     info.BaseInfo["hash"].(string),
		Height:        int(info.BaseInfo["height"].(float64)),
		Version:       int(info.BaseInfo["version"].(float64)),
		PrevBlockHash: info.BaseInfo["previousblockhash"].(string),
		MerkleRoot:    info.BaseInfo["merkleroot"].(string),
		Timestamp:     int64(info.BaseInfo["time"].(float64)) * 1000, // Convert to milliseconds
		MedianTime:    int64(info.BaseInfo["mediantime"].(float64)) * 1000,
		Reward:        info.Reward,
		Miner:         info.Miner,
		MinerAddress:  nil,
		TxCount:       int(info.BaseInfo["num_tx"].(float64)),
		InputCount:    0, // Calculate from transactions
		OutputCount:   0, // Calculate from transactions
		Size:          info.Size,
		Bits:          int(bitsInt),
		Nonce:         uint32(info.BaseInfo["nonce"].(float64)),
		Coinbase:      nil,
	}
	return result, nil
}

func GetBlockInfoList(lastHeight int64, limit int) ([]*BlockInfo, error) {
	if limit <= 0 {
		limit = 10 // Default to return 10 block information
	}
	if lastHeight <= 0 {
		lastHeight, _ = GetMaxBlockHeight()
	}
	if lastHeight <= 0 {
		return nil, fmt.Errorf("no blocks indexed yet")
	}
	startHeight := lastHeight - int64(limit) + 1
	if startHeight < 0 {
		startHeight = 0
	}
	blockInfos := make([]*BlockInfo, 0, limit)
	for h := lastHeight; h >= startHeight; h-- {
		info, err := GetBlockInfo(h)
		if err != nil {
			continue
		}
		blockInfos = append(blockInfos, info)
	}
	return blockInfos, nil
}
func GetBlockTxList(blockHeight int64, cursor int, size int) ([]string, int64, error) {
	key := fmt.Sprintf("%08d", blockHeight)
	value, closer, err := blockTxDB.Get([]byte(key))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get block transactions: %w", err)
	}
	defer closer.Close()

	var txList []string
	if err = sonic.Unmarshal(value, &txList); err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal block transactions: %w", err)
	}
	total := len(txList)
	if cursor < 0 || cursor >= total {
		return nil, 0, fmt.Errorf("invalid cursor: %d", cursor)
	}
	if size <= 0 {
		size = 10 // Default to return 10 transactions
	}
	if cursor+size > total {
		size = total - cursor // Adjust size to avoid out of bounds
	}
	txList = txList[cursor : cursor+size]
	info, err := GetBlockInfo(blockHeight) // Ensure block info is loaded
	if err == nil && info != nil {
		total = info.TxCount
	}
	return txList, int64(total), nil
}
func GetBlockAllTxList(blockHeight int64) ([]string, error) {
	key := fmt.Sprintf("%08d", blockHeight)
	value, closer, err := blockTxDB.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("failed to get block transactions: %w", err)
	}
	defer closer.Close()

	var txList []string
	if err = sonic.Unmarshal(value, &txList); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block transactions: %w", err)
	}

	return txList, nil
}
