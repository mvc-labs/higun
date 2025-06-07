package blockindexer

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/bytedance/sonic"
	"github.com/cockroachdb/pebble/v2"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/config"
)

var (
	blockInfoDB *pebble.DB
	blockTxDB   *pebble.DB
	dbOnce      sync.Once
	noopLogger  = &customLogger{}
	client      *blockchain.Client
)

// 自定义日志记录器 - 不输出任何内容
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
	MinerAddress  *string `json:"minerAddress"` // 允许为null
	TxCount       int     `json:"txCount"`
	InputCount    int     `json:"inputCount"`
	OutputCount   int     `json:"outputCount"`
	Size          int     `json:"size"`
	Bits          int     `json:"bits"`
	Nonce         uint32  `json:"nonce"`
	Coinbase      *string `json:"coinbase"` // 允许为null
}
type BlockData struct {
	BaseInfo map[string]interface{} `json:"baseInfo"`
	TotalFee float64                `json:"totalFee"`
	Miner    string                 `json:"miner"`
	Reward   int64                  `json:"reward"`
	Size     int                    `json:"size"`
}

// BlockIndexerInit 初始化 blockInfo 和 blockTx 两个 Pebble 数据库（全局单例）
func IndexerInit(dataDir string, cfg *config.Config) error {
	var err error
	client, err = blockchain.NewClient(cfg)

	dbOptions := &pebble.Options{
		Logger: noopLogger,
		Levels: []pebble.LevelOptions{
			{
				Compression: func() pebble.Compression { return pebble.NoCompression },
			},
		},
		//MemTableSize:                32 << 20, // 降低为32MB (默认64MB)
		//MemTableStopWritesThreshold: 2,        // 默认4
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

// DoBlockInfoIndex 每隔10秒获取最新区块高度，和本地txt比较，有新区块则打印并更新txt
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

// 同步新区块并逐个更新txt
func SyncNewBlocks(fromHeight, toHeight int64, fileName string) {
	total := toHeight - fromHeight
	if total <= 0 {
		return
	}
	barLen := 40 // 进度条长度
	for h := fromHeight + 1; h <= toHeight; h++ {
		blockHash, err := client.GetBlockHash(h)
		if err != nil {
			fmt.Printf("\nGetBlockHash(%d) error: %v\n", h, err)
			break
		}
		block, err := client.GetBlock(blockHash)
		if err != nil {
			fmt.Printf("\nGetBlock(%d) error: %v\n", block.Height, err)
			break
		}
		// 进度条
		progress := float64(h-fromHeight) / float64(total)
		done := int(progress * float64(barLen))
		fmt.Printf("\r同步区块 [%s%s] %d/%d (%.2f%%)",
			strings.Repeat("=", done),
			strings.Repeat(" ", barLen-done),
			h, toHeight, progress*100)
		// 这里可以处理 block 的其它业务逻辑
		err = SaveBlockInfo(h, blockHash)
		if err != nil {
			fmt.Printf("\nSaveBlockInfo(%d) error: %v\n", h, err)
			break
		}
		_ = os.WriteFile(fileName, []byte(strconv.FormatInt(h, 10)), 0644)
	}
	fmt.Println("\n同步完成")
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
	block, _ := client.GetBlock(blockHash)
	findCoinBase := false
	txIdList := make([]string, 0, len(block.Tx))
	for _, tx := range block.Tx {
		txIdList = append(txIdList, tx.Txid)
		if len(tx.Vin) == 0 {
			break
		}
		if !findCoinBase {
			for _, in := range tx.Vin {
				if in.IsCoinBase() {
					tx2, err := client.GetRawTransaction(tx.Txid)
					if err == nil {
						infoData.Miner, infoData.Reward = GetMinerAndReward(tx2)
					}
					findCoinBase = true
					break
				}
			}
		}
		for _, out := range tx.Vout {
			totalFee += out.Value
		}
	}
	infoData.TotalFee = totalFee
	infoData.Size = int(block.Size)
	key := fmt.Sprintf("%08d", blockHeight)
	blockValue, err := sonic.Marshal(txIdList)
	if err != nil {
		return
	}
	infoValue, err := sonic.Marshal(infoData)
	if err != nil {
		return
	}
	// 保存区块交易列表到 blockTxDB
	if err = blockTxDB.Set([]byte(key), blockValue, nil); err != nil {
		return fmt.Errorf("failed to save block transactions: %w", err)
	}
	// 保存区块信息到 blockInfoDB
	if err = blockInfoDB.Set([]byte(key), infoValue, nil); err != nil {
		return fmt.Errorf("failed to save block info: %w", err)
	}
	return
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
		bitsInt = 0 // 或根据需要处理错误
	}
	result := &BlockInfo{
		BlockHash:     info.BaseInfo["hash"].(string),
		Height:        int(info.BaseInfo["height"].(float64)),
		Version:       int(info.BaseInfo["version"].(float64)),
		PrevBlockHash: info.BaseInfo["previousblockhash"].(string),
		MerkleRoot:    info.BaseInfo["merkleroot"].(string),
		Timestamp:     int64(info.BaseInfo["time"].(float64)),
		MedianTime:    int64(info.BaseInfo["mediantime"].(float64)),
		Reward:        info.Reward,
		Miner:         info.Miner,
		MinerAddress:  nil,
		TxCount:       int(info.BaseInfo["num_tx"].(float64)),
		InputCount:    0, // 需要从交易中计算
		OutputCount:   0, // 需要从交易中计算
		Size:          info.Size,
		Bits:          int(bitsInt),
		Nonce:         uint32(info.BaseInfo["nonce"].(float64)),
		Coinbase:      nil,
	}
	return result, nil
}

func GetBlockInfoList(lastHeight int64, limit int) ([]*BlockInfo, error) {
	if limit <= 0 {
		limit = 10 // 默认返回10个区块信息
	}
	startHeight := lastHeight - int64(limit) + 1
	if startHeight < 0 {
		startHeight = 0
	}
	blockInfos := make([]*BlockInfo, 0, limit)
	for h := startHeight; h < lastHeight; h++ {
		info, err := GetBlockInfo(h)
		if err != nil {
			continue
		}
		blockInfos = append(blockInfos, info)
	}
	return blockInfos, nil
}
