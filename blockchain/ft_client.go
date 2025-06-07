package blockchain

import (
	"encoding/hex"
	"fmt"

	indexer "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"

	"math"
	"runtime"
	"strconv"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/contract/meta-contract/decoder"
)

type FtClient struct {
	rpcClient *rpcclient.Client
	cfg       *config.Config
	params    *chaincfg.Params
}

func NewFtClient(cfg *config.Config) (*FtClient, error) {
	connCfg := &rpcclient.ConnConfig{
		Host:         fmt.Sprintf("%s:%s", cfg.RPC.Host, cfg.RPC.Port),
		User:         cfg.RPC.User,
		Pass:         cfg.RPC.Password,
		HTTPPostMode: true,
		DisableTLS:   true,
	}

	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC client: %w", err)
	}

	params, err := cfg.GetChainParams()
	if err != nil {
		return nil, fmt.Errorf("failed to get chain params: %w", err)
	}

	return &FtClient{
		rpcClient: client,
		cfg:       cfg,
		params:    params,
	}, nil
}

func (c *FtClient) GetBlock(hash *chainhash.Hash) (*btcjson.GetBlockVerboseTxResult, error) {
	return c.rpcClient.GetBlockVerboseTx(hash)
}

func (c *FtClient) GetBlockHash(height int64) (*chainhash.Hash, error) {
	hash, err := c.rpcClient.GetBlockHash(height)
	if err != nil {
		return nil, fmt.Errorf("failed to get block hash at height %d: %w", height, err)
	}
	return hash, nil
}

// GetRawMempool 获取内存池中的所有交易ID
func (c *FtClient) GetRawMempool() ([]string, error) {
	hashes, err := c.rpcClient.GetRawMempool()
	if err != nil {
		return nil, fmt.Errorf("获取内存池交易列表失败: %w", err)
	}

	// 将哈希转换为字符串
	txids := make([]string, len(hashes))
	for i, hash := range hashes {
		txids[i] = hash.String()
	}

	return txids, nil
}

func (c *FtClient) GetRawTransaction(txHashStr string) (*btcutil.Tx, error) {
	txHash, err := chainhash.NewHashFromStr(txHashStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction hash %s: %w", txHashStr, err)
	}
	tx, err := c.rpcClient.GetRawTransaction(txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction %s: %w", txHash, err)
	}
	return tx, nil
}

func (c *FtClient) GetBlockCount() (int, error) {
	count, err := c.rpcClient.GetBlockCount()
	if err != nil {
		return 0, fmt.Errorf("failed to get block count: %w", err)
	}
	return int(count), nil
}

func (c *FtClient) Shutdown() {
	c.rpcClient.Shutdown()
}

// SyncBlocks 持续同步区块的修改版
func (c *FtClient) SyncBlocks(idx *indexer.ContractFtIndexer, checkInterval time.Duration, stopCh <-chan struct{}, onFirstSyncDone func()) error {
	firstSyncComplete := false

	for {
		select {
		case <-stopCh:
			return nil
		default:
		}

		lastHeight, err := idx.GetLastIndexedHeight()
		if err != nil {
			return fmt.Errorf("获取上次索引高度失败: %w", err)
		}

		currentHeight, err := c.GetBlockCount()
		if err != nil {
			return fmt.Errorf("获取当前区块高度失败: %w", err)
		}

		if currentHeight <= lastHeight {
			if !firstSyncComplete && onFirstSyncDone != nil {
				fmt.Printf("当前已索引到最新区块，高度: %d，首次同步完成\n", lastHeight)
				firstSyncComplete = true
				onFirstSyncDone()
			}
			time.Sleep(checkInterval)
			continue
		}

		fmt.Printf("发现新区块，从高度 %d 索引到 %d\n", lastHeight+1, currentHeight)
		idx.InitProgressBar(currentHeight, lastHeight+1)

		for height := lastHeight + 1; height <= currentHeight; height++ {
			if err := c.ProcessBlock(idx, height, true); err != nil {
				return fmt.Errorf("处理区块失败，高度 %d: %w", height, err)
			}
		}

		fmt.Printf("成功索引到当前高度 %d\n", currentHeight)

		if !firstSyncComplete && onFirstSyncDone != nil {
			fmt.Printf("首次区块同步已完成，现在调用回调函数\n")
			firstSyncComplete = true
			onFirstSyncDone()
		}

		time.Sleep(checkInterval)
	}
}

// ProcessBlock 处理指定高度的区块
func (c *FtClient) ProcessBlock(idx *indexer.ContractFtIndexer, height int, updateHeight bool) error {
	t1 := time.Now()
	hash, err := c.GetBlockHash(int64(height))
	if err != nil {
		return fmt.Errorf("获取区块哈希失败，高度 %d: %w", height, err)
	}

	block, err := c.GetBlock(hash)
	if err != nil {
		return fmt.Errorf("获取区块失败，高度 %d: %w", height, err)
	}
	t2 := time.Now()

	txCount := len(block.Tx)
	maxTxPerBatch := c.GetMaxTxPerBatch()

	t3 := time.Now()
	if txCount > maxTxPerBatch {
		fmt.Printf("\n大区块处理: 高度=%d, 交易数=%d, 超过最大批次大小=%d, 从RPC获取区块数据耗时: %v 秒, 将使用Channel进行分批处理\n",
			height, txCount, maxTxPerBatch, t2.Sub(t1).Seconds())

		batchCh := make(chan *indexer.ContractFtBlock, 2)
		errCh := make(chan error, 1)
		doneCh := make(chan struct{})

		go func() {
			defer close(doneCh)
			for convertedBlock := range batchCh {
				// t5 := time.Now()
				if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
					errCh <- fmt.Errorf("索引高度 %d 的批次失败: %w", height, err)
					return
				}
				// t6 := time.Now()
				// fmt.Printf("索引区块 %d-%d 耗时: %v 秒\n", convertedBlock.Height, convertedBlock.Height, t6.Sub(t5).Seconds())
				convertedBlock.Transactions = nil
				convertedBlock.ContractFtOutputs = nil
				convertedBlock = nil
				runtime.GC()
			}
		}()

		startIdx := 0
		var lastBatch bool

		for startIdx < txCount {
			endIdx := startIdx + maxTxPerBatch
			if endIdx >= txCount {
				endIdx = txCount
				lastBatch = true
			} else {
				lastBatch = false
			}

			// t3 := time.Now()
			convertedBlock := c.convertBlockBatch(block, height, startIdx, endIdx, lastBatch)
			// t4 := time.Now()
			// fmt.Printf("转换区块	 %d-%d 耗时: %v 秒\n", startIdx+1, endIdx, t4.Sub(t3).Seconds())
			if convertedBlock == nil {
				close(batchCh)
				return fmt.Errorf("高度 %d 的批次 %d-%d 转换失败", height, startIdx+1, endIdx)
			}

			select {
			case batchCh <- convertedBlock:
			case err := <-errCh:
				close(batchCh)
				return err
			}

			startIdx = endIdx
		}

		close(batchCh)

		select {
		case err := <-errCh:
			return err
		case <-doneCh:
		}
	} else {
		convertedBlock := c.convertBlock(block, height)
		if convertedBlock == nil {
			return fmt.Errorf("区块转换失败，高度 %d", height)
		}

		if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
			return fmt.Errorf("索引区块失败，高度 %d: %w", height, err)
		}
	}

	t4 := time.Now()
	fmt.Printf("\n处理区块 %d 耗时: %v 秒\n", height, t4.Sub(t3).Seconds())
	return nil
}

// convertBlock 转换区块
func (c *FtClient) convertBlock(block *btcjson.GetBlockVerboseTxResult, height int) *indexer.ContractFtBlock {
	if height < 0 {
		return nil
	}

	maxTxPerBatch := c.GetMaxTxPerBatch()
	txCount := len(block.Tx)

	if txCount > maxTxPerBatch {
		fmt.Printf("\n大区块预处理: 高度=%d, 交易数=%d, 超过最大批次大小=%d, 将进行分批转换\n",
			height, txCount, maxTxPerBatch)

		contractFtOutputs := make(map[string][]*indexer.ContractFtOutput)
		txs := make([]*indexer.ContractFtTransaction, 0, txCount)

		for _, tx := range block.Tx {
			inputs := make([]*indexer.ContractFtInput, len(tx.Vin))
			for j, in := range tx.Vin {
				id := in.Txid
				if id == "" {
					id = "0000000000000000000000000000000000000000000000000000000000000000"
				}
				inputs[j] = &indexer.ContractFtInput{
					TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.Vout))}, ":"),
				}
			}

			outputs := make([]*indexer.ContractFtOutput, 0)
			for k, out := range tx.Vout {
				address := GetAddressFromScript(out.ScriptPubKey.Hex, nil, c.params, c.cfg.RPC.Chain)
				amount := strconv.FormatInt(int64(math.Round(out.Value*1e8)), 10)

				// 解析FT相关信息
				var output *indexer.ContractFtOutput
				ftInfo, uniqueUtxoInfo, contractTypeStr, err := ParseContractFtInfo(out.ScriptPubKey.Hex, c.params)
				if err != nil {
					fmt.Println("ParseFtInfo error", err)
					continue
				}
				if contractTypeStr == "ft" {
					if ftInfo == nil {
						continue
					}
					output = &indexer.ContractFtOutput{
						ContractType: contractTypeStr,
						Address:      address,
						Value:        amount,
						Index:        int64(k),
						Height:       int64(height),
						CodeHash:     ftInfo.CodeHash,
						Genesis:      ftInfo.Genesis,
						SensibleId:   ftInfo.SensibleId,
						Name:         ftInfo.Name,
						Symbol:       ftInfo.Symbol,
						Amount:       strconv.FormatUint(ftInfo.Amount, 10),
						Decimal:      ftInfo.Decimal,
						FtAddress:    ftInfo.Address,
					}
				} else if contractTypeStr == "unique" {
					if uniqueUtxoInfo == nil {
						continue
					}
					output = &indexer.ContractFtOutput{
						ContractType: contractTypeStr,
						Address:      address,
						Value:        amount,
						Index:        int64(k),
						Height:       int64(height),
						CodeHash:     uniqueUtxoInfo.CodeHash,
						Genesis:      uniqueUtxoInfo.Genesis,
						SensibleId:   uniqueUtxoInfo.SensibleId,
						CustomData:   uniqueUtxoInfo.CustomData,
					}
				} else {
					continue
				}
				if output == nil {
					continue
				}
				outputs = append(outputs, output)

				if find := contractFtOutputs[address]; find != nil {
					contractFtOutputs[address] = append(find, output)
				} else {
					contractFtOutputs[address] = []*indexer.ContractFtOutput{output}
				}
			}

			txs = append(txs, &indexer.ContractFtTransaction{
				ID:      tx.Txid,
				Inputs:  inputs,
				Outputs: outputs,
			})

			if len(txs) >= maxTxPerBatch {
				subBlock := &indexer.ContractFtBlock{
					Height:            height,
					Transactions:      txs,
					ContractFtOutputs: contractFtOutputs,
					IsPartialBlock:    true,
				}
				return subBlock
			}
		}

		if len(txs) > 0 {
			subBlock := &indexer.ContractFtBlock{
				Height:            height,
				Transactions:      txs,
				ContractFtOutputs: contractFtOutputs,
				IsPartialBlock:    false,
			}
			return subBlock
		}

		return nil
	}

	// 小区块处理
	contractFtOutputs := make(map[string][]*indexer.ContractFtOutput)
	txs := make([]*indexer.ContractFtTransaction, len(block.Tx))

	for i, tx := range block.Tx {
		inputs := make([]*indexer.ContractFtInput, len(tx.Vin))
		for j, in := range tx.Vin {
			id := in.Txid
			if id == "" {
				id = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			inputs[j] = &indexer.ContractFtInput{
				TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.Vout))}, ":"),
			}
		}

		outputs := make([]*indexer.ContractFtOutput, 0)
		for k, out := range tx.Vout {
			address := GetAddressFromScript(out.ScriptPubKey.Hex, nil, c.params, c.cfg.RPC.Chain)
			amount := strconv.FormatInt(int64(math.Round(out.Value*1e8)), 10)

			// 解析FT相关信息
			ftInfo, uniqueUtxoInfo, contractTypeStr, err := ParseContractFtInfo(out.ScriptPubKey.Hex, c.params)
			if err != nil {
				fmt.Println("ParseFtInfo error", err)
				return nil
			}
			var output *indexer.ContractFtOutput
			if contractTypeStr == "ft" {
				if ftInfo == nil {
					continue
				}
				output = &indexer.ContractFtOutput{
					ContractType: contractTypeStr,
					Address:      address,
					Value:        amount,
					Index:        int64(k),
					Height:       int64(height),
					CodeHash:     ftInfo.CodeHash,
					Genesis:      ftInfo.Genesis,
					SensibleId:   ftInfo.SensibleId,
					Name:         ftInfo.Name,
					Symbol:       ftInfo.Symbol,
					Amount:       strconv.FormatUint(ftInfo.Amount, 10),
					Decimal:      ftInfo.Decimal,
					FtAddress:    ftInfo.Address,
				}
			} else if contractTypeStr == "unique" {
				if uniqueUtxoInfo == nil {
					continue
				}
				output = &indexer.ContractFtOutput{
					ContractType: contractTypeStr,
					Address:      address,
					Value:        amount,
					Index:        int64(k),
					Height:       int64(height),
					CodeHash:     uniqueUtxoInfo.CodeHash,
					Genesis:      uniqueUtxoInfo.Genesis,
					SensibleId:   uniqueUtxoInfo.SensibleId,
					CustomData:   uniqueUtxoInfo.CustomData,
				}
			} else {
				continue
			}
			if output == nil {
				continue
			}
			outputs = append(outputs, output)

			if find := contractFtOutputs[address]; find != nil {
				contractFtOutputs[address] = append(find, output)
			} else {
				contractFtOutputs[address] = []*indexer.ContractFtOutput{output}
			}
		}

		txs[i] = &indexer.ContractFtTransaction{
			ID:      tx.Txid,
			Inputs:  inputs,
			Outputs: outputs,
		}
	}

	return &indexer.ContractFtBlock{
		Height:            height,
		Transactions:      txs,
		ContractFtOutputs: contractFtOutputs,
	}
}

// convertBlockBatch 转换区块中指定范围的交易
func (c *FtClient) convertBlockBatch(block *btcjson.GetBlockVerboseTxResult, height int, startIdx int, endIdx int, isLastBatch bool) *indexer.ContractFtBlock {
	if height < 0 || startIdx < 0 || endIdx <= startIdx || endIdx > len(block.Tx) {
		return nil
	}

	contractFtOutputs := make(map[string][]*indexer.ContractFtOutput)
	txsCount := endIdx - startIdx
	txs := make([]*indexer.ContractFtTransaction, 0, txsCount)

	for i := startIdx; i < endIdx; i++ {
		tx := block.Tx[i]

		inputs := make([]*indexer.ContractFtInput, len(tx.Vin))
		for j, in := range tx.Vin {
			id := in.Txid
			if id == "" {
				id = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			inputs[j] = &indexer.ContractFtInput{
				TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.Vout))}, ":"),
			}
		}

		outputs := make([]*indexer.ContractFtOutput, 0)
		for k, out := range tx.Vout {
			address := GetAddressFromScript(out.ScriptPubKey.Hex, nil, c.params, c.cfg.RPC.Chain)
			amount := strconv.FormatInt(int64(math.Round(out.Value*1e8)), 10)

			// 解析FT相关信息
			ftInfo, uniqueUtxoInfo, contractTypeStr, err := ParseContractFtInfo(out.ScriptPubKey.Hex, c.params)
			if err != nil {
				fmt.Println("ParseFtInfo error", err)
				return nil
			}
			var output *indexer.ContractFtOutput
			if contractTypeStr == "ft" {
				if ftInfo == nil {
					continue
				}
				output = &indexer.ContractFtOutput{
					ContractType: contractTypeStr,
					Address:      address,
					Value:        amount,
					Index:        int64(k),
					Height:       int64(height),
					CodeHash:     ftInfo.CodeHash,
					Genesis:      ftInfo.Genesis,
					SensibleId:   ftInfo.SensibleId,
					Name:         ftInfo.Name,
					Symbol:       ftInfo.Symbol,
					Amount:       strconv.FormatUint(ftInfo.Amount, 10),
					Decimal:      ftInfo.Decimal,
					FtAddress:    ftInfo.Address,
				}
			} else if contractTypeStr == "unique" {
				if uniqueUtxoInfo == nil {
					continue
				}
				output = &indexer.ContractFtOutput{
					ContractType: contractTypeStr,
					Address:      address,
					Value:        amount,
					Index:        int64(k),
					Height:       int64(height),
					CodeHash:     uniqueUtxoInfo.CodeHash,
					Genesis:      uniqueUtxoInfo.Genesis,
					SensibleId:   uniqueUtxoInfo.SensibleId,
					CustomData:   uniqueUtxoInfo.CustomData,
				}
			} else {
				continue
			}
			if output == nil {
				continue
			}
			outputs = append(outputs, output)

			if find := contractFtOutputs[address]; find != nil {
				contractFtOutputs[address] = append(find, output)
			} else {
				contractFtOutputs[address] = []*indexer.ContractFtOutput{output}
			}
		}

		txs = append(txs, &indexer.ContractFtTransaction{
			ID:      tx.Txid,
			Inputs:  inputs,
			Outputs: outputs,
		})
	}

	return &indexer.ContractFtBlock{
		Height:            height,
		Transactions:      txs,
		ContractFtOutputs: contractFtOutputs,
		IsPartialBlock:    !isLastBatch,
	}
}

// GetMaxTxPerBatch 获取每个批次中最大交易数量
func (c *FtClient) GetMaxTxPerBatch() int {
	if c.cfg != nil && c.cfg.MaxTxPerBatch > 0 {
		return c.cfg.MaxTxPerBatch
	}
	return 3000 // 默认值
}

// ParseFtInfo 从脚本中解析FT信息
func ParseContractFtInfo(scriptHex string, params *chaincfg.Params) (*decoder.FTUtxoInfo, *decoder.UniqueUtxoInfo, string, error) {
	scriptBytes, err := hex.DecodeString(scriptHex)
	if err != nil {
		return nil, nil, "", err
	}

	contractTypeStr := ""
	contractType := decoder.GetContractType(scriptBytes)
	if contractType == decoder.ContractTypeFT {
		contractTypeStr = "ft"
		ftUtxoInfo, err := decoder.ExtractFTUtxoInfo(scriptBytes, params)
		if err != nil {
			return nil, nil, "", err
		}
		return ftUtxoInfo, nil, contractTypeStr, nil
	} else if contractType == decoder.ContractTypeUnique {
		contractTypeStr = "unique"
		uniqueUtxoInfo, err := decoder.ExtractUniqueUtxoInfo(scriptBytes, params)
		if err != nil {
			return nil, nil, "", err
		}
		return nil, uniqueUtxoInfo, contractTypeStr, nil
	} else {
		contractTypeStr = "unknown"
		return nil, nil, contractTypeStr, nil
	}
}

// func checkFtUtxoValid() bool {
// 	ftUtxo := mempool.GetMempoolFtUtxo(utxoID)
// 	if ftUtxo == nil {
// 		return false
// 	}
// 	return true
// }
