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
	hash, err := c.GetBlockHash(int64(height))
	if err != nil {
		return fmt.Errorf("获取区块哈希失败，高度 %d: %w", height, err)
	}

	block, err := c.GetBlock(hash)
	if err != nil {
		return fmt.Errorf("获取区块失败，高度 %d: %w", height, err)
	}

	txCount := len(block.Tx)
	maxTxPerBatch := c.GetMaxTxPerBatch()

	if txCount > maxTxPerBatch {
		fmt.Printf("\n大区块处理: 高度=%d, 交易数=%d, 超过最大批次大小=%d, 将使用Channel进行分批处理\n",
			height, txCount, maxTxPerBatch)

		batchCh := make(chan *indexer.ContractFtBlock, 2)
		errCh := make(chan error, 1)
		doneCh := make(chan struct{})

		go func() {
			defer close(doneCh)
			for convertedBlock := range batchCh {
				if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
					errCh <- fmt.Errorf("索引高度 %d 的批次失败: %w", height, err)
					return
				}
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

			convertedBlock := c.convertBlockBatch(block, height, startIdx, endIdx, lastBatch)
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
				ftInfo, err := ParseFtInfo(out.ScriptPubKey.Hex, c.params)
				if err != nil {
					fmt.Println("ParseFtInfo error", err)
					continue
				}
				if ftInfo == nil {
					continue
				}

				outputs = append(outputs, &indexer.ContractFtOutput{
					Address:    address,
					Value:      amount,
					Index:      int64(k),
					CodeHash:   ftInfo.CodeHash,
					Genesis:    ftInfo.Genesis,
					SensibleId: ftInfo.SensibleId,
					Name:       ftInfo.Name,
					Symbol:     ftInfo.Symbol,
					Amount:     strconv.FormatUint(ftInfo.Amount, 10),
					Decimal:    ftInfo.Decimal,
					FtAddress:  ftInfo.Address,
				})

				if find := contractFtOutputs[address]; find != nil {
					contractFtOutputs[address] = append(find, outputs[k])
				} else {
					contractFtOutputs[address] = []*indexer.ContractFtOutput{outputs[k]}
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
			ftInfo, err := ParseFtInfo(out.ScriptPubKey.Hex, c.params)
			if err != nil {
				fmt.Println("ParseFtInfo error", err)
				return nil
			}
			if ftInfo == nil {
				continue
			}

			outputs = append(outputs, &indexer.ContractFtOutput{
				Address:    address,
				Value:      amount,
				CodeHash:   ftInfo.CodeHash,
				Genesis:    ftInfo.Genesis,
				SensibleId: ftInfo.SensibleId,
				Name:       ftInfo.Name,
				Symbol:     ftInfo.Symbol,
				Amount:     strconv.FormatUint(ftInfo.Amount, 10),
				Decimal:    ftInfo.Decimal,
				FtAddress:  ftInfo.Address,
			})

			if find := contractFtOutputs[address]; find != nil {
				contractFtOutputs[address] = append(find, outputs[k])
			} else {
				contractFtOutputs[address] = []*indexer.ContractFtOutput{outputs[k]}
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
			ftInfo, err := ParseFtInfo(out.ScriptPubKey.Hex, c.params)
			if err != nil {
				fmt.Println("ParseFtInfo error", err)
				return nil
			}
			if ftInfo == nil {
				continue
			}

			outputs = append(outputs, &indexer.ContractFtOutput{
				Address:    address,
				Value:      amount,
				CodeHash:   ftInfo.CodeHash,
				Genesis:    ftInfo.Genesis,
				SensibleId: ftInfo.SensibleId,
				Name:       ftInfo.Name,
				Symbol:     ftInfo.Symbol,
				Amount:     strconv.FormatUint(ftInfo.Amount, 10),
				Decimal:    ftInfo.Decimal,
				FtAddress:  ftInfo.Address,
			})

			if find := contractFtOutputs[address]; find != nil {
				contractFtOutputs[address] = append(find, outputs[k])
			} else {
				contractFtOutputs[address] = []*indexer.ContractFtOutput{outputs[k]}
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
func ParseFtInfo(scriptHex string, params *chaincfg.Params) (*decoder.FTUtxoInfo, error) {
	scriptBytes, err := hex.DecodeString(scriptHex)
	if err != nil {
		return nil, err
	}

	return decoder.ExtractFTUtxoInfo(scriptBytes, params)
}

// func checkFtUtxoValid() bool {
// 	ftUtxo := mempool.GetMempoolFtUtxo(utxoID)
// 	if ftUtxo == nil {
// 		return false
// 	}
// 	return true
// }
