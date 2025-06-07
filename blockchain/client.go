package blockchain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/indexer"
)

type Client struct {
	rpcClient *rpcclient.Client
	cfg       *config.Config
	params    *chaincfg.Params
}

func NewClient(cfg *config.Config) (*Client, error) {
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

	return &Client{
		rpcClient: client,
		cfg:       cfg,
		params:    params,
	}, nil
}

func (c *Client) GetBlock2(hash *chainhash.Hash) (*btcutil.Block, error) {
	msgBlock, err := c.rpcClient.GetBlock(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get block %s: %w", hash, err)
	}
	return btcutil.NewBlock(msgBlock), nil
}
func (c *Client) GetBlock(hash *chainhash.Hash) (*btcjson.GetBlockVerboseTxResult, error) {
	//msgBlock, err := c.rpcClient.GetBlock(hash)
	return c.rpcClient.GetBlockVerboseTx(hash)
}
func (c *Client) GetBlockHeader(hash *chainhash.Hash) (*wire.BlockHeader, error) {
	return c.rpcClient.GetBlockHeader(hash)
}

func (c *Client) GetBlockHash(height int64) (*chainhash.Hash, error) {
	hash, err := c.rpcClient.GetBlockHash(height)
	if err != nil {
		return nil, fmt.Errorf("failed to get block hash at height %d: %w", height, err)
	}
	return hash, nil
}

func (c *Client) GetBestBlockHash() (*chainhash.Hash, error) {
	hash, err := c.rpcClient.GetBestBlockHash()
	if err != nil {
		return nil, fmt.Errorf("failed to get best block hash: %w", err)
	}
	return hash, nil
}

func (c *Client) GetRawTransaction(txHashStr string) (*btcutil.Tx, error) {
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

func (c *Client) Shutdown() {
	c.rpcClient.Shutdown()
}

func (c *Client) GetBlockCount() (int, error) {
	count, err := c.rpcClient.GetBlockCount()
	if err != nil {
		return 0, fmt.Errorf("failed to get block count: %w", err)
	}
	return int(count), nil
}

// GetRawMempool 获取内存池中的所有交易ID
func (c *Client) GetRawMempool() ([]string, error) {
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

// func (c *Client) SyncBlocks(idx *indexer.UTXOIndexer) error {
// 	// Get last indexed height from storage
// 	lastHeight, err := idx.GetLastIndexedHeight()
// 	if err != nil {
// 		return fmt.Errorf("failed to get last indexed height: %w", err)
// 	}

// 	// Get current block count
// 	currentHeight, err := c.GetBlockCount()
// 	if err != nil {
// 		return err
// 	}
// 	// lastHeight = 120820
// 	// currentHeight = 120821
// 	// Sync from lastHeight+1 to currentHeight
// 	for height := lastHeight + 1; height <= currentHeight; height++ {
// 		hash, err := c.GetBlockHash(int64(height))
// 		if err != nil {
// 			return fmt.Errorf("failed to get block hash at height %d: %w", height, err)
// 		}
// 		block, err := c.GetBlock(hash)
// 		if err != nil {
// 			return fmt.Errorf("failed to get block at height %d: %w", height, err)
// 		}
// 		convertedBlock := c.convertBlock(block, height)
// 		if convertedBlock == nil {
// 			continue
// 		}

// 		if err := idx.IndexBlock(convertedBlock); err != nil {
// 			return fmt.Errorf("failed to index block at height %d: %w", height, err)
// 		}
// 	}

//		return nil
//	}
//
// SyncBlocks 持续同步区块的修改版
func (c *Client) SyncBlocks(idx *indexer.UTXOIndexer, checkInterval time.Duration, stopCh <-chan struct{}, onFirstSyncDone func()) error {
	// 参数说明：
	// idx - 索引器实例
	// checkInterval - 检查新区块的间隔时间
	// stopCh - 可选的停止信号通道
	// onFirstSyncDone - 首次同步完成后的回调函数

	firstSyncComplete := false

	for {
		select {
		case <-stopCh:
			return nil // 收到停止信号，干净退出
		default:
			// 继续执行
		}

		// 获取上次索引的高度
		lastHeight, err := idx.GetLastIndexedHeight()
		if err != nil {
			return fmt.Errorf("获取上次索引高度失败: %w", err)
		}

		// 获取当前区块链高度
		currentHeight, err := c.GetBlockCount()
		if err != nil {
			return fmt.Errorf("获取当前区块高度失败: %w", err)
		}

		// 检查是否有新区块
		if currentHeight <= lastHeight {
			// 如果首次同步已完成并且有回调函数，则调用它
			if !firstSyncComplete && onFirstSyncDone != nil {
				fmt.Printf("当前已索引到最新区块，高度: %d，首次同步完成\n", lastHeight)
				firstSyncComplete = true
				onFirstSyncDone()
			}
			//fmt.Printf("当前已索引到最新区块，高度: %d，等待新区块...\n", lastHeight)
			time.Sleep(checkInterval)
			continue
		}

		// 有新区块需要索引 - 设置进度条
		fmt.Printf("发现新区块，从高度 %d 索引到 %d\n", lastHeight+1, currentHeight)
		idx.InitProgressBar(currentHeight, lastHeight+1)

		// 同步新区块
		for height := lastHeight + 1; height <= currentHeight; height++ {
			if err := c.ProcessBlock(idx, height, true); err != nil {
				return fmt.Errorf("处理区块失败，高度 %d: %w", height, err)
			}
		}

		fmt.Printf("成功索引到当前高度 %d\n", currentHeight)

		// 如果首次同步已完成并且有回调函数，则调用它
		if !firstSyncComplete && onFirstSyncDone != nil {
			fmt.Printf("首次区块同步已完成，现在调用回调函数\n")
			firstSyncComplete = true
			onFirstSyncDone()
		}

		time.Sleep(checkInterval)
	}
}

// ProcessBlock 处理指定高度的区块
// 此函数封装了区块处理的通用流程，可被同步和重新索引功能共用
func (c *Client) ProcessBlock(idx *indexer.UTXOIndexer, height int, updateHeight bool) error {
	// 获取区块哈希
	var hash *chainhash.Hash
	var err error
	for {
		hash, err = c.GetBlockHash(int64(height))
		if err != nil {
			log.Printf("获取区块哈希失败，高度 %d: %v，3秒后重试...", height, err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}

	// 获取区块
	var block *btcjson.GetBlockVerboseTxResult
	for {
		block, err = c.GetBlock(hash)
		if err != nil {
			log.Printf("获取区块失败，高度 %d: %v，3秒后重试...", height, err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}

	// 获取交易总数
	txCount := len(block.Tx)

	// 获取批处理大小
	maxTxPerBatch := c.GetMaxTxPerBatch()

	// 如果是大区块，使用Channel进行分批处理
	if txCount > maxTxPerBatch {
		fmt.Printf("\n大区块处理: 高度=%d, 交易数=%d, 超过最大批次大小=%d, 将使用Channel进行分批处理\n",
			height, txCount, maxTxPerBatch)

		// 创建交易批次的通道
		batchCh := make(chan *indexer.Block, 2) // 缓冲区大小为2，平衡内存使用和生产-消费速率
		errCh := make(chan error, 1)
		doneCh := make(chan struct{})

		// 启动消费者goroutine来处理批次
		go func() {
			defer close(doneCh)

			for convertedBlock := range batchCh {
				// 索引当前批次
				if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
					errCh <- fmt.Errorf("索引高度 %d 的批次失败: %w", height, err)
					return
				}

				// 释放已处理的区块内存
				convertedBlock.Transactions = nil
				convertedBlock.AddressIncome = nil
				convertedBlock = nil

				// 强制垃圾回收
				runtime.GC()
			}
		}()

		// 生产者：分批读取交易并发送到通道
		// 初始化起始索引
		startIdx := 0
		var lastBatch bool

		for startIdx < txCount {
			// 确定当前批次的结束索引
			endIdx := startIdx + maxTxPerBatch
			if endIdx >= txCount {
				endIdx = txCount
				lastBatch = true
			} else {
				lastBatch = false
			}

			// 处理当前批次
			convertedBlock := c.convertBlockBatch(block, height, startIdx, endIdx, lastBatch)

			if convertedBlock == nil {
				close(batchCh)
				return fmt.Errorf("高度 %d 的批次 %d-%d 转换失败", height, startIdx+1, endIdx)
			}

			// 发送到通道
			select {
			case batchCh <- convertedBlock:
				// 成功发送
			case err := <-errCh:
				// 消费者出错
				close(batchCh)
				return err
			}

			// 更新起始索引
			startIdx = endIdx
		}

		// 关闭批次通道，表示不再有新批次
		close(batchCh)

		// 等待所有批次处理完成
		select {
		case err := <-errCh:
			return err
		case <-doneCh:
			// 所有批次处理完成
		}
	} else {
		// 正常处理小区块
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

func (c *Client) convertBlock(block *btcjson.GetBlockVerboseTxResult, height int) *indexer.Block {
	if height < 0 {
		return nil
	}

	// 获取MaxTxPerBatch参数
	maxTxPerBatch := 3000 // 默认值
	// 使用客户端配置
	if c.cfg != nil && c.cfg.MaxTxPerBatch > 0 {
		maxTxPerBatch = c.cfg.MaxTxPerBatch
	}

	// 检查区块交易数是否超过阈值
	txCount := len(block.Tx)
	if txCount > maxTxPerBatch {
		// 区块过大，需要分批处理
		fmt.Printf("\n大区块预处理: 高度=%d, 交易数=%d, 超过最大批次大小=%d, 将进行分批转换\n",
			height, txCount, maxTxPerBatch)

		// 处理所有输出（收入部分）
		addressIncome := make(map[string][]*indexer.Income)
		txs := make([]*indexer.Transaction, 0, txCount)

		for _, tx := range block.Tx {
			inputs := make([]*indexer.Input, len(tx.Vin))
			for j, in := range tx.Vin {
				id := in.Txid
				if id == "" {
					id = "0000000000000000000000000000000000000000000000000000000000000000"
				}
				inputs[j] = &indexer.Input{
					TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.Vout))}, ":"),
				}
			}

			outputs := make([]*indexer.Output, len(tx.Vout))
			for k, out := range tx.Vout {
				// var address string
				// // Extract address from script
				// scriptBytes, err := hex.DecodeString(out.ScriptPubKey.Hex)
				// if err != nil {
				// 	address = "errAddress"
				// } else {
				// 	_, addrs, _, err := txscript.ExtractPkScriptAddrs(scriptBytes, c.params)
				// 	if err == nil && len(addrs) > 0 {
				// 		if c.cfg.RPC.Chain == "mvc" {
				// 			address = addrs[0].EncodeAddress()
				// 		} else {
				// 			address = addrs[0].String()
				// 		}
				// 	} else {
				// 		address = "errAddress"
				// 	}
				// }
				address := GetAddressFromScript(out.ScriptPubKey.Hex, nil, c.params, c.cfg.RPC.Chain)

				amount := strconv.FormatInt(int64(math.Round(out.Value*1e8)), 10)
				outputs[k] = &indexer.Output{
					Address: address,
					Amount:  amount,
				}
				if find := addressIncome[address]; find != nil {
					addressIncome[address] = append(find, &indexer.Income{TxID: tx.Txid, Index: strconv.Itoa(k), Value: amount})
				} else {
					addressIncome[address] = []*indexer.Income{{TxID: tx.Txid, Index: strconv.Itoa(k), Value: amount}}
				}
			}

			txs = append(txs, &indexer.Transaction{
				ID:      tx.Txid,
				Inputs:  inputs,
				Outputs: outputs,
			})

			// 每批次处理maxTxPerBatch个交易
			if len(txs) >= maxTxPerBatch {
				// 创建子区块并返回
				subBlock := &indexer.Block{
					Height:        height,
					Transactions:  txs,
					AddressIncome: addressIncome,
				}

				// 由于大区块已在convertBlock阶段分批处理，设置特殊标记
				subBlock.IsPartialBlock = true

				return subBlock
			}
		}

		// 如果有剩余交易，创建最后一个子区块
		if len(txs) > 0 {
			subBlock := &indexer.Block{
				Height:        height,
				Transactions:  txs,
				AddressIncome: addressIncome,
			}

			// 最后一个批次标记为非部分区块
			subBlock.IsPartialBlock = false

			return subBlock
		}

		// 不应该执行到这里
		return nil
	}

	// 小区块正常处理
	addressIncome := make(map[string][]*indexer.Income)
	txs := make([]*indexer.Transaction, len(block.Tx))
	for i, tx := range block.Tx {
		inputs := make([]*indexer.Input, len(tx.Vin))
		for j, in := range tx.Vin {
			id := in.Txid
			if id == "" {
				//fmt.Println("==>find null id", in.Txid, in.Vout)
				id = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			inputs[j] = &indexer.Input{
				TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.Vout))}, ":"),
			}
		}

		outputs := make([]*indexer.Output, len(tx.Vout))
		for k, out := range tx.Vout {
			// var address string
			// // Extract address from script
			// scriptBytes, err := hex.DecodeString(out.ScriptPubKey.Hex)
			// if err != nil {
			// 	address = "errAddress"
			// } else {
			// 	_, addrs, _, err := txscript.ExtractPkScriptAddrs(scriptBytes, c.params)
			// 	if err == nil && len(addrs) > 0 {
			// 		if c.cfg.RPC.Chain == "mvc" {
			// 			address = addrs[0].EncodeAddress()
			// 		} else {
			// 			address = addrs[0].String()
			// 		}
			// 	} else {
			// 		// isCoinbase := i == 0 || (len(tx.Vin) == 1 && tx.Vin[0].Txid == "0000000000000000000000000000000000000000000000000000000000000000")
			// 		// if isCoinbase {
			// 		// 	fmt.Printf("  [Coinbase交易]\n")
			// 		// 	fmt.Printf("    脚本类型: %s,%v\n", out.ScriptPubKey.Type, out.Value)
			// 		// 	if len(out.ScriptPubKey.Addresses) > 0 {
			// 		// 		fmt.Printf("    地址: %s\n", out.ScriptPubKey.Addresses[0])
			// 		// 	}
			// 		// }
			// 		// Fallback for non-standard scripts
			// 		address = "errAddress"
			// 	}
			// }
			address := GetAddressFromScript(out.ScriptPubKey.Hex, nil, c.params, c.cfg.RPC.Chain)
			//fmt.Println("address", address, out.Value)
			amount := strconv.FormatInt(int64(math.Round(out.Value*1e8)), 10)
			outputs[k] = &indexer.Output{
				Address: address,
				Amount:  amount,
			}
			if find := addressIncome[address]; find != nil {
				addressIncome[address] = append(find, &indexer.Income{TxID: tx.Txid, Index: strconv.Itoa(k), Value: amount})
			} else {
				addressIncome[address] = []*indexer.Income{{TxID: tx.Txid, Index: strconv.Itoa(k), Value: amount}}
			}
		}

		txs[i] = &indexer.Transaction{
			ID:      tx.Txid,
			Inputs:  inputs,
			Outputs: outputs,
		}
		//fmt.Printf("%+v", outputs)
	}
	return &indexer.Block{
		Height:        height,
		Transactions:  txs,
		AddressIncome: addressIncome,
	}
}

// convertBlockBatch 转换区块中指定范围的交易
func (c *Client) convertBlockBatch(block *btcjson.GetBlockVerboseTxResult, height int, startIdx int, endIdx int, isLastBatch bool) *indexer.Block {
	if height < 0 || startIdx < 0 || endIdx <= startIdx || endIdx > len(block.Tx) {
		return nil
	}

	// 只处理指定范围内的交易
	addressIncome := make(map[string][]*indexer.Income)
	txsCount := endIdx - startIdx
	txs := make([]*indexer.Transaction, 0, txsCount)

	for i := startIdx; i < endIdx; i++ {
		tx := block.Tx[i]

		// 处理输入
		inputs := make([]*indexer.Input, len(tx.Vin))
		for j, in := range tx.Vin {
			id := in.Txid
			if id == "" {
				id = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			inputs[j] = &indexer.Input{
				TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.Vout))}, ":"),
			}
		}

		// 处理输出
		outputs := make([]*indexer.Output, len(tx.Vout))
		for k, out := range tx.Vout {
			// var address string
			// // 提取地址
			// scriptBytes, err := hex.DecodeString(out.ScriptPubKey.Hex)
			// if err != nil {
			// 	address = "errAddress"
			// } else {
			// 	_, addrs, _, err := txscript.ExtractPkScriptAddrs(scriptBytes, c.params)
			// 	if err == nil && len(addrs) > 0 {
			// 		if c.cfg.RPC.Chain == "mvc" {
			// 			address = addrs[0].EncodeAddress()
			// 		} else {
			// 			address = addrs[0].String()
			// 		}
			// 	} else {
			// 		address = "errAddress"
			// 	}
			// }
			address := GetAddressFromScript(out.ScriptPubKey.Hex, nil, c.params, c.cfg.RPC.Chain)
			// 处理金额
			amount := strconv.FormatInt(int64(math.Round(out.Value*1e8)), 10)
			outputs[k] = &indexer.Output{
				Address: address,
				Amount:  amount,
			}

			// 记录地址收入
			if find := addressIncome[address]; find != nil {
				addressIncome[address] = append(find, &indexer.Income{TxID: tx.Txid, Index: strconv.Itoa(k), Value: amount})
			} else {
				addressIncome[address] = []*indexer.Income{{TxID: tx.Txid, Index: strconv.Itoa(k), Value: amount}}
			}
		}

		// 添加到批次交易列表
		txs = append(txs, &indexer.Transaction{
			ID:      tx.Txid,
			Inputs:  inputs,
			Outputs: outputs,
		})
	}

	// 创建区块
	resultBlock := &indexer.Block{
		Height:         height,
		Transactions:   txs,
		AddressIncome:  addressIncome,
		IsPartialBlock: !isLastBatch, // 只有最后一个批次是完整的
	}

	return resultBlock
}

// GetMaxTxPerBatch 获取每个批次中最大交易数量
func (c *Client) GetMaxTxPerBatch() int {
	if c.cfg != nil && c.cfg.MaxTxPerBatch > 0 {
		return c.cfg.MaxTxPerBatch
	}
	return 3000 // 默认值
}

// ConvertBlock 提供区块转换的导出方法
func (c *Client) ConvertBlock(block *btcjson.GetBlockVerboseTxResult, height int) *indexer.Block {
	return c.convertBlock(block, height)
}

// ConvertBlockBatch 提供批量区块转换的导出方法
func (c *Client) ConvertBlockBatch(block *btcjson.GetBlockVerboseTxResult, height int, startIdx int, endIdx int, isLastBatch bool) *indexer.Block {
	return c.convertBlockBatch(block, height, startIdx, endIdx, isLastBatch)
}

// GetBlockHeaderWithTimeout 通过 HTTP JSON-RPC 请求节点 getblockheader，带超时
func (c *Client) GetBlockHeaderWithTimeout(blockHash string, timeout time.Duration) (map[string]interface{}, error) {
	reqBody := map[string]interface{}{
		"jsonrpc": "1.0",
		"id":      "getblockheader",
		"method":  "getblockheader",
		"params":  []interface{}{blockHash},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", "http://"+c.cfg.RPC.Host+":"+c.cfg.RPC.Port, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.cfg.RPC.User, c.cfg.RPC.Password)

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var respObj struct {
		Result map[string]interface{} `json:"result"`
		Error  interface{}            `json:"error"`
	}
	if err := json.Unmarshal(respBytes, &respObj); err != nil {
		return nil, err
	}
	if respObj.Error != nil {
		return nil, fmt.Errorf("rpc error: %v", respObj.Error)
	}
	return respObj.Result, nil
}
