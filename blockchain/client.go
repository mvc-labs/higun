package blockchain

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	bsvwire "github.com/bitcoinsv/bsvd/wire"
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
	Rpc       *rpcclient.Client
	cfg       *config.Config
	params    *chaincfg.Params
}

var RpcClient *rpcclient.Client

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
	// Set global RPC client
	RpcClient = client
	return &Client{
		rpcClient: client,
		cfg:       cfg,
		params:    params,
		Rpc:       client,
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
func (c *Client) GetBlockOnlyTxId(hash *chainhash.Hash) (*btcjson.GetBlockVerboseResult, error) {
	return c.rpcClient.GetBlockVerbose(hash)
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

// GetRawMempool gets all transaction IDs in the mempool
func (c *Client) GetRawMempool() ([]string, error) {
	hashes, err := c.rpcClient.GetRawMempool()
	if err != nil {
		return nil, fmt.Errorf("Failed to get mempool transaction list: %w", err)
	}

	// Convert hashes to strings
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
// SyncBlocks modified version for continuous block synchronization
func (c *Client) SyncBlocks(idx *indexer.UTXOIndexer, checkInterval time.Duration, stopCh <-chan struct{}, onFirstSyncDone func()) error {
	// Parameter description:
	// idx - Indexer instance
	// checkInterval - Interval for checking new blocks
	// stopCh - Optional stop signal channel
	// onFirstSyncDone - Callback function after first sync completion

	firstSyncComplete := false

	for {
		select {
		case <-stopCh:
			return nil // Received stop signal, clean exit
		default:
			// Continue execution
		}

		// Get last indexed height
		lastHeight, err := idx.GetLastIndexedHeight()
		if err != nil {
			return fmt.Errorf("Failed to get last indexed height: %w", err)
		}
		//test := []int{10000, 10002}
		// test := []int{126230, 126235}
		// if lastHeight < test[0] {
		// 	lastHeight = test[0] // For testing, should get from indexer in actual use
		// }
		// Get current blockchain height
		currentHeight, err := c.GetBlockCount()
		if err != nil {
			return fmt.Errorf("Failed to get current block height: %w", err)
		}
		//currentHeight = test[1] // For testing, should get from blockchain in actual use
		// Check if there are new blocks
		if currentHeight <= lastHeight {
			// If first sync is complete and there's a callback function, call it
			if !firstSyncComplete && onFirstSyncDone != nil {
				fmt.Printf("Currently indexed to latest block, height: %d, first sync completed\n", lastHeight)
				firstSyncComplete = true
				onFirstSyncDone()
			}
			//fmt.Printf("Currently indexed to latest block, height: %d, waiting for new blocks...\n", lastHeight)
			time.Sleep(checkInterval)
			continue
		}

		// There are new blocks to index - set progress bar
		fmt.Printf(">>Found new blocks, indexing from height %d to %d\n", lastHeight+1, currentHeight)
		idx.InitProgressBar(currentHeight, lastHeight+1)

		// Sync new blocks
		for height := lastHeight + 1; height <= currentHeight; height++ {
			t0 := time.Now()
			if err := c.ProcessBlock(idx, height, true); err != nil {
				return fmt.Errorf("Failed to process block at height %d: %w", height, err)
			}

			fmt.Printf(">>>Indexing height %d took: %.2fs\n", height, time.Since(t0).Seconds())
		}

		fmt.Printf("Successfully indexed to current height %d\n", currentHeight)

		// If first sync is complete and there's a callback function, call it
		if !firstSyncComplete && onFirstSyncDone != nil {
			fmt.Printf("First block sync completed, now calling callback function\n")
			firstSyncComplete = true
			onFirstSyncDone()
		}

		time.Sleep(checkInterval)
	}
}

// ProcessBlock processes blocks at specified height
// This function encapsulates the common block processing flow, can be shared by sync and reindex functions
func (c *Client) ProcessBlockBak(idx *indexer.UTXOIndexer, height int, updateHeight bool) error {
	// Get block hash
	var hash *chainhash.Hash
	var err error
	for {
		hash, err = c.GetBlockHash(int64(height))
		if err != nil {
			log.Printf("Failed to get block hash at height %d: %v, retrying in 3 seconds...", height, err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}

	// Get block (deprecated): fetching all transactions at once can cause memory explosion and even OOM in super large blocks.
	// var block *btcjson.GetBlockVerboseTxResult
	// for {
	// 	block, err = c.GetBlock(hash)
	// 	if err != nil {
	// 		log.Printf("Failed to get block, height %d: %v, retrying in 3 seconds...", height, err)
	// 		time.Sleep(3 * time.Second)
	// 		continue
	// 	}
	// 	break
	// }
	//Changed to only get block header and transaction IDs
	var block *btcjson.GetBlockVerboseResult
	for {
		block, err = c.GetBlockOnlyTxId(hash)
		if err != nil {
			log.Printf("Failed to get block, height %d: %v, retrying in 3 seconds...", height, err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}
	// Get total transaction count
	txCount := len(block.Tx)

	// Get batch size
	maxTxPerBatch := config.GlobalConfig.MaxTxPerBatch

	// If it's a large block, use Channel for batch processing
	if txCount >= maxTxPerBatch {
		fmt.Printf("\nLarge block processing: height=%d, transaction count=%d, exceeds max batch size=%d, will use Channel for batch processing\n",
			height, txCount, maxTxPerBatch)
	}
	// Create channel for transaction batches
	batchCh := make(chan *indexer.Block, 2) // Buffer size of 2, balancing memory usage and producer-consumer rate
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	// Start consumer goroutine to process batches
	go func() {
		defer close(doneCh)

		for convertedBlock := range batchCh {
			// Index current batch
			if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
				errCh <- fmt.Errorf("Failed to index batch at height %d: %w", height, err)
				return
			}

			// Release memory of processed block
			convertedBlock.Transactions = nil
			convertedBlock.AddressIncome = nil
			convertedBlock = nil

			// Force garbage collection
			runtime.GC()
		}
	}()

	// Producer: read transactions in batches and send to channel
	// Initialize start index
	startIdx := 0
	var lastBatch bool

	for startIdx < txCount {
		// Determine end index of current batch
		endIdx := startIdx + maxTxPerBatch
		if endIdx >= txCount {
			endIdx = txCount
			lastBatch = true
		} else {
			lastBatch = false
		}

		// Process current batch
		convertedBlock := c.convertBlockBatch(block, height, startIdx, endIdx, lastBatch)
		fmt.Printf("\rCurrent %d/%d ", startIdx, txCount)
		if convertedBlock == nil {
			close(batchCh)
			return fmt.Errorf("Failed to convert batch %d-%d at height %d", startIdx+1, endIdx, height)
		}

		// Send to channel
		select {
		case batchCh <- convertedBlock:
			// Successfully sent
		case err := <-errCh:
			// Consumer error
			close(batchCh)
			return err
		}

		// Update start index
		startIdx = endIdx
	}

	// Close batch channel, indicating no more batches
	close(batchCh)

	// Wait for all batches to complete
	select {
	case err := <-errCh:
		return err
	case <-doneCh:
		// All batches completed
	}
	// } else {
	// 	// Normal processing for small blocks
	// 	convertedBlock := c.convertBlock(block, height)
	// 	if convertedBlock == nil {
	// 		return fmt.Errorf("Block conversion failed, height %d", height)
	// 	}

	// 	if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
	// 		return fmt.Errorf("Failed to index block, height %d: %w", height, err)
	// 	}
	// }

	return nil
}
func (c *Client) ProcessBlock(idx *indexer.UTXOIndexer, height int, updateHeight bool) error {
	// Get block hash
	var hash *chainhash.Hash
	var err error
	for {
		hash, err = c.GetBlockHash(int64(height))
		if err != nil {
			log.Printf("Failed to get block hash, height %d: %v, retrying in 3 seconds...", height, err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}
	// Get original block data (hex string)
	var blockHex string
	t1 := time.Now()
	for {
		// getblock <blockhash> 0
		resp, err := c.rpcClient.RawRequest("getblock", []json.RawMessage{
			json.RawMessage(fmt.Sprintf("\"%s\"", hash.String())),
			json.RawMessage("0"),
		})
		if err != nil {
			log.Printf("Failed to get original block data, height %d: %v, retrying in 3 seconds...", height, err)
			time.Sleep(3 * time.Second)
			continue
		}
		if err := json.Unmarshal(resp, &blockHex); err != nil {
			log.Printf("Failed to parse original block data, height %d: %v, retrying in 3 seconds...", height, err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}
	log.Printf("Time to get block(%d) original data: %.2fs", height, time.Since(t1).Seconds())
	t2 := time.Now()
	// Local block parsing
	blockBytes, err := hex.DecodeString(blockHex)
	if err != nil {
		return fmt.Errorf("Block hex decode failed, height %d: %w", height, err)
	}

	msgBlock := &bsvwire.MsgBlock{}

	if err := msgBlock.Deserialize(bytes.NewReader(blockBytes)); err != nil {
		return fmt.Errorf("Block deserialization failed, height %d: %w", height, err)
	}
	log.Printf("Block deserialization time: %.2fs", time.Since(t2).Seconds())

	txCount := len(msgBlock.Transactions)
	maxTxPerBatch := config.GlobalConfig.MaxTxPerBatch

	// Local batch processing
	startIdx := 0
	for startIdx < txCount {
		endIdx := startIdx + maxTxPerBatch
		if endIdx > txCount {
			endIdx = txCount
		}
		// Assemble indexer.Block
		blockPart := &indexer.Block{
			Height:         height,
			Transactions:   make([]*indexer.Transaction, 0, endIdx-startIdx),
			AddressIncome:  make(map[string][]*indexer.Income),
			IsPartialBlock: endIdx != txCount,
		}
		t0 := time.Now()
		for i := startIdx; i < endIdx; i++ {
			tx := msgBlock.Transactions[i]
			// You can add input/output parsing logic here, same as original convertBlock
			indexerTx := c.convertWireTxToIndexerTx(tx)
			//fmt.Println("@@txID:", indexerTx.ID)
			// for _, ot := range indexerTx.Outputs {
			// 	fmt.Println("@@address:", ot.Address, "amount:", ot.Amount)
			// }
			blockPart.Transactions = append(blockPart.Transactions, indexerTx)
			// Merge incomes
			// for addr, incs := range incomes {
			// 	if addr == "errAddress" {
			// 		continue
			// 	}
			// 	blockPart.AddressIncome[addr] = append(blockPart.AddressIncome[addr], incs...)
			// }
		}
		fmt.Println("Current batch:", startIdx, "to", endIdx, "transactions:", len(blockPart.Transactions), "time:", time.Since(t0).Seconds())
		totalInputs, totalOutputs := 0, 0
		for _, tx := range blockPart.Transactions {
			totalInputs += len(tx.Inputs)
			totalOutputs += len(tx.Outputs)
		}
		fmt.Printf("Batch %d-%d, total inputs: %d, total outputs: %d\n", startIdx, endIdx, totalInputs, totalOutputs)
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		fmt.Printf("Batch %d-%d, memory usage: %.2f MB\n", startIdx, endIdx, float64(m.Alloc)/1024/1024)
		// Index
		if err := idx.IndexBlock(blockPart, updateHeight); err != nil {
			return fmt.Errorf("Index block failed, height %d: %w", height, err)
		}
		// Release
		blockPart.Transactions = nil
		blockPart.AddressIncome = nil
		startIdx = endIdx
		if txCount > 400000 {
			runtime.GC() // Force garbage collection to avoid high memory usage
		}
	}
	idx.SetMempoolCleanedHeight(int64(height)) // Update cleaned height
	return nil
}
func (c *Client) ProcessBlockBak2(idx *indexer.UTXOIndexer, height int, updateHeight bool) error {
	// Get block hash
	var hash *chainhash.Hash
	var err error
	for {
		hash, err = c.GetBlockHash(int64(height))
		if err != nil {
			log.Printf("Failed to get block hash, height %d: %v, retrying in 3 seconds...", height, err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}

	// Only get block header and transaction IDs
	var block *btcjson.GetBlockVerboseResult
	for {
		block, err = c.GetBlockOnlyTxId(hash)
		if err != nil {
			log.Printf("Failed to get block, height %d: %v, retrying in 3 seconds...", height, err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}
	txCount := len(block.Tx)

	// Threshold check
	if txCount <= config.GlobalConfig.MaxTxPerBatch {
		// Get all transactions at once
		var fullBlock *btcjson.GetBlockVerboseTxResult
		for {
			fullBlock, err = c.GetBlock(hash)
			if err != nil {
				log.Printf("Failed to get block (with all transactions), height %d: %v, retrying in 3 seconds...", height, err)
				time.Sleep(3 * time.Second)
				continue
			}
			break
		}
		convertedBlock := c.convertBlock(fullBlock, height)
		if convertedBlock == nil {
			return fmt.Errorf("Block conversion failed, height %d", height)
		}
		if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
			return fmt.Errorf("Failed to index block, height %d: %w", height, err)
		}
		return nil
	}

	// Large block, use batch + channel method
	maxTxPerBatch := config.GlobalConfig.MaxTxPerBatch
	st := time.Now()
	fmt.Printf("\nLarge block processing: height=%d, transaction count=%d, exceeds threshold=%d, will use Channel for batch processing\n",
		height, txCount, maxTxPerBatch)

	batchCh := make(chan *indexer.Block, 2)
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		for convertedBlock := range batchCh {
			t1 := time.Now()
			if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
				errCh <- fmt.Errorf("Failed to index batch at height %d: %w", height, err)
				return
			}
			fmt.Printf("\nIndex batch time: %.2fs", time.Since(t1).Seconds())
			convertedBlock.Transactions = nil
			convertedBlock.AddressIncome = nil
			convertedBlock = nil
			//runtime.GC()
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
		t0 := time.Now()
		convertedBlock := c.convertBlockBatch(block, height, startIdx, endIdx, lastBatch)
		fmt.Printf("Current %d/%d/%d, conversion time %.2fs\n", startIdx, endIdx, txCount, time.Since(t0).Seconds())
		if convertedBlock == nil {
			close(batchCh)
			return fmt.Errorf("Failed to convert batch %d-%d at height %d", startIdx+1, endIdx, height)
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
	fmt.Printf("\nLarge block processing complete: height=%d, transaction count=%d, time=%.2f seconds\n", height, txCount, time.Since(st).Seconds())
	return nil
}
func (c *Client) convertBlock(block *btcjson.GetBlockVerboseTxResult, height int) *indexer.Block {
	if height < 0 {
		return nil
	}

	// Get MaxTxPerBatch parameter
	maxTxPerBatch := config.GlobalConfig.MaxTxPerBatch
	// Use client configuration
	// if c.cfg != nil && c.cfg.MaxTxPerBatch > 0 {
	// 	maxTxPerBatch = c.cfg.MaxTxPerBatch
	// }

	// Check if block transaction count exceeds threshold
	txCount := len(block.Tx)
	if txCount > maxTxPerBatch {
		// Block is too large, needs batch processing
		fmt.Printf("\n==>Large block preprocessing: height=%d, transaction count=%d, exceeds max batch size=%d, will perform batch conversion\n",
			height, txCount, maxTxPerBatch)

		// Process all outputs (income part)
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

			// Process maxTxPerBatch transactions per batch
			if len(txs) >= maxTxPerBatch {
				// Create sub-block and return
				subBlock := &indexer.Block{
					Height:        height,
					Transactions:  txs,
					AddressIncome: addressIncome,
				}

				// Since large blocks are already batch processed in convertBlock stage, set special flag
				subBlock.IsPartialBlock = true

				return subBlock
			}
		}

		// If there are remaining transactions, create the last sub-block
		if len(txs) > 0 {
			subBlock := &indexer.Block{
				Height:        height,
				Transactions:  txs,
				AddressIncome: addressIncome,
			}

			// Mark the last batch as non-partial block
			subBlock.IsPartialBlock = false

			return subBlock
		}

		// Should not execute here
		return nil
	}

	// Normal processing for small blocks
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
			// 		// 	fmt.Printf("  [Coinbase Transaction]\n")
			// 		// 	fmt.Printf("    Script Type: %s,%v\n", out.ScriptPubKey.Type, out.Value)
			// 		// 	if len(out.ScriptPubKey.Addresses) > 0 {
			// 		// 		fmt.Printf("    Address: %s\n", out.ScriptPubKey.Addresses[0])
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

// Concurrently get transaction details
func (c *Client) fetchTxsConcurrently(ctx context.Context, txHashes []string, maxConcurrency int) ([]*btcjson.TxRawResult, error) {
	type result struct {
		idx int
		tx  *btcjson.TxRawResult
		err error
	}
	results := make([]*btcjson.TxRawResult, len(txHashes))
	jobs := make(chan int, len(txHashes))
	resCh := make(chan result, len(txHashes))

	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case idx, ok := <-jobs:
					if !ok {
						return
					}
					txid := txHashes[idx]
					txHash, err := chainhash.NewHashFromStr(txid)
					if err != nil {
						resCh <- result{idx: idx, tx: nil, err: err}
						continue
					}
					tx, err := c.rpcClient.GetRawTransactionVerbose(txHash)
					resCh <- result{idx: idx, tx: tx, err: err}
				}
			}
		}()
	}

	// Distribute tasks
	for i := range txHashes {
		jobs <- i
	}
	close(jobs)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(resCh)
	}()

	// Collect results
	var firstErr error
	for r := range resCh {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		results[r.idx] = r.tx
	}
	return results, firstErr
}
func (c *Client) getRawTransactionsConcurrent(txids []string, concurrency int) ([]*btcjson.TxRawResult, error) {
	type result struct {
		idx int
		tx  *btcjson.TxRawResult
		err error
	}
	results := make([]*btcjson.TxRawResult, len(txids))
	ch := make(chan result, len(txids))
	sem := make(chan struct{}, concurrency)

	for i, txid := range txids {
		sem <- struct{}{}
		go func(i int, txid string) {
			defer func() { <-sem }()
			txHash, err := chainhash.NewHashFromStr(txid)
			if err != nil {
				ch <- result{i, nil, err}
				return
			}
			tx, err := c.rpcClient.GetRawTransactionVerbose(txHash)
			ch <- result{i, tx, err}
		}(i, txid)
	}

	for range txids {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		results[r.idx] = r.tx
	}
	return results, nil
}
func (c *Client) convertWireTxToIndexerTx(tx *bsvwire.MsgTx) *indexer.Transaction {
	newHash, _ := GetNewHash2(tx)
	//fmt.Printf("convertWireTxToIndexerTx txid: %s\n", newHash)
	// Process inputs
	inputs := make([]*indexer.Input, len(tx.TxIn))
	for j, in := range tx.TxIn {
		prevTxid := in.PreviousOutPoint.Hash.String()
		vout := in.PreviousOutPoint.Index
		id := prevTxid
		if id == "" {
			id = "0000000000000000000000000000000000000000000000000000000000000000"
		}
		inputs[j] = &indexer.Input{
			TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(vout))}, ":"),
		}
	}

	// Process outputs
	outputs := make([]*indexer.Output, len(tx.TxOut))

	for k, out := range tx.TxOut {
		// Get address (you need to implement or reuse GetAddressFromScript, parameter is out.PkScript)
		address := GetAddressFromScript(hex.EncodeToString(out.PkScript), nil, c.params, c.cfg.RPC.Chain)
		// Amount
		amount := strconv.FormatInt(int64(math.Round(float64(out.Value))), 10) // out.Value unit is satoshi
		outputs[k] = &indexer.Output{
			Address: address,
			Amount:  amount,
		}
	}

	indexerTx := &indexer.Transaction{
		ID:      newHash,
		Inputs:  inputs,
		Outputs: outputs,
	}
	return indexerTx
}

// convertBlockBatch converts transactions in specified range of a block
func (c *Client) convertBlockBatch(block *btcjson.GetBlockVerboseResult, height int, startIdx int, endIdx int, isLastBatch bool) *indexer.Block {
	if height < 0 || startIdx < 0 || endIdx <= startIdx || endIdx > len(block.Tx) {
		return nil
	}

	t0 := time.Now()
	addressIncome := make(map[string][]*indexer.Income)
	txsCount := endIdx - startIdx
	txs := make([]*indexer.Transaction, 0, txsCount)

	// 1. Batch get transaction details
	tFetch := time.Now()
	results, err := c.fetchTxsConcurrently(context.TODO(), block.Tx[startIdx:endIdx], config.GlobalConfig.TxConcurrency)
	if err != nil {
		fmt.Printf("fetchTxsConcurrently batch get transactions time %.2fs, error: %v\n", time.Since(tFetch).Seconds(), err)
		return nil
	}
	fmt.Printf("fetchTxsConcurrently batch get transactions time %.2fs\n", time.Since(tFetch).Seconds())

	// 2. Process each transaction
	tParse := time.Now()
	for _, tx := range results {
		// Process inputs
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

		// Process outputs
		outputs := make([]*indexer.Output, len(tx.Vout))
		for k, out := range tx.Vout {
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
	}
	fmt.Printf("Transaction parsing and assembly time %.2fs\n", time.Since(tParse).Seconds())

	results = nil

	fmt.Printf("convertBlockBatch total time %.2fs\n", time.Since(t0).Seconds())

	resultBlock := &indexer.Block{
		Height:         height,
		Transactions:   txs,
		AddressIncome:  addressIncome,
		IsPartialBlock: !isLastBatch,
	}
	return resultBlock
}

// GetMaxTxPerBatch gets the maximum number of transactions per batch
// func (c *Client) GetMaxTxPerBatch() int {
// 	if c.cfg != nil && c.cfg.MaxTxPerBatch > 0 {
// 		return c.cfg.MaxTxPerBatch
// 	}
// 	return 3000 // Default value
// }

// ConvertBlock provides exported method for block conversion
func (c *Client) ConvertBlock(block *btcjson.GetBlockVerboseTxResult, height int) *indexer.Block {
	return c.convertBlock(block, height)
}

// ConvertBlockBatch provides exported method for batch block conversion
func (c *Client) ConvertBlockBatch(block *btcjson.GetBlockVerboseResult, height int, startIdx int, endIdx int, isLastBatch bool) *indexer.Block {
	return c.convertBlockBatch(block, height, startIdx, endIdx, isLastBatch)
}

// GetBlockHeaderWithTimeout requests node getblockheader via HTTP JSON-RPC with timeout
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
