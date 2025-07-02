package blockchain

import (
	"encoding/hex"
	"encoding/json"
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

func (c *FtClient) GetBlockVerbose(hash *chainhash.Hash) (*btcjson.GetBlockVerboseResult, error) {
	return c.rpcClient.GetBlockVerbose(hash)
}

func (c *FtClient) GetBlockHash(height int64) (*chainhash.Hash, error) {
	hash, err := c.rpcClient.GetBlockHash(height)
	if err != nil {
		return nil, fmt.Errorf("failed to get block hash at height %d: %w", height, err)
	}
	return hash, nil
}

// GetRawMempool gets all transaction IDs in mempool
func (c *FtClient) GetRawMempool() ([]string, error) {
	hashes, err := c.rpcClient.GetRawMempool()
	if err != nil {
		return nil, fmt.Errorf("failed to get mempool transaction list: %w", err)
	}

	// Convert hashes to strings
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

func (c *FtClient) GetRawTransactionHex(txHashStr string) (string, error) {
	txHash, err := chainhash.NewHashFromStr(txHashStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse transaction hash %s: %w", txHashStr, err)
	}

	// Use RawRequest to directly call getrawtransaction RPC command
	params := []json.RawMessage{
		json.RawMessage(fmt.Sprintf(`"%s"`, txHash.String())),
	}
	result, err := c.rpcClient.RawRequest("getrawtransaction", params)
	if err != nil {
		return "", fmt.Errorf("failed to get transaction hex %s: %w", txHash, err)
	}

	var txHex string
	if err := json.Unmarshal(result, &txHex); err != nil {
		return "", fmt.Errorf("failed to unmarshal transaction hex: %w", err)
	}

	return txHex, nil
}

// func (c *FtClient) GetRawTransactionHex(txHashStr string) (*btcutil.Tx, error) {
// 	txHash, err := chainhash.NewHashFromStr(txHashStr)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to parse transaction hash %s: %w", txHashStr, err)
// 	}
// 	tx, err := c.rpcClient.GetRawTransactionVerbose(txHash)
// 	if err != nil {
// 		return nil,  fmt.Errorf("failed to get transaction %s: %w", txHash, err)
// 	}
// 	return tx, nil
// }

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

// SyncBlocks continuously syncs blocks (modified version)
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
			return fmt.Errorf("failed to get last indexed height: %w", err)
		}

		currentHeight, err := c.GetBlockCount()
		if err != nil {
			return fmt.Errorf("failed to get current block height: %w", err)
		}

		if currentHeight <= lastHeight {
			if !firstSyncComplete && onFirstSyncDone != nil {
				fmt.Printf("Currently indexed to latest block, height: %d, first sync completed\n", lastHeight)
				firstSyncComplete = true
				onFirstSyncDone()
			}
			time.Sleep(checkInterval)
			continue
		}

		fmt.Printf("Found new blocks, indexing from height %d to %d\n", lastHeight+1, currentHeight)
		idx.InitProgressBar(currentHeight, lastHeight+1)

		for height := lastHeight + 1; height <= currentHeight; height++ {
			if err := c.ProcessBlock(idx, height, true); err != nil {
				return fmt.Errorf("failed to process block, height %d: %w", height, err)
			}
		}

		fmt.Printf("Successfully indexed to current height %d\n", currentHeight)

		if !firstSyncComplete && onFirstSyncDone != nil {
			fmt.Printf("First block sync completed, now calling callback function\n")
			firstSyncComplete = true
			onFirstSyncDone()
		}

		time.Sleep(checkInterval)
	}
}

// ProcessBlock processes block at specified height
func (c *FtClient) ProcessBlock(idx *indexer.ContractFtIndexer, height int, updateHeight bool) error {
	t1 := time.Now()
	hash, err := c.GetBlockHash(int64(height))
	if err != nil {
		return fmt.Errorf("failed to get block hash, height %d: %w", height, err)
	}

	// First use GetBlockVerbose to get block size
	blockVerbose, err := c.GetBlockVerbose(hash)
	if err != nil {
		return fmt.Errorf("failed to get block, height %d: %w", height, err)
	}

	txCount := len(blockVerbose.Tx)
	blockSize := blockVerbose.Size
	useRawTx := txCount > 100000 || blockSize > 800*1024*1024 // If transaction count exceeds 100k or block size exceeds 800MB, use RawTxInBlock mode

	var block *btcjson.GetBlockVerboseTxResult
	if !useRawTx {
		block, err = c.GetBlock(hash)
		if err != nil {
			return fmt.Errorf("failed to get block, height %d: %w", height, err)
		}
	} else {
		fmt.Printf("[%d]Transaction count: %d, Block size: %d MB, using RawTxInBlock mode\n", height, txCount, blockSize/1024/1024)
	}
	t2 := time.Now()

	maxTxPerBatch := c.GetMaxTxPerBatch()

	t3 := time.Now()
	if txCount > maxTxPerBatch {
		fmt.Printf("\nLarge block processing: Height=%d, Transaction count=%d, Exceeds max batch size=%d, RPC block data fetch time: %v seconds, will use Channel for batch processing\n",
			height, txCount, maxTxPerBatch, t2.Sub(t1).Seconds())

		batchCh := make(chan *indexer.ContractFtBlock, 2)
		errCh := make(chan error, 1)
		doneCh := make(chan struct{})

		go func() {
			defer close(doneCh)
			for convertedBlock := range batchCh {
				if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
					errCh <- fmt.Errorf("failed to index batch at height %d: %w", height, err)
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

			var convertedBlock *indexer.ContractFtBlock
			if useRawTx {
				convertedBlock = c.convertBlockBatchByRawTx(blockVerbose, height, startIdx, endIdx, lastBatch)
			} else {
				convertedBlock = c.convertBlockBatch(block, height, startIdx, endIdx, lastBatch)
			}

			if convertedBlock == nil {
				close(batchCh)
				return fmt.Errorf("failed to convert batch %d-%d at height %d", startIdx+1, endIdx, height)
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
		var convertedBlock *indexer.ContractFtBlock
		if useRawTx {
			convertedBlock = c.convertBlockByRawTx(blockVerbose, height)
		} else {
			convertedBlock = c.convertBlock(block, height)
		}
		if convertedBlock == nil {
			return fmt.Errorf("failed to convert block, height %d", height)
		}

		if err := idx.IndexBlock(convertedBlock, updateHeight); err != nil {
			return fmt.Errorf("failed to index block, height %d: %w", height, err)
		}
	}

	t4 := time.Now()
	fmt.Printf("\nProcessing block %d took: %v seconds\n", height, t4.Sub(t3).Seconds())
	return nil
}

// convertBlock converts block
func (c *FtClient) convertBlock(block *btcjson.GetBlockVerboseTxResult, height int) *indexer.ContractFtBlock {
	if height < 0 {
		return nil
	}

	maxTxPerBatch := c.GetMaxTxPerBatch()
	txCount := len(block.Tx)

	if txCount > maxTxPerBatch {
		fmt.Printf("\nLarge block preprocessing: Height=%d, Transaction count=%d, Exceeds max batch size=%d, will perform batch conversion\n",
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

				// Parse FT related information
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

	// Small block processing
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

			// Parse FT related information
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

// convertBlockBatch converts transactions in specified range of block
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

			// Parse FT related information
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

// convertBlock converts block
func (c *FtClient) convertBlockByRawTx(block *btcjson.GetBlockVerboseResult, height int) *indexer.ContractFtBlock {
	if height < 0 {
		return nil
	}

	maxTxPerBatch := c.GetMaxTxPerBatch()
	txCount := len(block.Tx)

	if txCount > maxTxPerBatch {
		fmt.Printf("\nLarge block preprocessing: Height=%d, Transaction count=%d, Exceeds max batch size=%d, will perform batch conversion\n",
			height, txCount, maxTxPerBatch)

		contractFtOutputs := make(map[string][]*indexer.ContractFtOutput)
		txs := make([]*indexer.ContractFtTransaction, 0, txCount)

		for _, txId := range block.Tx {
			txRaw, err := c.GetRawTransactionHex(txId)
			if err != nil {
				fmt.Println("GetRawTransaction error", err)
				return nil
			}
			txRawByte, err := hex.DecodeString(txRaw)
			if err != nil {
				fmt.Println("DecodeString error", err)
				return nil
			}
			msgTx, err := DeserializeMvcTransaction(txRawByte)
			if err != nil {
				fmt.Println("DeserializeMvcTransaction error", err)
				return nil
			}

			inputs := make([]*indexer.ContractFtInput, len(msgTx.TxIn))
			for j, in := range msgTx.TxIn {
				id := in.PreviousOutPoint.Hash.String()
				if id == "" {
					id = "0000000000000000000000000000000000000000000000000000000000000000"
				}
				inputs[j] = &indexer.ContractFtInput{
					TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.PreviousOutPoint.Index))}, ":"),
				}
			}

			outputs := make([]*indexer.ContractFtOutput, 0)
			for k, out := range msgTx.TxOut {
				address := GetAddressFromScript(hex.EncodeToString(out.PkScript), nil, c.params, c.cfg.RPC.Chain)
				amount := strconv.FormatInt(out.Value, 10)

				// Parse FT related information
				var output *indexer.ContractFtOutput
				ftInfo, uniqueUtxoInfo, contractTypeStr, err := ParseContractFtInfo(hex.EncodeToString(out.PkScript), c.params)
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
				ID:      txId,
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

	// Small block processing
	contractFtOutputs := make(map[string][]*indexer.ContractFtOutput)
	txs := make([]*indexer.ContractFtTransaction, len(block.Tx))

	for i, txId := range block.Tx {
		txRaw, err := c.GetRawTransactionHex(txId)
		if err != nil {
			fmt.Println("GetRawTransaction error", err)
			return nil
		}
		txRawByte, err := hex.DecodeString(txRaw)
		if err != nil {
			fmt.Println("DecodeString error", err)
			return nil
		}
		msgTx, err := DeserializeMvcTransaction(txRawByte)
		if err != nil {
			fmt.Println("DeserializeMvcTransaction error", err)
			return nil
		}

		inputs := make([]*indexer.ContractFtInput, len(msgTx.TxIn))
		for j, in := range msgTx.TxIn {
			id := in.PreviousOutPoint.Hash.String()
			if id == "" {
				id = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			inputs[j] = &indexer.ContractFtInput{
				TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.PreviousOutPoint.Index))}, ":"),
			}
		}

		outputs := make([]*indexer.ContractFtOutput, 0)
		for k, out := range msgTx.TxOut {
			address := GetAddressFromScript(hex.EncodeToString(out.PkScript), nil, c.params, c.cfg.RPC.Chain)
			amount := strconv.FormatInt(out.Value, 10)

			// Parse FT related information
			ftInfo, uniqueUtxoInfo, contractTypeStr, err := ParseContractFtInfo(hex.EncodeToString(out.PkScript), c.params)
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
			ID:      txId,
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

// convertBlockBatch converts transactions in specified range of block
func (c *FtClient) convertBlockBatchByRawTx(block *btcjson.GetBlockVerboseResult, height int, startIdx int, endIdx int, isLastBatch bool) *indexer.ContractFtBlock {
	if height < 0 || startIdx < 0 || endIdx <= startIdx || endIdx > len(block.Tx) {
		return nil
	}

	contractFtOutputs := make(map[string][]*indexer.ContractFtOutput)
	txsCount := endIdx - startIdx
	txs := make([]*indexer.ContractFtTransaction, 0, txsCount)

	for i := startIdx; i < endIdx; i++ {
		txId := block.Tx[i]

		txRaw, err := c.GetRawTransactionHex(txId)
		if err != nil {
			fmt.Println("GetRawTransaction error", err)
			return nil
		}
		txRawByte, err := hex.DecodeString(txRaw)
		if err != nil {
			fmt.Println("DecodeString error", err)
			return nil
		}
		msgTx, err := DeserializeMvcTransaction(txRawByte)
		if err != nil {
			fmt.Println("DeserializeMvcTransaction error", err)
			return nil
		}

		inputs := make([]*indexer.ContractFtInput, len(msgTx.TxIn))
		for j, in := range msgTx.TxIn {
			id := in.PreviousOutPoint.Hash.String()
			if id == "" {
				id = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			inputs[j] = &indexer.ContractFtInput{
				TxPoint: common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.PreviousOutPoint.Index))}, ":"),
			}
		}

		outputs := make([]*indexer.ContractFtOutput, 0)
		for k, out := range msgTx.TxOut {
			address := GetAddressFromScript(hex.EncodeToString(out.PkScript), nil, c.params, c.cfg.RPC.Chain)
			amount := strconv.FormatInt(out.Value, 10)

			// Parse FT related information
			ftInfo, uniqueUtxoInfo, contractTypeStr, err := ParseContractFtInfo(hex.EncodeToString(out.PkScript), c.params)
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
			ID:      txId,
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

// GetMaxTxPerBatch gets the maximum number of transactions per batch
func (c *FtClient) GetMaxTxPerBatch() int {
	if c.cfg != nil && c.cfg.MaxTxPerBatch > 0 {
		return c.cfg.MaxTxPerBatch
	}
	return 3000 // Default value
}

// ParseFtInfo parses FT information from script
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
