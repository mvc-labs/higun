package indexer

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-colorable"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/storage"
	"github.com/schollz/progressbar/v3"
)

var (
	// Record the last time logs were printed
	lastLogTime time.Time
)

type ContractFtIndexer struct {
	contractFtUtxoStore  *storage.PebbleStore // Store contract Utxo data key: txID, value:FtAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType,...
	addressFtIncomeStore *storage.PebbleStore // Store address-related FT contract Utxo data key: FtAddress, value: CodeHash@Genesis@Amount@TxID@Index@Value@height,...
	addressFtSpendStore  *storage.PebbleStore // Store used FT contract Utxo data key: FtAddress, value: txid@index@codeHash@genesis@sensibleId@amount@value@height@usedTxId,...

	contractFtInfoStore          *storage.PebbleStore // Store contract info key:codeHash@genesis, value: sensibleId@name@symbol@decimal
	contractFtGenesisStore       *storage.PebbleStore // Store contract genesis info key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
	contractFtGenesisOutputStore *storage.PebbleStore // Store used contract genesis output info key:usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
	contractFtGenesisUtxoStore   *storage.PebbleStore // Store contract genesis UTXO info key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}

	addressFtIncomeValidStore *storage.PebbleStore // Store address-related FT contract Utxo data key: FtAddress, value: CodeHash@Genesis@Amount@TxID@Index@Value@height,...
	uncheckFtOutpointStore    *storage.PebbleStore // Store unchecked FT contract Utxo data key: outpoint, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
	usedFtIncomeStore         *storage.PebbleStore // Store used FT contract Utxo data key: UsedtxID, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height,...

	uniqueFtIncomeStore *storage.PebbleStore // Store unique contract UTXO data key:codeHash@genesis, value: TxID@Index@Value@sensibleId@customData@height,...
	uniqueFtSpendStore  *storage.PebbleStore // Store unique contract UTXO data key:codeHash@genesis, value: TxID@Index@usedTxId,...

	invalidFtOutpointStore *storage.PebbleStore // Store invalid FT contract Utxo data key: outpoint, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height@reason,...

	metaStore   *storage.MetaStore // Store metadata
	mu          sync.RWMutex
	bar         *progressbar.ProgressBar
	params      config.IndexerParams
	mempoolMgr  FtMempoolManager
	mempoolInit bool // Whether mempool is initialized

	stopCh <-chan struct{}
}

var workers = 1
var batchSize = 1000

func NewContractFtIndexer(params config.IndexerParams,
	contractFtUtxoStore,
	addressFtIncomeStore,
	addressFtSpendStore,
	contractFtInfoStore,
	contractFtGenesisStore,
	contractFtGenesisOutputStore,
	contractFtGenesisUtxoStore *storage.PebbleStore,
	addressFtIncomeValidStore,
	uncheckFtOutpointStore,
	usedFtIncomeStore,
	uniqueFtIncomeStore,
	uniqueFtSpendStore,
	invalidFtOutpointStore *storage.PebbleStore,
	metaStore *storage.MetaStore) *ContractFtIndexer {
	return &ContractFtIndexer{
		params:                       params,
		contractFtUtxoStore:          contractFtUtxoStore,
		addressFtIncomeStore:         addressFtIncomeStore,
		addressFtSpendStore:          addressFtSpendStore,
		contractFtInfoStore:          contractFtInfoStore,
		contractFtGenesisStore:       contractFtGenesisStore,
		contractFtGenesisOutputStore: contractFtGenesisOutputStore,
		contractFtGenesisUtxoStore:   contractFtGenesisUtxoStore,
		addressFtIncomeValidStore:    addressFtIncomeValidStore,
		uncheckFtOutpointStore:       uncheckFtOutpointStore,
		usedFtIncomeStore:            usedFtIncomeStore,
		uniqueFtIncomeStore:          uniqueFtIncomeStore,
		uniqueFtSpendStore:           uniqueFtSpendStore,
		invalidFtOutpointStore:       invalidFtOutpointStore,
		metaStore:                    metaStore,
	}
}

func (i *ContractFtIndexer) InitProgressBar(totalBlocks, startHeight int) {
	remainingBlocks := totalBlocks - startHeight
	if remainingBlocks <= 0 {
		remainingBlocks = 1
	}
	i.bar = progressbar.NewOptions(remainingBlocks,
		progressbar.OptionSetWriter(colorable.NewColorableStdout()),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSetDescription("Indexing FT contract blocks..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionSetRenderBlankState(false),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(colorable.NewColorableStdout(), "\nDone!\n")
		}),
	)
}

func (i *ContractFtIndexer) IndexBlock(block *ContractFtBlock, updateHeight bool) error {
	if block == nil {
		return fmt.Errorf("cannot index nil block")
	}

	workers = i.params.WorkerCount
	batchSize = i.params.BatchSize

	if err := block.Validate(); err != nil {
		return fmt.Errorf("invalid block: %w", err)
	}

	// Add timer
	startTime := time.Now()
	// if lastLogTime.IsZero() {
	// 	lastLogTime = startTime
	// }
	txCount := len(block.Transactions)

	// Phase 1: Index all contract outputs
	if err := i.indexContractFtOutputs(block); err != nil {
		return fmt.Errorf("failed to index contract outputs: %w", err)
	}
	block.ContractFtOutputs = nil
	elapsed1 := time.Now().Sub(startTime)

	startTime2 := time.Now()
	// Phase 2: Process all contract inputs
	if err := i.processContractFtInputs(block); err != nil {
		return fmt.Errorf("failed to process contract inputs: %w", err)
	}
	block.Transactions = nil
	elapsed2 := time.Now().Sub(startTime2)

	// Check if log should be printed
	currentTime := time.Now()
	if lastLogTime.IsZero() || currentTime.Sub(lastLogTime) >= 5*time.Minute {
		log.Printf("[IndexBlock][%d] Completed indexContractFtOutputs, processed %d transactions, took: %v seconds", block.Height, txCount, elapsed1.Seconds())
		log.Printf("[IndexBlock][%d] Completed processContractFtInputs, processed %d transactions, total time: %v seconds", block.Height, txCount, elapsed2.Seconds())
		lastLogTime = currentTime
	}

	if !block.IsPartialBlock && updateHeight {
		heightStr := strconv.Itoa(block.Height)
		if err := i.metaStore.Set([]byte(common.MetaStoreKeyLastFtIndexedHeight), []byte(heightStr)); err != nil {
			return err
		}

		if err := i.metaStore.Sync(); err != nil {
			log.Printf("Failed to sync meta store: %v", err)
			return err
		}

		if i.bar != nil {
			i.bar.Add(1)
		}
	}

	block = nil
	return nil
}

func (i *ContractFtIndexer) indexContractFtOutputs(block *ContractFtBlock) error {
	txCount := len(block.Transactions)
	batchCount := (txCount + batchSize - 1) / batchSize

	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		start := batchIndex * batchSize
		end := start + batchSize
		if end > txCount {
			end = txCount
		}

		batchSize := end - start
		contractFtUtxoMap := make(map[string][]string, batchSize*3)
		addressFtUtxoMap := make(map[string][]string, batchSize)
		ftInfoMap := make(map[string]string, batchSize)
		genesisMap := make(map[string]string, batchSize)
		genesisUtxoMap := make(map[string]string, batchSize)
		uniqueFtIncomeMap := make(map[string][]string, batchSize)
		uncheckFtOutpointMap := make(map[string]string, batchSize)

		hasFt := false
		hasUnique := false
		for i := start; i < end; i++ {
			tx := block.Transactions[i]
			for _, out := range tx.Outputs {
				// Process contract UTXO storage
				//key: txID, value:FtAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType
				contractFtUtxoMap[tx.ID] = append(contractFtUtxoMap[tx.ID], common.ConcatBytesOptimized([]string{out.FtAddress, out.CodeHash, out.Genesis, out.SensibleId, out.Amount, strconv.Itoa(int(out.Index)), out.Value, strconv.FormatInt(out.Height, 10), out.ContractType}, "@"))

				if out.ContractType == "ft" {
					hasFt = true
					// Process FT info storage
					// key: codeHash@genesis, value: sensibleId@name@symobl@decimal
					ftInfoKey := common.ConcatBytesOptimized([]string{out.CodeHash, out.Genesis}, "@")
					if _, exists := ftInfoMap[ftInfoKey]; !exists {
						ftInfoMap[ftInfoKey] = common.ConcatBytesOptimized([]string{out.SensibleId, out.Name, out.Symbol, strconv.FormatUint(uint64(out.Decimal), 10)}, "@")
					}

					// Process initial genesis UTXO storage
					// key: outpoint, value: sensibleId@name@symobl@decimal@codeHash@genesis
					if out.SensibleId == "000000000000000000000000000000000000000000000000000000000000000000000000" {
						genesisKey := common.ConcatBytesOptimized([]string{tx.ID, strconv.Itoa(int(out.Index))}, ":")
						if _, exists := genesisMap[genesisKey]; !exists {
							genesisMap[genesisKey] = common.ConcatBytesOptimized([]string{out.SensibleId, out.Name, out.Symbol, strconv.FormatUint(uint64(out.Decimal), 10), out.CodeHash, out.Genesis}, "@")
						}
					}

					// Process new genesis UTXO records
					// key: outpoint, value: sensibleId@name@symobl@decimal@codeHash@genesis@amount@index@value
					if out.Amount == "0" {
						genesisUtxoKey := common.ConcatBytesOptimized([]string{tx.ID, strconv.Itoa(int(out.Index))}, ":")
						if _, exists := genesisUtxoMap[genesisUtxoKey]; !exists {
							genesisUtxoMap[genesisUtxoKey] = common.ConcatBytesOptimized([]string{out.SensibleId, out.Name, out.Symbol, strconv.FormatUint(uint64(out.Decimal), 10), out.CodeHash, out.Genesis, out.Amount, strconv.Itoa(int(out.Index)), out.Value}, "@")
						}
					}

					// Process address FT UTXO storage
					// key: FtAddress, value: CodeHash@Genesis@Amount@TxID@Index@Value
					if _, exists := addressFtUtxoMap[out.FtAddress]; !exists {
						addressFtUtxoMap[out.FtAddress] = make([]string, 0, 4)
					}
					addressFtUtxoMap[out.FtAddress] = append(addressFtUtxoMap[out.FtAddress], common.ConcatBytesOptimized([]string{out.CodeHash, out.Genesis, out.Amount, tx.ID, strconv.Itoa(int(out.Index)), out.Value, strconv.FormatInt(out.Height, 10)}, "@"))

					// Process unchecked FT contract Utxo storage
					// key: outpoint, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
					outpoint := common.ConcatBytesOptimized([]string{tx.ID, strconv.Itoa(int(out.Index))}, ":")
					if _, exists := uncheckFtOutpointMap[outpoint]; !exists {
						uncheckFtOutpointMap[outpoint] = common.ConcatBytesOptimized([]string{out.FtAddress, out.CodeHash, out.Genesis, out.SensibleId, out.Amount, tx.ID, strconv.Itoa(int(out.Index)), out.Value, strconv.FormatInt(out.Height, 10)}, "@")
					}

				} else if out.ContractType == "unique" {
					hasUnique = true
					codehashGenesisKey := common.ConcatBytesOptimized([]string{out.CodeHash, out.Genesis}, "@")
					// key: codeHash@genesis, value: TxID@Index@Value@sensibleId@customData@height
					uniqueFtIncomeMap[codehashGenesisKey] = append(uniqueFtIncomeMap[codehashGenesisKey], common.ConcatBytesOptimized([]string{tx.ID, strconv.Itoa(int(out.Index)), out.Value, out.SensibleId, out.CustomData, strconv.FormatInt(out.Height, 10)}, "@"))

				} else {
					continue
				}

			}
		}

		if hasFt {
			// Batch process various storages
			if err := i.contractFtUtxoStore.BulkMergeMapConcurrent(&contractFtUtxoMap, workers); err != nil {
				return err
			}

			if err := i.addressFtIncomeStore.BulkMergeMapConcurrent(&addressFtUtxoMap, workers); err != nil {
				return err
			}

			if err := i.contractFtInfoStore.BulkWriteConcurrent(&ftInfoMap, workers); err != nil {
				return err
			}

			if err := i.contractFtGenesisStore.BulkWriteConcurrent(&genesisMap, workers); err != nil {
				return err
			}

			if err := i.contractFtGenesisUtxoStore.BulkWriteConcurrent(&genesisUtxoMap, workers); err != nil {
				return err
			}

			if err := i.uncheckFtOutpointStore.BulkWriteConcurrent(&uncheckFtOutpointMap, workers); err != nil {
				return err
			}
		}

		if hasUnique {
			if err := i.uniqueFtIncomeStore.BulkMergeMapConcurrent(&uniqueFtIncomeMap, workers); err != nil {
				return err
			}
		}

		// Clean up memory
		for k := range contractFtUtxoMap {
			delete(contractFtUtxoMap, k)
		}
		for k := range addressFtUtxoMap {
			delete(addressFtUtxoMap, k)
		}
		for k := range ftInfoMap {
			delete(ftInfoMap, k)
		}
		for k := range genesisMap {
			delete(genesisMap, k)
		}
		for k := range genesisUtxoMap {
			delete(genesisUtxoMap, k)
		}
		for k := range uniqueFtIncomeMap {
			delete(uniqueFtIncomeMap, k)
		}
		for k := range uncheckFtOutpointMap {
			delete(uncheckFtOutpointMap, k)
		}
		contractFtUtxoMap = nil
		addressFtUtxoMap = nil
		ftInfoMap = nil
		genesisMap = nil
		genesisUtxoMap = nil
		uniqueFtIncomeMap = nil
		uncheckFtOutpointMap = nil
	}

	return nil
}

func (i *ContractFtIndexer) processContractFtInputs(block *ContractFtBlock) error {
	var allTxPoints []string
	var txPointUsedMap = make(map[string]string)
	// Query if all input points exist in contractFtGenesisUtxoStore
	var usedGenesisUtxoMap = make(map[string]string)

	// First collect all input points
	for _, tx := range block.Transactions {
		for _, in := range tx.Inputs {
			allTxPoints = append(allTxPoints, in.TxPoint)
			txPointUsedMap[in.TxPoint] = tx.ID
			// if in.TxPoint == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec:0" {
			// 	fmt.Printf("Found: txPoint: %s, txId: %s\n", in.TxPoint, tx.ID)
			// }
		}
	}

	for _, txPoint := range allTxPoints {
		value, err := i.contractFtGenesisUtxoStore.Get([]byte(txPoint))
		if err == nil {
			// Found genesis UTXO, record transaction outpoint
			usedGenesisUtxoMap[txPoint] = string(value)
			// if txPoint == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec:0" {
			// 	fmt.Printf("Found GenesisUtxo: txPoint: %s, value: %s\n", txPoint, string(value))
			// }
		}
	}

	totalPoints := len(allTxPoints)
	batchCount := (totalPoints + batchSize - 1) / batchSize

	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		start := batchIndex * batchSize
		end := start + batchSize
		if end > totalPoints {
			end = totalPoints
		}

		batchPoints := allTxPoints[start:end]
		addressFtResult, uniqueFtResult, err := i.contractFtUtxoStore.QueryFtUTXOAddresses(&batchPoints, workers, txPointUsedMap)
		if err != nil {
			return err
		}

		//Process addressFtSpendStore
		if err := i.addressFtSpendStore.BulkMergeMapConcurrent(&addressFtResult, workers); err != nil {
			return err
		}

		//Process uniqueFtSpendStore
		if err := i.uniqueFtSpendStore.BulkMergeMapConcurrent(&uniqueFtResult, workers); err != nil {
			return err
		}

		//Process usedFtIncomeStore
		usedFtIncomeMap := make(map[string][]string)
		// if block.Height == 65369 {
		// 	fmt.Println("block.Height == 65369")
		// 	for k, v := range addressFtResult {
		// 		fmt.Println("k: ", k)
		// 		fmt.Println("v: ", v)
		// 	}

		// 	fmt.Println("--------------------------------")
		// 	//Print txPointUsedMap
		// 	fmt.Println("len: block.Transactions: ", len(block.Transactions))
		// 	fmt.Println("batchPoints: ", batchPoints)
		// 	fmt.Println("--------------------------------")
		// 	fmt.Println("txPointUsedMap: ", txPointUsedMap)
		// 	fmt.Println("--------------------------------")

		// }
		for k, vList := range addressFtResult {
			for _, v := range vList {
				//k: FtAddress
				//v: txid@index@codeHash@genesis@sensibleId@amount@value@height@usedTxId
				vStrs := strings.Split(v, "@")
				if len(vStrs) != 9 {
					fmt.Println("Processing addressFtResult invalid vStrs: ", vStrs)
					continue
				}
				//newKey: usedTxId
				//newValue: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height,...
				usedTxId := vStrs[8]
				if _, exists := usedFtIncomeMap[usedTxId]; !exists {
					usedFtIncomeMap[usedTxId] = make([]string, 0)
				}
				newValue := common.ConcatBytesOptimized([]string{k, vStrs[2], vStrs[3], vStrs[4], vStrs[5], vStrs[0], vStrs[1], vStrs[6], vStrs[7]}, "@")
				usedFtIncomeMap[usedTxId] = append(usedFtIncomeMap[usedTxId], newValue)
			}
		}
		if err := i.usedFtIncomeStore.BulkMergeMapConcurrent(&usedFtIncomeMap, workers); err != nil {
			return err
		}

		// Process genesis UTXO consumption
		if len(usedGenesisUtxoMap) > 0 {
			genesisSpendMap := make(map[string]string)
			for txPoint, _ := range usedGenesisUtxoMap {
				// Get original UTXO info and add @IsSpent flag
				originalValue := usedGenesisUtxoMap[txPoint]
				genesisSpendMap[txPoint] = originalValue + "@1" // Add @IsSpent flag
			}
			if err := i.contractFtGenesisUtxoStore.BulkWriteConcurrent(&genesisSpendMap, workers); err != nil {
				return err
			}
		}

		// Process genesis output storage
		// key: txID, value: sensibleId@name@symobl@decimal@codeHash@genesis@amount@index@value,...
		// if len(usedGenesisUtxoMap) > 0 {
		// 	genesisOutputMap := make(map[string]string)
		// 	for usedTxPoint, _ := range usedGenesisUtxoMap {
		// 		if _, exists := txPointUsedMap[usedTxPoint]; !exists {
		// 			continue
		// 		}
		// 		txID := txPointUsedMap[usedTxPoint]
		// 		// if txID == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec" {
		// 		// 	fmt.Printf("Found: usedTxPoint: %s, txID: %s\n", usedTxPoint, txID)
		// 		// }
		// 		var tx *ContractFtTransaction
		// 		for _, v := range block.Transactions {
		// 			if v.ID == txID {
		// 				tx = v
		// 				break
		// 			}
		// 		}
		// 		if tx == nil {
		// 			continue
		// 		}

		// 		// Collect all output information for this transaction
		// 		var outputs []string
		// 		for x, out := range tx.Outputs {
		// 			//key: usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
		// 			outputInfo := common.ConcatBytesOptimized([]string{
		// 				out.SensibleId,
		// 				out.Name,
		// 				out.Symbol,
		// 				strconv.FormatUint(uint64(out.Decimal), 10),
		// 				out.CodeHash,
		// 				out.Genesis,
		// 				out.Amount,
		// 				txID,
		// 				strconv.Itoa(x),
		// 				out.Value,
		// 			}, "@")
		// 			outputs = append(outputs, outputInfo)
		// 		}
		// 		genesisOutputMap[usedTxPoint] = strings.Join(outputs, ",")
		// 		// if usedTxPoint == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec:0" {
		// 		// 	fmt.Printf("Found: usedTxPoint: %s, outputs: %s\n", usedTxPoint, strings.Join(outputs, ","))
		// 		// }
		// 	}
		// 	if err := i.contractFtGenesisOutputStore.BulkWriteConcurrent(&genesisOutputMap, workers); err != nil {
		// 		return err
		// 	}

		// 	for k := range genesisOutputMap {
		// 		delete(genesisOutputMap, k)
		// 	}
		// 	genesisOutputMap = nil
		// }

		if len(allTxPoints) > 0 {
			genesisOutputMap := make(map[string]string)
			for _, usedTxPoint := range allTxPoints {
				txID := txPointUsedMap[usedTxPoint]
				// if txID == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec" {
				// 	fmt.Printf("Found: usedTxPoint: %s, txID: %s\n", usedTxPoint, txID)
				// }
				var tx *ContractFtTransaction
				for _, v := range block.Transactions {
					if v.ID == txID {
						tx = v
						break
					}
				}
				if tx == nil {
					continue
				}

				// Collect all output information for this transaction
				var outputs []string
				for x, out := range tx.Outputs {
					//key: usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
					outputInfo := common.ConcatBytesOptimized([]string{
						out.SensibleId,
						out.Name,
						out.Symbol,
						strconv.FormatUint(uint64(out.Decimal), 10),
						out.CodeHash,
						out.Genesis,
						out.Amount,
						txID,
						strconv.Itoa(x),
						out.Value,
					}, "@")
					outputs = append(outputs, outputInfo)
				}
				genesisOutputMap[usedTxPoint] = strings.Join(outputs, ",")
			}
			if err := i.contractFtGenesisOutputStore.BulkWriteConcurrent(&genesisOutputMap, workers); err != nil {
				return err
			}

			for k := range genesisOutputMap {
				delete(genesisOutputMap, k)
			}
			genesisOutputMap = nil

		}

		for k := range addressFtResult {
			delete(addressFtResult, k)
		}
		for k := range uniqueFtResult {
			delete(uniqueFtResult, k)
		}
		for k := range usedFtIncomeMap {
			delete(usedFtIncomeMap, k)
		}

		addressFtResult = nil
		uniqueFtResult = nil
		usedFtIncomeMap = nil
		batchPoints = nil
	}

	for k := range txPointUsedMap {
		delete(txPointUsedMap, k)
	}
	txPointUsedMap = nil
	allTxPoints = nil
	usedGenesisUtxoMap = nil
	return nil
}

func (i *ContractFtIndexer) GetLastIndexedHeight() (int, error) {
	heightBytes, err := i.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
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

	return height, nil
}

type ContractFtBlock struct {
	Height            int                            `json:"height"`
	Transactions      []*ContractFtTransaction       `json:"transactions"`
	ContractFtOutputs map[string][]*ContractFtOutput `json:"contract_outputs"`
	IsPartialBlock    bool                           `json:"-"`
}

func (b *ContractFtBlock) Validate() error {
	if b.Height < 0 {
		return fmt.Errorf("invalid block height: %d", b.Height)
	}
	return nil
}

type ContractFtTransaction struct {
	ID      string
	Inputs  []*ContractFtInput
	Outputs []*ContractFtOutput
}

type ContractFtOutput struct {
	Address string
	Value   string
	Index   int64
	Height  int64

	ContractType string // ft, unique
	//FtInfo
	CodeHash   string
	Genesis    string
	SensibleId string
	Name       string
	Symbol     string
	Amount     string
	Decimal    uint8
	FtAddress  string

	CustomData string // unique custom data
}

type ContractFtInput struct {
	TxPoint string
}

// GetUtxoStore returns UTXO storage object
func (i *ContractFtIndexer) GetContractFtUtxoStore() *storage.PebbleStore {
	return i.contractFtUtxoStore
}

func (i *ContractFtIndexer) GetContractFtInfoStore() *storage.PebbleStore {
	return i.contractFtInfoStore
}

func (i *ContractFtIndexer) GetContractFtGenesisStore() *storage.PebbleStore {
	return i.contractFtGenesisStore
}

func (i *ContractFtIndexer) GetContractFtGenesisOutputStore() *storage.PebbleStore {
	return i.contractFtGenesisOutputStore
}

func (i *ContractFtIndexer) GetContractFtGenesisUtxoStore() *storage.PebbleStore {
	return i.contractFtGenesisUtxoStore
}

// SetMempoolManager sets mempool manager
func (i *ContractFtIndexer) SetMempoolManager(mempoolMgr FtMempoolManager) {
	i.mempoolMgr = mempoolMgr
}

// func (i *ContractFtIndexer) StartAll() error {
// 	// Mark as initialized
// 	if i.mempoolInit {
// 		log.Println("Mempool already started")
// 		return nil
// 	}
//
// 	// Start mempool
// 	log.Println("Starting ZMQ and mempool monitoring automatically...")
// 	err := i.mempoolMgr.StartMempool()
// 	if err != nil {
// 		return err
// 	}
//
// 	i.mempoolInit = true
// 	log.Println("Mempool manager started via API, monitoring new transactions...")
//
// 	// Initialize mempool data (load existing mempool transactions)
// 	go func() {
// 		log.Println("Starting FT mempool data initialization...")
// 		i.mempoolMgr.InitializeMempool(s.bcClient)
// 		log.Println("FT mempool data initialization completed")
// 	}()
//
// 	// Get current index height as cleanup start height
// 	lastIndexedHeightBytes, err := i.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
// 	if err == nil {
// 		// Set current height as cleanup start height to avoid cleaning historical blocks
// 		log.Println("Setting mempool cleanup start height to current index height:", string(lastIndexedHeightBytes))
// 		err = i.metaStore.Set([]byte("last_mempool_clean_height"), lastIndexedHeightBytes)
// 		if err != nil {
// 			log.Printf("Failed to set mempool cleanup start height: %v", err)
// 		}
// 	} else {
// 		log.Printf("Failed to get current index height: %v", err)
// 	}
//
// 	// Start mempool cleanup goroutine
// 	go func() {
// 		// Mempool cleanup interval
// 		cleanInterval := 10 * time.Second
//
// 		for {
// 			select {
// 			case <-i.stopCh:
// 				return
// 			case <-time.After(cleanInterval):
// 				// 1. Get last cleaned height
// 				lastCleanHeight := 0
// 				lastCleanHeightBytes, err := i.metaStore.Get([]byte("last_mempool_clean_height"))
// 				if err == nil {
// 					lastCleanHeight, _ = strconv.Atoi(string(lastCleanHeightBytes))
// 				}
//
// 				// 2. Get latest indexed height
// 				lastIndexedHeight := 0
// 				lastIndexedHeightBytes, err := i.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
// 				if err == nil {
// 					lastIndexedHeight, _ = strconv.Atoi(string(lastIndexedHeightBytes))
// 				}
//
// 				// 3. If latest indexed height is greater than last cleaned height, perform cleanup
// 				if lastIndexedHeight > lastCleanHeight {
// 					log.Printf("Performing mempool cleanup from height %d to %d", lastCleanHeight+1, lastIndexedHeight)
//
// 					// Perform cleanup for each new block
// 					for height := lastCleanHeight + 1; height <= lastIndexedHeight; height++ {
// 						err := i.mempoolMgr.CleanByHeight(height, s.bcClient)
// 						if err != nil {
// 							log.Printf("Failed to clean height %d: %v", height, err)
// 						}
// 					}
//
// 					// Update last cleaned height
// 					err := i.metaStore.Set([]byte("last_mempool_clean_height"), []byte(strconv.Itoa(lastIndexedHeight)))
// 					if err != nil {
// 						log.Printf("Failed to update last cleaned height: %v", err)
// 					}
// 				}
// 			}
// 		}
// 	}()
//
// 	return nil
// }

// GetInvalidFtOutpointStore returns invalid FT contract UTXO storage object
func (i *ContractFtIndexer) GetInvalidFtOutpointStore() *storage.PebbleStore {
	return i.invalidFtOutpointStore
}

// QueryInvalidFtOutpoint queries invalid FT contract UTXO data
// outpoint: transaction output point, format is "txID:index"
// returns: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height@reason
func (i *ContractFtIndexer) QueryInvalidFtOutpoint(outpoint string) (string, error) {
	value, err := i.invalidFtOutpointStore.Get([]byte(outpoint))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("failed to query invalid FT contract UTXO: %w", err)
	}
	return string(value), nil
}
