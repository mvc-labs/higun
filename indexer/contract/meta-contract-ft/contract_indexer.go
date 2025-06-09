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
	// 记录上一次打印日志的时间
	lastLogTime time.Time
)

type ContractFtIndexer struct {
	contractFtUtxoStore  *storage.PebbleStore // 存储合约Utxo数据 key: txID, value:FtAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType,...
	addressFtIncomeStore *storage.PebbleStore // 存储地址相关的FT合约Utxo数据  key: FtAddress, value: CodeHash@Genesis@Amount@TxID@Index@Value@height,...
	addressFtSpendStore  *storage.PebbleStore // 存储已使用的FT合约Utxo数据  key: FtAddress, value: txid@index@codeHash@genesis@sensibleId@amount@value@height@usedTxId,...

	contractFtInfoStore          *storage.PebbleStore // 存储合约信息 key:codeHash@genesis, value: sensibleId@name@symbol@decimal
	contractFtGenesisStore       *storage.PebbleStore // 存储合约创世信息 key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
	contractFtGenesisOutputStore *storage.PebbleStore // 存储使用合约创世输出信息 key:usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
	contractFtGenesisUtxoStore   *storage.PebbleStore // 存储合约创世UTXO信息 key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}

	addressFtIncomeValidStore *storage.PebbleStore // 存储地址相关的FT合约Utxo数据  key: FtAddress, value: CodeHash@Genesis@Amount@TxID@Index@Value@height,...
	uncheckFtOutpointStore    *storage.PebbleStore // 存储未检查的FT合约Utxo数据  key: outpoint, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
	usedFtIncomeStore         *storage.PebbleStore // 存储已使用的FT合约Utxo数据  key: UsedtxID, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height,...

	uniqueFtIncomeStore *storage.PebbleStore // 存储unique合约UTXO数据 key:codeHash@genesis, value: TxID@Index@Value@sensibleId@customData@height,...
	uniqueFtSpendStore  *storage.PebbleStore // 存储unique合约UTXO数据 key:codeHash@genesis, value: TxID@Index@usedTxId,...

	metaStore   *storage.MetaStore // 存储元数据
	mu          sync.RWMutex
	bar         *progressbar.ProgressBar
	params      config.IndexerParams
	mempoolMgr  FtMempoolManager
	mempoolInit bool // 内存池是否已初始化

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
	uniqueFtSpendStore *storage.PebbleStore,
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

	// 添加计时器
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

	// 检查是否需要打印日志
	currentTime := time.Now()
	if lastLogTime.IsZero() || currentTime.Sub(lastLogTime) >= 5*time.Minute {
		log.Printf("[IndexBlock][%d] 完成 indexContractFtOutputs, 处理 %d 交易, 耗时: %v 秒", block.Height, txCount, elapsed1.Seconds())
		log.Printf("[IndexBlock][%d] 完成 processContractFtInputs, 处理 %d 交易, 总耗时: %v 秒", block.Height, txCount, elapsed2.Seconds())
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
				// 处理合约UTXO存储
				//key: txID, value:FtAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType
				contractFtUtxoMap[tx.ID] = append(contractFtUtxoMap[tx.ID], common.ConcatBytesOptimized([]string{out.FtAddress, out.CodeHash, out.Genesis, out.SensibleId, out.Amount, strconv.Itoa(int(out.Index)), out.Value, strconv.FormatInt(out.Height, 10), out.ContractType}, "@"))

				if out.ContractType == "ft" {
					hasFt = true
					// 处理FT信息存储
					// key: codeHash@genesis, value: sensibleId@name@symobl@decimal
					ftInfoKey := common.ConcatBytesOptimized([]string{out.CodeHash, out.Genesis}, "@")
					if _, exists := ftInfoMap[ftInfoKey]; !exists {
						ftInfoMap[ftInfoKey] = common.ConcatBytesOptimized([]string{out.SensibleId, out.Name, out.Symbol, strconv.FormatUint(uint64(out.Decimal), 10)}, "@")
					}

					// 处理初始创世UTXO存储
					// key: outpoint, value: sensibleId@name@symobl@decimal@codeHash@genesis
					if out.SensibleId == "000000000000000000000000000000000000000000000000000000000000000000000000" {
						genesisKey := common.ConcatBytesOptimized([]string{tx.ID, strconv.Itoa(int(out.Index))}, ":")
						if _, exists := genesisMap[genesisKey]; !exists {
							genesisMap[genesisKey] = common.ConcatBytesOptimized([]string{out.SensibleId, out.Name, out.Symbol, strconv.FormatUint(uint64(out.Decimal), 10), out.CodeHash, out.Genesis}, "@")
						}
					}

					// 处理新创世UTXO记录
					// key: outpoint, value: sensibleId@name@symobl@decimal@codeHash@genesis@amount@index@value
					if out.Amount == "0" {
						genesisUtxoKey := common.ConcatBytesOptimized([]string{tx.ID, strconv.Itoa(int(out.Index))}, ":")
						if _, exists := genesisUtxoMap[genesisUtxoKey]; !exists {
							genesisUtxoMap[genesisUtxoKey] = common.ConcatBytesOptimized([]string{out.SensibleId, out.Name, out.Symbol, strconv.FormatUint(uint64(out.Decimal), 10), out.CodeHash, out.Genesis, out.Amount, strconv.Itoa(int(out.Index)), out.Value}, "@")
						}
					}

					// 处理地址FT UTXO存储
					// key: FtAddress, value: CodeHash@Genesis@Amount@TxID@Index@Value
					if _, exists := addressFtUtxoMap[out.FtAddress]; !exists {
						addressFtUtxoMap[out.FtAddress] = make([]string, 0, 4)
					}
					addressFtUtxoMap[out.FtAddress] = append(addressFtUtxoMap[out.FtAddress], common.ConcatBytesOptimized([]string{out.CodeHash, out.Genesis, out.Amount, tx.ID, strconv.Itoa(int(out.Index)), out.Value, strconv.FormatInt(out.Height, 10)}, "@"))

					// 处理未检查的FT合约Utxo存储
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
			// 批量处理各种存储
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

		// 清理内存
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
	// 查询所有输入点是否存在于contractFtGenesisUtxoStore
	var usedGenesisUtxoMap = make(map[string]string)

	// 首先收集所有输入点
	for _, tx := range block.Transactions {
		for _, in := range tx.Inputs {
			allTxPoints = append(allTxPoints, in.TxPoint)
			txPointUsedMap[in.TxPoint] = tx.ID
			// if in.TxPoint == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec:0" {
			// 	fmt.Printf("找到：txPoint: %s, txId: %s\n", in.TxPoint, tx.ID)
			// }
		}
	}

	for _, txPoint := range allTxPoints {
		value, err := i.contractFtGenesisUtxoStore.Get([]byte(txPoint))
		if err == nil {
			// 找到创世UTXO，记录交易outpoint
			usedGenesisUtxoMap[txPoint] = string(value)
			// if txPoint == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec:0" {
			// 	fmt.Printf("找到GenesisUtxo：txPoint: %s, value: %s\n", txPoint, string(value))
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

		//处理addressFtSpendStore
		if err := i.addressFtSpendStore.BulkMergeMapConcurrent(&addressFtResult, workers); err != nil {
			return err
		}

		//处理uniqueFtSpendStore
		if err := i.uniqueFtSpendStore.BulkMergeMapConcurrent(&uniqueFtResult, workers); err != nil {
			return err
		}

		//处理usedFtIncomeStore
		usedFtIncomeMap := make(map[string][]string)
		for k, vList := range addressFtResult {
			for _, v := range vList {
				//k: FtAddress
				//v: txid@index@codeHash@genesis@sensibleId@amount@value@height@usedTxId
				vStrs := strings.Split(v, "@")
				if len(vStrs) != 9 {
					fmt.Println("处理addressFtResult invalid vStrs: ", vStrs)
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

		// 处理创世UTXO的消费
		if len(usedGenesisUtxoMap) > 0 {
			genesisSpendMap := make(map[string]string)
			for txPoint, _ := range usedGenesisUtxoMap {
				// 获取原始UTXO信息并添加@IsSpent标记
				originalValue := usedGenesisUtxoMap[txPoint]
				genesisSpendMap[txPoint] = originalValue + "@1" // 添加@IsSpent标记
			}
			if err := i.contractFtGenesisUtxoStore.BulkWriteConcurrent(&genesisSpendMap, workers); err != nil {
				return err
			}
		}

		// 处理创世输出存储
		// key: txID, value: sensibleId@name@symobl@decimal@codeHash@genesis@amount@index@value,...
		// if len(usedGenesisUtxoMap) > 0 {
		// 	genesisOutputMap := make(map[string]string)
		// 	for usedTxPoint, _ := range usedGenesisUtxoMap {
		// 		if _, exists := txPointUsedMap[usedTxPoint]; !exists {
		// 			continue
		// 		}
		// 		txID := txPointUsedMap[usedTxPoint]
		// 		// if txID == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec" {
		// 		// 	fmt.Printf("找到：usedTxPoint: %s, txID: %s\n", usedTxPoint, txID)
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

		// 		// 收集该交易的所有输出信息
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
		// 		// 	fmt.Printf("找到：usedTxPoint: %s, outputs: %s\n", usedTxPoint, strings.Join(outputs, ","))
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
				// 	fmt.Printf("找到：usedTxPoint: %s, txID: %s\n", usedTxPoint, txID)
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

				// 收集该交易的所有输出信息
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
		for k := range txPointUsedMap {
			delete(txPointUsedMap, k)
		}
		addressFtResult = nil
		uniqueFtResult = nil
		usedFtIncomeMap = nil
		txPointUsedMap = nil
		batchPoints = nil
	}

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

// GetUtxoStore 返回UTXO存储对象
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

// SetMempoolManager 设置mempool管理器
func (i *ContractFtIndexer) SetMempoolManager(mempoolMgr FtMempoolManager) {
	i.mempoolMgr = mempoolMgr
}

// func (i *ContractFtIndexer) StartAll() error {
// 	// 检查是否已经初始化
// 	if i.mempoolInit {
// 		log.Println("内存池已经启动")
// 		return nil
// 	}

// 	// 启动内存池
// 	log.Println("通过自动启动ZMQ和内存池监听...")
// 	err := i.mempoolMgr.StartMempool()
// 	if err != nil {
// 		return err
// 	}

// 	// 标记为已初始化
// 	i.mempoolInit = true
// 	log.Println("内存池管理器已通过API启动，监听新交易...")

// 	// 初始化内存池数据（加载现有内存池交易）
// 	go func() {
// 		log.Println("开始初始化FT内存池数据...")
// 		i.mempoolMgr.InitializeMempool(s.bcClient)
// 		log.Println("FT内存池数据初始化完成")
// 	}()

// 	// 获取当前索引高度作为清理起始高度
// 	lastIndexedHeightBytes, err := i.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
// 	if err == nil {
// 		// 将当前高度设置为清理起始高度，避免清理历史区块
// 		log.Println("将内存池清理起始高度设置为当前索引高度:", string(lastIndexedHeightBytes))
// 		err = i.metaStore.Set([]byte("last_mempool_clean_height"), lastIndexedHeightBytes)
// 		if err != nil {
// 			log.Printf("设置内存池清理起始高度失败: %v", err)
// 		}
// 	} else {
// 		log.Printf("获取当前索引高度失败: %v", err)
// 	}

// 	// 启动内存池清理协程
// 	go func() {
// 		// 内存池清理间隔时间
// 		cleanInterval := 10 * time.Second

// 		for {
// 			select {
// 			case <-i.stopCh:
// 				return
// 			case <-time.After(cleanInterval):
// 				// 1. 获取最后清理的高度
// 				lastCleanHeight := 0
// 				lastCleanHeightBytes, err := i.metaStore.Get([]byte("last_mempool_clean_height"))
// 				if err == nil {
// 					lastCleanHeight, _ = strconv.Atoi(string(lastCleanHeightBytes))
// 				}

// 				// 2. 获取最新索引高度
// 				lastIndexedHeight := 0
// 				lastIndexedHeightBytes, err := i.metaStore.Get([]byte(common.MetaStoreKeyLastFtIndexedHeight))
// 				if err == nil {
// 					lastIndexedHeight, _ = strconv.Atoi(string(lastIndexedHeightBytes))
// 				}

// 				// 3. 如果最新索引高度大于最后清理高度，执行清理
// 				if lastIndexedHeight > lastCleanHeight {
// 					log.Printf("执行内存池清理，从高度 %d 到 %d", lastCleanHeight+1, lastIndexedHeight)

// 					// 对每个新块执行清理
// 					for height := lastCleanHeight + 1; height <= lastIndexedHeight; height++ {
// 						err := i.mempoolMgr.CleanByHeight(height, s.bcClient)
// 						if err != nil {
// 							log.Printf("清理高度 %d 失败: %v", height, err)
// 						}
// 					}

// 					// 更新最后清理高度
// 					err := i.metaStore.Set([]byte("last_mempool_clean_height"), []byte(strconv.Itoa(lastIndexedHeight)))
// 					if err != nil {
// 						log.Printf("更新最后清理高度失败: %v", err)
// 					}
// 				}
// 			}
// 		}
// 	}()

// 	return nil
// }
