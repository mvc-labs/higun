package mempool

import (
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/wire"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/storage"
)

// FtMempoolManager 管理FT内存池交易
type FtMempoolManager struct {
	contractFtUtxoStore       *storage.PebbleStore // 存储合约数据 key: txID, value:FtAddress@CodeHash@Genesis@Amount@Index@Value,...
	mempoolContractFtIncomeDB *storage.SimpleDB    // 内存池收入数据库 key: outpoint+address   value:CodeHash@Genesis@Amount@Value
	mempoolContractFtSpendDB  *storage.SimpleDB    // 内存池支出数据库 key: outpoint+address   value:CodeHash@Genesis@Amount@Value

	contractFtInfoStore                 *storage.PebbleStore // 存储合约信息 key:codeHash@genesis, value: sensibleId@name@symbol@decimal
	contractFtGenesisStore              *storage.PebbleStore // 存储合约创世信息 key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
	contractFtGenesisOutputStore        *storage.PebbleStore // 存储使用合约创世输出信息 key:txID, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value,...
	contractFtGenesisUtxoStore          *storage.PebbleStore // 存储合约创世UTXO信息 key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}
	mempoolContractFtInfoStore          *storage.SimpleDB    // 内存池合约信息数据库
	mempoolContractFtGenesisStore       *storage.SimpleDB    // 内存池合约创世信息数据库
	mempoolContractFtGenesisOutputStore *storage.SimpleDB    // 内存池合约创世输出信息数据库
	mempoolContractFtGenesisUtxoStore   *storage.SimpleDB    // 内存池合约创世UTXO信息数据库

	chainCfg  *chaincfg.Params
	zmqClient *ZMQClient
	basePath  string // 数据目录基础路径
}

// NewFtMempoolManager 创建一个新的FT内存池管理器
func NewFtMempoolManager(basePath string,
	contractFtUtxoStore *storage.PebbleStore,
	contractFtInfoStore *storage.PebbleStore,
	contractFtGenesisStore *storage.PebbleStore,
	contractFtGenesisOutputStore *storage.PebbleStore,
	contractFtGenesisUtxoStore *storage.PebbleStore,
	chainCfg *chaincfg.Params, zmqAddress string) *FtMempoolManager {
	// 创建内存池数据库
	mempoolContractFtIncomeDB, err := storage.NewSimpleDB(basePath + "/mempool_address_ft_income")
	if err != nil {
		log.Printf("创建FT内存池收入数据库失败: %v", err)
		return nil
	}

	mempoolContractFtSpendDB, err := storage.NewSimpleDB(basePath + "/mempool_address_ft_spend")
	if err != nil {
		log.Printf("创建FT内存池支出数据库失败: %v", err)
		mempoolContractFtIncomeDB.Close()
		return nil
	}

	mempoolContractFtInfoStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_info")
	if err != nil {
		log.Printf("创建FT内存池信息数据库失败: %v", err)
		mempoolContractFtIncomeDB.Close()
		mempoolContractFtSpendDB.Close()
		return nil
	}

	mempoolContractFtGenesisStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_genesis")
	if err != nil {
		log.Printf("创建FT内存池创世数据库失败: %v", err)
		mempoolContractFtIncomeDB.Close()
		mempoolContractFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		return nil
	}

	mempoolContractFtGenesisOutputStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_genesis_output")
	if err != nil {
		log.Printf("创建FT内存池创世输出数据库失败: %v", err)
		mempoolContractFtIncomeDB.Close()
		mempoolContractFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		return nil
	}

	mempoolContractFtGenesisUtxoStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_genesis_utxo")
	if err != nil {
		log.Printf("创建FT内存池创世UTXO数据库失败: %v", err)
		mempoolContractFtIncomeDB.Close()
		mempoolContractFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		return nil
	}

	m := &FtMempoolManager{
		contractFtUtxoStore:                 contractFtUtxoStore,
		mempoolContractFtIncomeDB:           mempoolContractFtIncomeDB,
		mempoolContractFtSpendDB:            mempoolContractFtSpendDB,
		mempoolContractFtInfoStore:          mempoolContractFtInfoStore,
		mempoolContractFtGenesisStore:       mempoolContractFtGenesisStore,
		mempoolContractFtGenesisOutputStore: mempoolContractFtGenesisOutputStore,
		mempoolContractFtGenesisUtxoStore:   mempoolContractFtGenesisUtxoStore,
		contractFtInfoStore:                 contractFtInfoStore,
		contractFtGenesisStore:              contractFtGenesisStore,
		contractFtGenesisOutputStore:        contractFtGenesisOutputStore,
		contractFtGenesisUtxoStore:          contractFtGenesisUtxoStore,
		chainCfg:                            chainCfg,
		basePath:                            basePath,
	}

	// 创建ZMQ客户端
	m.zmqClient = NewZMQClient(zmqAddress, nil)

	// 添加"rawtx"主题监听
	m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)

	return m
}

// Start 启动FT内存池管理器
func (m *FtMempoolManager) Start() error {
	return m.zmqClient.Start()
}

// Stop 停止FT内存池管理器
func (m *FtMempoolManager) Stop() {
	m.zmqClient.Stop()
	if m.mempoolContractFtIncomeDB != nil {
		m.mempoolContractFtIncomeDB.Close()
	}
	if m.mempoolContractFtSpendDB != nil {
		m.mempoolContractFtSpendDB.Close()
	}
	if m.mempoolContractFtInfoStore != nil {
		m.mempoolContractFtInfoStore.Close()
	}
	if m.mempoolContractFtGenesisStore != nil {
		m.mempoolContractFtGenesisStore.Close()
	}
	if m.mempoolContractFtGenesisOutputStore != nil {
		m.mempoolContractFtGenesisOutputStore.Close()
	}
	if m.mempoolContractFtGenesisUtxoStore != nil {
		m.mempoolContractFtGenesisUtxoStore.Close()
	}
}

// HandleRawTransaction 处理原始交易数据
func (m *FtMempoolManager) HandleRawTransaction(topic string, data []byte) error {
	// 1. 解析原始交易
	tx, err := DeserializeTransaction(data)
	if err != nil {
		return fmt.Errorf("解析交易失败: %w", err)
	}

	// 2. 处理交易输出，创建新的FT UTXO
	err = m.processFtOutputs(tx)
	if err != nil {
		return fmt.Errorf("处理FT交易输出失败: %w", err)
	}

	// 3. 处理交易输入，标记花费的FT UTXO
	err = m.processFtInputs(tx)
	if err != nil {
		return fmt.Errorf("处理FT交易输入失败: %w", err)
	}

	return nil
}

// processFtOutputs 处理FT交易输出，创建新的UTXO
func (m *FtMempoolManager) processFtOutputs(tx *wire.MsgTx) error {
	txHash := tx.TxHash().String()
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txHash, _ = blockchain.GetNewHash(tx)
	}

	// 处理每个输出
	for i, out := range tx.TxOut {
		pkScriptStr := hex.EncodeToString(out.PkScript)

		ftInfo, uniqueUtxoInfo, contractTypeStr, err := blockchain.ParseContractFtInfo(pkScriptStr, m.chainCfg)
		if err != nil {
			fmt.Println("ParseFtInfo error", err)
			return nil
		}
		_ = uniqueUtxoInfo
		_ = contractTypeStr
		if ftInfo == nil {
			continue
		}

		// 为每个地址创建FT UTXO索引
		outputIndex := strconv.Itoa(i)
		utxoID := txHash + ":" + outputIndex
		ftAddress := ftInfo.Address
		value := strconv.FormatInt(out.Value, 10)

		// 存储FT UTXO信息，格式：CodeHash@Genesis@Amount@Value
		ftAmount := strconv.FormatUint(ftInfo.Amount, 10)
		mempoolFtUtxo := common.ConcatBytesOptimized([]string{ftInfo.CodeHash, ftInfo.Genesis, ftAmount, value}, "@")
		//codeHash@genesis@ftAmount@value
		err = m.mempoolContractFtIncomeDB.AddRecord(utxoID, ftAddress, []byte(mempoolFtUtxo))
		if err != nil {
			log.Printf("[Mempool] 存储FT内存池UTXO索引失败 %s -> %s: %v", utxoID, ftAddress, err)
			continue
		}

		// 1. 处理FT信息存储
		ftInfoKey := common.ConcatBytesOptimized([]string{ftInfo.CodeHash, ftInfo.Genesis}, "@")
		// 先检查主存储中是否存在
		_, err = m.contractFtInfoStore.Get([]byte(ftInfoKey))
		if err == storage.ErrNotFound {
			// 主存储中不存在，添加到内存池存储
			ftInfoValue := common.ConcatBytesOptimized([]string{ftInfo.SensibleId, ftInfo.Name, ftInfo.Symbol, strconv.FormatUint(uint64(ftInfo.Decimal), 10)}, "@")
			err = m.mempoolContractFtInfoStore.AddRecord(ftInfoKey, "", []byte(ftInfoValue))
			if err != nil {
				log.Printf("[Mempool] 存储FT内存池信息失败 %s: %v", ftInfoKey, err)
			}
		}

		// 2. 处理创世信息存储
		if ftInfo.SensibleId == "000000000000000000000000000000000000000000000000000000000000000000000000" {
			genesisKey := common.ConcatBytesOptimized([]string{txHash, outputIndex}, ":")
			// 先检查主存储中是否存在
			_, err = m.contractFtGenesisStore.Get([]byte(genesisKey))
			if err == storage.ErrNotFound {
				// 主存储中不存在，添加到内存池存储
				genesisValue := common.ConcatBytesOptimized([]string{ftInfo.SensibleId, ftInfo.Name, ftInfo.Symbol, strconv.FormatUint(uint64(ftInfo.Decimal), 10), ftInfo.CodeHash, ftInfo.Genesis}, "@")
				err = m.mempoolContractFtGenesisStore.AddRecord(genesisKey, "", []byte(genesisValue))
				if err != nil {
					log.Printf("[Mempool] 存储FT内存池创世信息失败 %s: %v", genesisKey, err)
				}
			}
		}

		// 3. 处理创世UTXO存储
		if ftInfo.Amount == 0 {
			genesisUtxoKey := common.ConcatBytesOptimized([]string{txHash, outputIndex}, ":")
			// 先检查主存储中是否存在
			_, err = m.contractFtGenesisUtxoStore.Get([]byte(genesisUtxoKey))
			if err == storage.ErrNotFound {
				// 主存储中不存在，添加到内存池存储
				genesisUtxoValue := common.ConcatBytesOptimized([]string{
					ftInfo.SensibleId,
					ftInfo.Name,
					ftInfo.Symbol,
					strconv.FormatUint(uint64(ftInfo.Decimal), 10),
					ftInfo.CodeHash,
					ftInfo.Genesis,
					ftAmount,
					outputIndex,
					value,
				}, "@")
				err = m.mempoolContractFtGenesisUtxoStore.AddRecord(genesisUtxoKey, "", []byte(genesisUtxoValue))
				if err != nil {
					log.Printf("[Mempool] 存储FT内存池创世UTXO失败 %s: %v", genesisUtxoKey, err)
				}
			}
		}
	}

	return nil
}

// processFtInputs 处理FT交易输入
func (m *FtMempoolManager) processFtInputs(tx *wire.MsgTx) error {
	// 跳过挖矿交易
	if IsCoinbaseTx(tx) {
		return nil
	}

	// 用于存储创世交易ID和其输出信息
	var usedGenesisUtxoMap = make(map[string]string)
	var allTxPoints []string

	// 首先收集所有输入点
	for _, in := range tx.TxIn {
		prevTxHash := in.PreviousOutPoint.Hash.String()
		prevOutputIndex := strconv.Itoa(int(in.PreviousOutPoint.Index))
		spentUtxoID := prevTxHash + ":" + prevOutputIndex
		allTxPoints = append(allTxPoints, spentUtxoID)

		var ftUtxoAddress string
		var ftUtxoCodeHash string
		var ftUtxoGenesis string
		var ftUtxoAmount string
		var ftUtxoIndex string
		var ftUtxoValue string
		_ = ftUtxoIndex

		//fmt.Println(spentUtxoID)
		// 直接从主UTXO存储获取数据
		utxoData, err := m.contractFtUtxoStore.Get([]byte(prevTxHash))
		if err == nil {
			// 处理开头的逗号
			utxoStr := string(utxoData)
			//fmt.Println("主库", utxoStr)
			if len(utxoStr) > 0 && utxoStr[0] == ',' {
				utxoStr = utxoStr[1:]
			}
			parts := strings.Split(utxoStr, ",")
			if len(parts) > int(in.PreviousOutPoint.Index) {

				ftUtxoPart := ""
				for _, part := range parts {
					//FtAddress@CodeHash@Genesis@Amount@Index@Value
					outputInfo := strings.Split(part, "@")
					// 解析地址和金额
					if len(outputInfo) >= 2 {
						if outputInfo[4] == strconv.Itoa(int(in.PreviousOutPoint.Index)) {
							ftUtxoPart = part
							break
						}
					}
				}
				if ftUtxoPart == "" {
					continue
				}
				outputInfo := strings.Split(ftUtxoPart, "@")

				// 获取地址
				ftUtxoAddress = outputInfo[0]
				ftUtxoCodeHash = outputInfo[1]
				ftUtxoGenesis = outputInfo[2]
				ftUtxoAmount = outputInfo[3]
				ftUtxoIndex = outputInfo[4]
				ftUtxoValue = outputInfo[5]

				// 获取金额
				// if amount, err := strconv.ParseUint(outputInfo[1], 10, 64); err == nil {
				// 	utxoAmount = amount
				// }
			}
			//fmt.Println("主库", utxoAddress, utxoAmount)
		} else if err == storage.ErrNotFound {
			// 主UTXO存储中没找到，尝试从内存池收入数据库查找
			//CodeHash@Genesis@Amount@Value
			ftUtxoAddress, ftUtxoCodeHash, ftUtxoGenesis, ftUtxoAmount, ftUtxoValue, err = m.mempoolContractFtIncomeDB.GetByFtUTXO(spentUtxoID)
			if err != nil {
				continue
			}
		} else {
			continue
		}
		// 记录到内存池支出数据库
		// 记录到内存池支出数据库，包含金额信息
		//CodeHash@Genesis@Amount@Value
		mempoolFtUtxo := common.ConcatBytesOptimized([]string{ftUtxoCodeHash, ftUtxoGenesis, ftUtxoAmount, ftUtxoValue}, "@")
		err = m.mempoolContractFtSpendDB.AddRecord(spentUtxoID, ftUtxoAddress, []byte(mempoolFtUtxo))
		//fmt.Println("存储花费：", spentUtxoID, utxoAddress, utxoAmount, err)
		if err != nil {
			continue
		}
	}

	// 查询所有输入点是否存在于contractFtGenesisUtxoStore
	for _, txPoint := range allTxPoints {
		value, err := m.contractFtGenesisUtxoStore.Get([]byte(txPoint))
		if err == nil {
			// 找到创世UTXO，记录交易outpoint
			usedGenesisUtxoMap[txPoint] = string(value)
		}

		// 检查内存池中是否存在
		mempoolValue, err := m.mempoolContractFtGenesisUtxoStore.Get(txPoint)
		if err == nil {
			// 如果是创世UTXO，记录交易ID
			usedGenesisUtxoMap[txPoint] = string(mempoolValue)
		}

		//// 从UTXO数据库中获取数据
		//_, _, err = m.mempoolContractFtIncomeDB.GetByUTXO(txPoint)
		//if err != nil {
		//	continue
		//}
		//
		//// 从地址数据库中获取数据
		//addr, _, err := m.mempoolContractFtSpendDB.GetByUTXO(txPoint)
		//if err != nil {
		//	continue
		//}
		//
		//// 删除UTXO记录
		//if err := m.mempoolContractFtIncomeDB.DeleteRecord(txPoint, ""); err != nil {
		//	continue
		//}
		//
		//// 删除支出记录
		//if err := m.mempoolContractFtSpendDB.DeleteSpendRecord(addr); err != nil {
		//	continue
		//}
	}

	// 处理创世UTXO的消费
	if len(usedGenesisUtxoMap) > 0 {
		for txPoint, value := range usedGenesisUtxoMap {
			// 获取原始UTXO信息并添加@IsSpent标记
			spentValue := value + "@1" // 添加@IsSpent标记
			if err := m.mempoolContractFtGenesisUtxoStore.AddRecord(txPoint, "", []byte(spentValue)); err != nil {
				log.Printf("[Mempool] 更新FT内存池创世UTXO状态失败 %s: %v", txPoint, err)
			}

		}
	}

	// 处理创世输出存储
	if len(usedGenesisUtxoMap) > 0 {
		txHash := tx.TxHash().String()
		if config.GlobalConfig.RPC.Chain == "mvc" {
			txHash, _ = blockchain.GetNewHash(tx)
		}

		hasGenesisUtxo := false
		for txPoint, _ := range usedGenesisUtxoMap {
			txID := strings.Split(txPoint, ":")[0]
			if txID == txHash {
				hasGenesisUtxo = true
				break
			}
		}
		if !hasGenesisUtxo {
			return nil
		}

		// 收集该交易的所有输出信息
		var outputs []string
		for i, out := range tx.TxOut {
			pkScriptStr := hex.EncodeToString(out.PkScript)
			ftInfo, _, _, err := blockchain.ParseContractFtInfo(pkScriptStr, m.chainCfg)
			if err != nil || ftInfo == nil {
				continue
			}

			outputInfo := common.ConcatBytesOptimized([]string{
				ftInfo.SensibleId,
				ftInfo.Name,
				ftInfo.Symbol,
				strconv.FormatUint(uint64(ftInfo.Decimal), 10),
				ftInfo.CodeHash,
				ftInfo.Genesis,
				strconv.FormatUint(ftInfo.Amount, 10),
				strconv.Itoa(i),
				strconv.FormatInt(out.Value, 10),
			}, "@")
			outputs = append(outputs, outputInfo)
		}

		if len(outputs) > 0 {
			outputValue := strings.Join(outputs, ",")
			if err := m.mempoolContractFtGenesisOutputStore.AddRecord(txHash, "", []byte(outputValue)); err != nil {
				log.Printf("[Mempool] 存储FT内存池创世输出失败 %s: %v", txHash, err)
			}
		}
	}

	return nil
}

// ProcessNewBlockTxs 处理新区块中的FT交易，清理内存池记录
func (m *FtMempoolManager) ProcessNewBlockTxs(incomeUtxoList []common.FtUtxo, spendOutpointList []string) error {
	if len(incomeUtxoList) == 0 {
		return nil
	}

	// 删除income
	for _, utxo := range incomeUtxoList {
		err := m.mempoolContractFtIncomeDB.DeleteRecord(utxo.UtxoId, utxo.Address)
		if err != nil {
			log.Printf("删除FT内存池收入记录失败 %s: %v", utxo.TxID, err)
		}

		// 检查并删除FT信息存储中的记录
		parts := strings.Split(utxo.TxID, ":")
		if len(parts) == 2 {
			outpoint := common.ConcatBytesOptimized([]string{parts[0], parts[1]}, ":")

			// 检查并删除创世信息存储中的记录
			genesisKey := outpoint
			_, err = m.mempoolContractFtGenesisStore.Get(genesisKey)
			if err == nil {
				if err := m.mempoolContractFtGenesisStore.DeleteRecord(genesisKey, ""); err != nil {
					log.Printf("删除FT内存池创世信息失败 %s: %v", genesisKey, err)
				}
			}

			// 检查并删除创世输出存储中的记录
			genesisOutputKey := parts[0]
			_, err = m.mempoolContractFtGenesisOutputStore.Get(genesisOutputKey)
			if err == nil {
				if err := m.mempoolContractFtGenesisOutputStore.DeleteRecord(genesisOutputKey, ""); err != nil {
					log.Printf("删除FT内存池创世输出失败 %s: %v", genesisOutputKey, err)
				}
			}
		}
	}

	for _, utxo := range incomeUtxoList {
		codeHashGenesis := common.ConcatBytesOptimized([]string{utxo.CodeHash, utxo.Genesis}, "@")
		err := m.mempoolContractFtInfoStore.DeleteRecord(codeHashGenesis, "")
		if err != nil {
			log.Printf("删除FT内存池创世信息失败 %s: %v", utxo.CodeHash+"@"+utxo.Genesis, err)
		}
	}

	// 删除spend
	for _, outpoint := range spendOutpointList {
		err := m.mempoolContractFtSpendDB.DeleteSpendRecord(outpoint)
		if err != nil {
			log.Printf("删除FT内存池支出记录失败 %s: %v", outpoint, err)
		}
	}
	return nil
}

// CleanByHeight 通过区块高度清理FT内存池记录
func (m *FtMempoolManager) CleanByHeight(height int, bcClient interface{}) error {
	log.Printf("开始清理FT内存池，处理到区块高度: %d", height)

	// 尝试断言bcClient为blockchain.Client类型
	client, ok := bcClient.(*blockchain.Client)
	if !ok {
		return fmt.Errorf("不支持的区块链客户端类型")
	}

	// 获取该高度的区块哈希
	blockHash, err := client.GetBlockHash(int64(height))
	if err != nil {
		return fmt.Errorf("获取区块哈希失败: %w", err)
	}

	// 获取区块详细信息
	block, err := client.GetBlock(blockHash)
	if err != nil {
		return fmt.Errorf("获取区块信息失败: %w", err)
	}

	// 提取incomeUtxo列表
	var incomeFtUtxoList []common.FtUtxo = make([]common.FtUtxo, 0)
	var spendOutpointList []string
	for _, tx := range block.Tx {
		for _, in := range tx.Vin {
			preTxId := in.Txid
			if preTxId == "" {
				preTxId = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			spendOutpointList = append(spendOutpointList, common.ConcatBytesOptimized([]string{preTxId, strconv.Itoa(int(in.Vout))}, ":"))
		}

		for k, out := range tx.Vout {
			// 由于ParseFtOutput未定义，暂时跳过FT输出判断
			address := blockchain.GetAddressFromScript(out.ScriptPubKey.Hex, nil, m.chainCfg, config.GlobalConfig.RPC.Chain)
			amount := strconv.FormatInt(int64(math.Round(out.Value*1e8)), 10)

			// 解析FT相关信息
			ftInfo, _, _, err := blockchain.ParseContractFtInfo(out.ScriptPubKey.Hex, m.chainCfg)
			if err != nil {
				return nil
			}

			if ftInfo == nil {
				continue
			}

			ftUtxo := &common.FtUtxo{
				UtxoId:   common.ConcatBytesOptimized([]string{tx.Txid, strconv.Itoa(k)}, ":"),
				TxID:     tx.Txid,
				Address:  address,
				Value:    amount,
				CodeHash: ftInfo.CodeHash,
				Genesis:  ftInfo.Genesis,
				// SensibleId: ftInfo.SensibleId,
				// Name:       ftInfo.Name,
				// Symbol:     ftInfo.Symbol,
				Amount: strconv.FormatUint(ftInfo.Amount, 10),
				// Decimal:    strconv.FormatUint(uint64(ftInfo.Decimal), 10),
			}

			incomeFtUtxoList = append(incomeFtUtxoList, *ftUtxo)
		}
	}

	// 清理内存池记录
	return m.ProcessNewBlockTxs(incomeFtUtxoList, spendOutpointList)
}

// InitializeMempool 在启动时从节点获取当前所有内存池交易并处理
func (m *FtMempoolManager) InitializeMempool(bcClient interface{}) {
	// 使用单独的goroutine执行，避免阻塞主程序
	go func() {
		log.Printf("开始初始化FT内存池数据...")

		// 断言为blockchain.Client
		client, ok := bcClient.(*blockchain.Client)
		if !ok {
			log.Printf("初始化FT内存池失败: 不支持的区块链客户端类型")
			return
		}

		// 获取内存池中的所有交易ID
		txids, err := client.GetRawMempool()
		if err != nil {
			log.Printf("获取内存池交易列表失败: %v", err)
			return
		}

		log.Printf("从节点获取到 %d 个内存池交易，开始处理...", len(txids))

		// 分批处理交易，每批处理100个，避免一次性占用太多内存
		batchSize := 100
		totalBatches := (len(txids) + batchSize - 1) / batchSize

		for batchIdx := 0; batchIdx < totalBatches; batchIdx++ {
			start := batchIdx * batchSize
			end := start + batchSize
			if end > len(txids) {
				end = len(txids)
			}

			// 处理当前批次
			currentBatch := txids[start:end]
			log.Printf("处理FT内存池交易批次 %d/%d (%d 个交易)", batchIdx+1, totalBatches, len(currentBatch))

			for _, txid := range currentBatch {
				// 获取交易详情
				tx, err := client.GetRawTransaction(txid)
				if err != nil {
					log.Printf("获取交易详情失败 %s: %v", txid, err)
					continue
				}

				// 使用现有的交易处理方法处理交易
				msgTx := tx.MsgTx()

				// 先处理输出（创建新的UTXO）
				if err := m.processFtOutputs(msgTx); err != nil {
					log.Printf("处理FT交易输出失败 %s: %v", txid, err)
					continue
				}

				// 再处理输入（标记花费的UTXO）
				if err := m.processFtInputs(msgTx); err != nil {
					log.Printf("处理FT交易输入失败 %s: %v", txid, err)
					continue
				}
			}

			// 批次处理完毕，暂停一小段时间让其他程序有机会执行
			time.Sleep(10 * time.Millisecond)
		}

		log.Printf("FT内存池数据初始化完成，共处理 %d 个交易", len(txids))
	}()
}

// CleanAllMempool 清理所有FT内存池数据，用于完全重建
func (m *FtMempoolManager) CleanAllMempool() error {
	log.Println("正在通过删除物理文件重置FT内存池数据...")

	// 保存ZMQ地址，后面需要重建
	zmqAddress := ""
	if m.zmqClient != nil {
		zmqAddress = m.zmqClient.address
	}

	// 使用basePath和固定表名获取数据库文件路径
	incomeDbPath := m.basePath + "/mempool_ft_income"
	spendDbPath := m.basePath + "/mempool_ft_spend"
	infoDbPath := m.basePath + "/mempool_ft_info"
	genesisDbPath := m.basePath + "/mempool_ft_genesis"
	genesisOutputDbPath := m.basePath + "/mempool_ft_genesis_output"
	genesisUtxoDbPath := m.basePath + "/mempool_ft_genesis_utxo"

	// 不再尝试检测数据库状态，直接使用defer和recover处理可能的panic
	defer func() {
		if r := recover(); r != nil {
			log.Printf("清理过程中捕获到异常: %v，继续执行文件删除", r)
		}
	}()

	// 安全关闭数据库连接
	log.Println("关闭现有FT内存池数据库连接...")
	// 使用recover避免重复关闭导致的panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭收入数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolContractFtIncomeDB != nil {
			m.mempoolContractFtIncomeDB.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭支出数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolContractFtSpendDB != nil {
			m.mempoolContractFtSpendDB.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭信息数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolContractFtInfoStore != nil {
			m.mempoolContractFtInfoStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭创世数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolContractFtGenesisStore != nil {
			m.mempoolContractFtGenesisStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭创世输出数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolContractFtGenesisOutputStore != nil {
			m.mempoolContractFtGenesisOutputStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭创世UTXO数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolContractFtGenesisUtxoStore != nil {
			m.mempoolContractFtGenesisUtxoStore.Close()
		}
	}()

	// 删除物理文件
	log.Printf("删除FT内存池收入数据库: %s", incomeDbPath)
	if err := os.RemoveAll(incomeDbPath); err != nil {
		log.Printf("删除FT内存池收入数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池支出数据库: %s", spendDbPath)
	if err := os.RemoveAll(spendDbPath); err != nil {
		log.Printf("删除FT内存池支出数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池信息数据库: %s", infoDbPath)
	if err := os.RemoveAll(infoDbPath); err != nil {
		log.Printf("删除FT内存池信息数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池创世数据库: %s", genesisDbPath)
	if err := os.RemoveAll(genesisDbPath); err != nil {
		log.Printf("删除FT内存池创世数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池创世输出数据库: %s", genesisOutputDbPath)
	if err := os.RemoveAll(genesisOutputDbPath); err != nil {
		log.Printf("删除FT内存池创世输出数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池创世UTXO数据库: %s", genesisUtxoDbPath)
	if err := os.RemoveAll(genesisUtxoDbPath); err != nil {
		log.Printf("删除FT内存池创世UTXO数据库失败: %v", err)
		return err
	}

	// 重新创建数据库
	log.Println("重新创建FT内存池数据库...")
	mempoolContractFtIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
	if err != nil {
		log.Printf("重新创建FT内存池收入数据库失败: %v", err)
		return err
	}

	mempoolContractFtSpendDB, err := storage.NewSimpleDB(spendDbPath)
	if err != nil {
		mempoolContractFtIncomeDB.Close()
		log.Printf("重新创建FT内存池支出数据库失败: %v", err)
		return err
	}

	mempoolContractFtInfoStore, err := storage.NewSimpleDB(infoDbPath)
	if err != nil {
		mempoolContractFtIncomeDB.Close()
		mempoolContractFtSpendDB.Close()
		log.Printf("重新创建FT内存池信息数据库失败: %v", err)
		return err
	}

	mempoolContractFtGenesisStore, err := storage.NewSimpleDB(genesisDbPath)
	if err != nil {
		mempoolContractFtIncomeDB.Close()
		mempoolContractFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		log.Printf("重新创建FT内存池创世数据库失败: %v", err)
		return err
	}

	mempoolContractFtGenesisOutputStore, err := storage.NewSimpleDB(genesisOutputDbPath)
	if err != nil {
		mempoolContractFtIncomeDB.Close()
		mempoolContractFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		log.Printf("重新创建FT内存池创世输出数据库失败: %v", err)
		return err
	}

	mempoolContractFtGenesisUtxoStore, err := storage.NewSimpleDB(genesisUtxoDbPath)
	if err != nil {
		mempoolContractFtIncomeDB.Close()
		mempoolContractFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		log.Printf("重新创建FT内存池创世UTXO数据库失败: %v", err)
		return err
	}

	// 更新数据库引用
	m.mempoolContractFtIncomeDB = mempoolContractFtIncomeDB
	m.mempoolContractFtSpendDB = mempoolContractFtSpendDB
	m.mempoolContractFtInfoStore = mempoolContractFtInfoStore
	m.mempoolContractFtGenesisStore = mempoolContractFtGenesisStore
	m.mempoolContractFtGenesisOutputStore = mempoolContractFtGenesisOutputStore
	m.mempoolContractFtGenesisUtxoStore = mempoolContractFtGenesisUtxoStore

	// 重新创建ZMQ客户端
	if zmqAddress != "" {
		log.Println("重新创建ZMQ客户端...")
		m.zmqClient = NewZMQClient(zmqAddress, nil)

		// 重新添加监听主题
		log.Println("重新添加ZMQ监听主题...")
		m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)
	}

	log.Println("FT内存池数据完全重置成功")
	return nil
}

// GetBasePath 返回FT内存池数据的基础路径
func (m *FtMempoolManager) GetBasePath() string {
	return m.basePath
}

// GetZmqAddress 返回ZMQ服务器地址
func (m *FtMempoolManager) GetZmqAddress() string {
	if m.zmqClient != nil {
		return m.zmqClient.address
	}
	return ""
}
