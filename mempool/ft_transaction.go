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
	contractFtUtxoStore      *storage.PebbleStore // 存储合约数据 key: txID, value:FtAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType,...
	mempoolAddressFtIncomeDB *storage.SimpleDB    // 内存池收入数据库 key: outpoint+address   value:CodeHash@Genesis@sensibleId@Amount@Index@Value
	mempoolAddressFtSpendDB  *storage.SimpleDB    // 内存池支出数据库 key: outpoint+address   value:CodeHash@Genesis@sensibleId@Amount@Index@Value

	contractFtInfoStore                 *storage.PebbleStore // 存储合约信息 key:codeHash@genesis, value: sensibleId@name@symbol@decimal
	contractFtGenesisStore              *storage.PebbleStore // 存储合约创世信息 key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
	contractFtGenesisOutputStore        *storage.PebbleStore // 存储使用合约创世输出信息 key:usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
	contractFtGenesisUtxoStore          *storage.PebbleStore // 存储合约创世UTXO信息 key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}
	mempoolContractFtInfoStore          *storage.SimpleDB    // 内存池合约信息数据库
	mempoolContractFtGenesisStore       *storage.SimpleDB    // 内存池合约创世信息数据库 key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
	mempoolContractFtGenesisOutputStore *storage.SimpleDB    // 内存池合约创世输出信息数据库 key: usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
	mempoolContractFtGenesisUtxoStore   *storage.SimpleDB    // 内存池合约创世UTXO信息数据库

	mempoolAddressFtIncomeValidStore *storage.SimpleDB // 内存池合约数据数据库 key: outpoint+address   value:CodeHash@Genesis@sensibleId@Amount@Index@Value
	mempoolUncheckFtOutpointStore    *storage.SimpleDB // 内存池未检查FT输出点数据库 key: outpoint   value:ftAddress@codeHash@genesis@sensibleId@ftAmount@index@value
	mempoolUsedFtIncomeStore         *storage.SimpleDB // 内存池已使用FT收入数据库 key: usedTxId, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height,...

	mempoolUniqueFtIncomeStore *storage.SimpleDB // 内存池唯一FT收入数据库  key: outpoint+ftCodehashGenesis, value: codeHash@genesis@sensibleId@customData@Index@Value
	mempoolUniqueFtSpendStore  *storage.SimpleDB // 内存池唯一FT支出数据库  key: outpoint+ftCodehashGenesis, value: CodeHash@Genesis@sensibleId@customData@Index@Value

	mempoolVerifyTxStore *storage.SimpleDB // key: txId, value: ""
	chainCfg             *chaincfg.Params
	zmqClient            *ZMQClient
	basePath             string // 数据目录基础路径
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
	mempoolAddressFtIncomeDB, err := storage.NewSimpleDB(basePath + "/mempool_address_ft_income")
	if err != nil {
		log.Printf("创建FT内存池收入数据库失败: %v", err)
		return nil
	}

	mempoolAddressFtSpendDB, err := storage.NewSimpleDB(basePath + "/mempool_address_ft_spend")
	if err != nil {
		log.Printf("创建FT内存池支出数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		return nil
	}

	mempoolContractFtInfoStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_info")
	if err != nil {
		log.Printf("创建FT内存池信息数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		return nil
	}

	mempoolContractFtGenesisStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_genesis")
	if err != nil {
		log.Printf("创建FT内存池创世数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		return nil
	}

	mempoolContractFtGenesisOutputStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_genesis_output")
	if err != nil {
		log.Printf("创建FT内存池创世输出数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		return nil
	}

	mempoolContractFtGenesisUtxoStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_genesis_utxo")
	if err != nil {
		log.Printf("创建FT内存池创世UTXO数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		return nil
	}

	mempoolAddressFtIncomeValidStore, err := storage.NewSimpleDB(basePath + "/mempool_address_ft_income_valid")
	if err != nil {
		log.Printf("创建FT内存池收入有效数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		return nil
	}

	mempoolUncheckFtOutpointStore, err := storage.NewSimpleDB(basePath + "/mempool_uncheck_ft_outpoint")
	if err != nil {
		log.Printf("创建FT内存池未检查FT输出点数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidStore.Close()
		return nil
	}

	mempoolUsedFtIncomeStore, err := storage.NewSimpleDB(basePath + "/mempool_used_ft_income")
	if err != nil {
		log.Printf("创建FT内存池已使用FT收入数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidStore.Close()
		mempoolUncheckFtOutpointStore.Close()
		return nil
	}

	mempoolUniqueFtIncomeStore, err := storage.NewSimpleDB(basePath + "/mempool_unique_ft_income")
	if err != nil {
		log.Printf("创建FT内存池唯一FT收入数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidStore.Close()
		mempoolUncheckFtOutpointStore.Close()
		mempoolUsedFtIncomeStore.Close()
		return nil
	}

	mempoolUniqueFtSpendStore, err := storage.NewSimpleDB(basePath + "/mempool_unique_ft_spend")
	if err != nil {
		log.Printf("创建FT内存池唯一FT支出数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidStore.Close()
		mempoolUncheckFtOutpointStore.Close()
		mempoolUsedFtIncomeStore.Close()
		mempoolUniqueFtIncomeStore.Close()
		return nil
	}

	mempoolVerifyTxStore, err := storage.NewSimpleDB(basePath + "/mempool_verify_tx")
	if err != nil {
		log.Printf("创建FT内存池唯一FT支出数据库失败: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidStore.Close()
		mempoolUncheckFtOutpointStore.Close()
		mempoolUsedFtIncomeStore.Close()
		mempoolUniqueFtIncomeStore.Close()
		mempoolUniqueFtSpendStore.Close()
		return nil
	}

	m := &FtMempoolManager{
		contractFtUtxoStore:                 contractFtUtxoStore,
		mempoolAddressFtIncomeDB:            mempoolAddressFtIncomeDB,
		mempoolAddressFtSpendDB:             mempoolAddressFtSpendDB,
		mempoolContractFtInfoStore:          mempoolContractFtInfoStore,
		mempoolContractFtGenesisStore:       mempoolContractFtGenesisStore,
		mempoolContractFtGenesisOutputStore: mempoolContractFtGenesisOutputStore,
		mempoolContractFtGenesisUtxoStore:   mempoolContractFtGenesisUtxoStore,
		contractFtInfoStore:                 contractFtInfoStore,
		contractFtGenesisStore:              contractFtGenesisStore,
		contractFtGenesisOutputStore:        contractFtGenesisOutputStore,
		contractFtGenesisUtxoStore:          contractFtGenesisUtxoStore,
		mempoolAddressFtIncomeValidStore:    mempoolAddressFtIncomeValidStore,
		mempoolUncheckFtOutpointStore:       mempoolUncheckFtOutpointStore,
		mempoolUsedFtIncomeStore:            mempoolUsedFtIncomeStore,
		mempoolUniqueFtIncomeStore:          mempoolUniqueFtIncomeStore,
		mempoolUniqueFtSpendStore:           mempoolUniqueFtSpendStore,
		mempoolVerifyTxStore:                mempoolVerifyTxStore,
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
	if m.mempoolAddressFtIncomeDB != nil {
		m.mempoolAddressFtIncomeDB.Close()
	}
	if m.mempoolAddressFtSpendDB != nil {
		m.mempoolAddressFtSpendDB.Close()
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
	if m.mempoolAddressFtIncomeValidStore != nil {
		m.mempoolAddressFtIncomeValidStore.Close()
	}
	if m.mempoolUncheckFtOutpointStore != nil {
		m.mempoolUncheckFtOutpointStore.Close()
	}
	if m.mempoolUsedFtIncomeStore != nil {
		m.mempoolUsedFtIncomeStore.Close()
	}
	if m.mempoolUniqueFtIncomeStore != nil {
		m.mempoolUniqueFtIncomeStore.Close()
	}
	if m.mempoolUniqueFtSpendStore != nil {
		m.mempoolUniqueFtSpendStore.Close()
	}
	if m.mempoolVerifyTxStore != nil {
		m.mempoolVerifyTxStore.Close()
	}
}

// HandleRawTransaction 处理原始交易数据
func (m *FtMempoolManager) HandleRawTransaction(topic string, data []byte) error {
	// 1. 解析原始交易
	tx, err := DeserializeTransaction(data)
	if err != nil {
		return fmt.Errorf("解析交易失败: %w", err)
	}
	// fmt.Printf("ZMQ收到交易: %s\n", tx.TxHash().String())

	txHash := tx.TxHash().String()
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txHash, _ = blockchain.GetNewHash(tx)
	}

	// 2. 处理交易输出，创建新的FT UTXO
	isFtTx, err := m.processFtOutputs(tx)
	if err != nil {
		return fmt.Errorf("处理FT交易输出失败: %w", err)
	}
	if isFtTx {
		fmt.Printf("ZMQ收到FT交易: %s\n", txHash)
	}

	// 3. 处理交易输入，标记花费的FT UTXO
	err = m.processFtInputs(tx)
	if err != nil {
		return fmt.Errorf("处理FT交易输入失败: %w", err)
	}

	if isFtTx {
		// 4. 处理VerifyTx
		err = m.processVerifyTx(tx)
		if err != nil {
			return fmt.Errorf("处理VerifyTx失败: %w", err)
		}
	}

	return nil
}

func (m *FtMempoolManager) processVerifyTx(tx *wire.MsgTx) error {
	txHash := tx.TxHash().String()
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txHash, _ = blockchain.GetNewHash(tx)
	}

	// verifyTxKey := common.ConcatBytesOptimized([]string{txHash}, ":")

	// 先检查主存储中是否存在
	fmt.Printf("processVerifyTx txHash: %s\n", txHash)
	_, err := m.mempoolVerifyTxStore.GetSimpleRecord(txHash)
	if err != nil {
		if strings.Contains(err.Error(), storage.ErrNotFound.Error()) {
			// 主存储中不存在，添加到内存池存储
			err = m.mempoolVerifyTxStore.AddSimpleRecord(txHash, []byte(txHash))
			if err != nil {
				return fmt.Errorf("存储VerifyTx失败: %w", err)
			} else {
				fmt.Printf("存储VerifyTx成功: %s\n", txHash)
			}
		} else {
			return fmt.Errorf("获取VerifyTx失败: %w", err)
		}
	}

	return nil
}

// processFtOutputs 处理FT交易输出，创建新的UTXO
func (m *FtMempoolManager) processFtOutputs(tx *wire.MsgTx) (bool, error) {
	txHash := tx.TxHash().String()
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txHash, _ = blockchain.GetNewHash(tx)
	}
	isFtTx := false

	// 处理每个输出
	for i, out := range tx.TxOut {
		pkScriptStr := hex.EncodeToString(out.PkScript)

		outputIndex := strconv.Itoa(i)
		utxoID := txHash + ":" + outputIndex

		ftInfo, uniqueUtxoInfo, contractTypeStr, err := blockchain.ParseContractFtInfo(pkScriptStr, m.chainCfg)
		if err != nil {
			fmt.Println("ParseFtInfo error", err)
			return isFtTx, nil
		}
		if contractTypeStr == "ft" {
			isFtTx = true
			if ftInfo == nil {
				continue
			}

			// 为每个地址创建FT UTXO索引
			ftAddress := ftInfo.Address
			valueStr := strconv.FormatInt(out.Value, 10)
			// 存储FT UTXO信息，格式：CodeHash@Genesis@sensible@Amount@Value
			ftAmount := strconv.FormatUint(ftInfo.Amount, 10)
			mempoolFtUtxo := common.ConcatBytesOptimized([]string{ftInfo.CodeHash, ftInfo.Genesis, ftInfo.SensibleId, ftAmount, outputIndex, valueStr}, "@")
			//codeHash@genesis@sensibleId@ftAmount@index@value
			err = m.mempoolAddressFtIncomeDB.AddRecord(utxoID, ftAddress, []byte(mempoolFtUtxo))
			if err != nil {
				log.Printf("[Mempool] 存储FT内存池UTXO索引失败 %s -> %s: %v", utxoID, ftAddress, err)
				continue
			}

			//处理uncheckFtOutpointStore
			uncheckFtOutpointKey := utxoID
			//ftAddress@codeHash@genesis@sensibleId@ftAmount@index@value
			newMempoolFtUtxo := common.ConcatBytesOptimized([]string{ftAddress, mempoolFtUtxo}, "@")
			err = m.mempoolUncheckFtOutpointStore.AddSimpleRecord(uncheckFtOutpointKey, []byte(newMempoolFtUtxo))
			if err != nil {
				log.Printf("[Mempool] 存储FT内存池未检查UTXO索引失败 %s -> %s: %v", utxoID, ftAddress, err)
				continue
			}

			// 1. 处理FT信息存储
			ftInfoKey := common.ConcatBytesOptimized([]string{ftInfo.CodeHash, ftInfo.Genesis}, "@")
			// 先检查主存储中是否存在
			_, err = m.contractFtInfoStore.Get([]byte(ftInfoKey))
			if err == storage.ErrNotFound {
				// 主存储中不存在，添加到内存池存储
				//key:codeHash@genesis, value: sensibleId@name@symbol@decimal
				ftInfoValue := common.ConcatBytesOptimized([]string{ftInfo.SensibleId, ftInfo.Name, ftInfo.Symbol, strconv.FormatUint(uint64(ftInfo.Decimal), 10)}, "@")
				err = m.mempoolContractFtInfoStore.AddSimpleRecord(ftInfoKey, []byte(ftInfoValue))
				if err != nil {
					log.Printf("[Mempool] 存储FT内存池信息失败 %s: %v", ftInfoKey, err)
				}
			}

			// 2. 处理初始创世信息存储
			if ftInfo.SensibleId == "000000000000000000000000000000000000000000000000000000000000000000000000" {
				genesisKey := common.ConcatBytesOptimized([]string{txHash, outputIndex}, ":")
				// 先检查主存储中是否存在
				_, err = m.contractFtGenesisStore.Get([]byte(genesisKey))
				if err == storage.ErrNotFound {
					// 主存储中不存在，添加到内存池存储
					//key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
					genesisValue := common.ConcatBytesOptimized([]string{ftInfo.SensibleId, ftInfo.Name, ftInfo.Symbol, strconv.FormatUint(uint64(ftInfo.Decimal), 10), ftInfo.CodeHash, ftInfo.Genesis}, "@")
					err = m.mempoolContractFtGenesisStore.AddSimpleRecord(genesisKey, []byte(genesisValue))
					if err != nil {
						log.Printf("[Mempool] 存储FT内存池创世信息失败 %s: %v", genesisKey, err)
					}
				}
			}

			// 3. 处理new创世UTXO存储
			if ftInfo.Amount == 0 {
				genesisUtxoKey := common.ConcatBytesOptimized([]string{txHash, outputIndex}, ":")
				// 先检查主存储中是否存在
				_, err = m.contractFtGenesisUtxoStore.Get([]byte(genesisUtxoKey))
				if err == storage.ErrNotFound {
					// 主存储中不存在，添加到内存池存储
					//key:outpoint, value:sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}
					genesisUtxoValue := common.ConcatBytesOptimized([]string{
						ftInfo.SensibleId,
						ftInfo.Name,
						ftInfo.Symbol,
						strconv.FormatUint(uint64(ftInfo.Decimal), 10),
						ftInfo.CodeHash,
						ftInfo.Genesis,
						ftAmount,
						outputIndex,
						valueStr,
					}, "@")
					err = m.mempoolContractFtGenesisUtxoStore.AddSimpleRecord(genesisUtxoKey, []byte(genesisUtxoValue))
					if err != nil {
						log.Printf("[Mempool] 存储FT内存池创世UTXO失败 %s: %v", genesisUtxoKey, err)
					}
				}
			}

		} else if contractTypeStr == "unique" {
			isFtTx = true
			if uniqueUtxoInfo == nil {
				continue
			}
			valueStr := strconv.FormatInt(out.Value, 10)

			ftCodehashGenesis := uniqueUtxoInfo.CodeHash + "@" + uniqueUtxoInfo.Genesis

			mempoolUniquetUtxo := common.ConcatBytesOptimized([]string{uniqueUtxoInfo.CodeHash, uniqueUtxoInfo.Genesis, uniqueUtxoInfo.SensibleId, uniqueUtxoInfo.CustomData, outputIndex, valueStr}, "@")
			//key: outpoint+ftCodehashGenesis, value: codeHash@genesis@sensibleId@customData@value
			err = m.mempoolUniqueFtIncomeStore.AddRecord(utxoID, ftCodehashGenesis, []byte(mempoolUniquetUtxo))
			if err != nil {
				log.Printf("[Mempool] 存储Unique内存池UTXO索引失败 %s -> %s: %v", utxoID, "", err)
				continue
			}

		} else {
			continue
		}

	}

	return isFtTx, nil
}

// processFtInputs 处理FT交易输入
func (m *FtMempoolManager) processFtInputs(tx *wire.MsgTx) error {
	// 跳过挖矿交易
	if IsCoinbaseTx(tx) {
		return nil
	}

	// 用于存储创世交易ID和其输出信息
	var usedGenesisUtxoMap = make(map[string]string)
	var txPointUsedMap = make(map[string]string)
	var allTxPoints []string
	var usedFtIncomeMap = make(map[string][]string)

	txId := tx.TxHash().String()
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txId, _ = blockchain.GetNewHash(tx)
	}

	// 首先收集所有输入点
	for _, in := range tx.TxIn {
		prevTxHash := in.PreviousOutPoint.Hash.String()
		prevOutputIndex := strconv.Itoa(int(in.PreviousOutPoint.Index))
		spentUtxoID := prevTxHash + ":" + prevOutputIndex
		allTxPoints = append(allTxPoints, spentUtxoID)

		txPointUsedMap[spentUtxoID] = txId

		var ftUtxoAddress string
		var ftUtxoCodeHash string
		var ftUtxoGenesis string
		var ftUtxoSensibleId string
		var ftUtxoAmount string
		var ftUtxoIndex string
		var ftUtxoValue string
		_ = ftUtxoIndex
		var ftUtxoContractType string
		var ftUtxoCodehashGenesis string
		var ftUtxoCustomData string

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
						if outputInfo[5] == strconv.Itoa(int(in.PreviousOutPoint.Index)) {
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
				ftUtxoSensibleId = outputInfo[3]
				ftUtxoAmount = outputInfo[4]
				ftUtxoIndex = outputInfo[5]
				ftUtxoValue = outputInfo[6]
				ftUtxoContractType = outputInfo[8]
			}
			//fmt.Println("主库", utxoAddress, utxoAmount)
		} else if err == storage.ErrNotFound {
			// 主UTXO存储中没找到，尝试从内存池收入数据库查找
			//CodeHash@Genesis@Amount@Value
			ftUtxoAddress, ftUtxoCodeHash, ftUtxoGenesis, ftUtxoSensibleId, ftUtxoAmount, ftUtxoValue, ftUtxoIndex, _ = m.mempoolAddressFtIncomeDB.GetByFtUTXO(spentUtxoID)
			if ftUtxoCodeHash == "" {
				ftUtxoCodehashGenesis, ftUtxoCodeHash, ftUtxoGenesis, ftUtxoSensibleId, ftUtxoCustomData, ftUtxoValue, ftUtxoIndex, _ = m.mempoolUniqueFtIncomeStore.GetByUniqueFtUTXO(spentUtxoID)
				if ftUtxoCodehashGenesis == "" {
					continue
				}
				ftUtxoContractType = "unique"
			} else {
				ftUtxoContractType = "ft"
			}

		} else {
			continue
		}

		// 记录到内存池支出数据库
		// 记录到内存池支出数据库，包含金额信息
		if ftUtxoContractType == "ft" {
			recordKey := ftUtxoAddress
			//CodeHash@Genesis@sensibleId@Amount@Value
			mempoolFtUtxo := common.ConcatBytesOptimized([]string{ftUtxoCodeHash, ftUtxoGenesis, ftUtxoSensibleId, ftUtxoAmount, ftUtxoIndex, ftUtxoValue}, "@")
			err = m.mempoolAddressFtSpendDB.AddRecord(spentUtxoID, recordKey, []byte(mempoolFtUtxo))
			//fmt.Println("存储花费：", spentUtxoID, utxoAddress, utxoAmount, err)
			if err != nil {
				continue
			}
		} else if ftUtxoContractType == "unique" {
			recordKey := ftUtxoCodehashGenesis
			//CodeHash@Genesis@sensibleId@customData@Value
			mempoolUniqueUtxo := common.ConcatBytesOptimized([]string{ftUtxoCodeHash, ftUtxoGenesis, ftUtxoSensibleId, ftUtxoCustomData, ftUtxoIndex, ftUtxoValue}, "@")
			err = m.mempoolUniqueFtSpendStore.AddRecord(spentUtxoID, recordKey, []byte(mempoolUniqueUtxo))
			if err != nil {
				continue
			}
		} else {
			continue
		}

	}

	usedTxId := txId
	usedFtIncomeMap[usedTxId] = make([]string, 0)
	// 查询所有输入点是否存在于contractFtGenesisUtxoStore
	for _, txPoint := range allTxPoints {
		value, err := m.contractFtGenesisUtxoStore.Get([]byte(txPoint))
		if err == nil {
			// 找到创世UTXO，记录交易outpoint
			//key:outpoint, value:sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}
			usedGenesisUtxoMap[txPoint] = string(value)
		}

		// 检查内存池中是否存在
		mempoolValue, err := m.mempoolContractFtGenesisUtxoStore.GetSimpleRecord(txPoint)
		if err == nil {
			// 如果是创世UTXO，记录交易ID
			//key:outpoint, value:sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}
			usedGenesisUtxoMap[txPoint] = string(mempoolValue)
		}

		preTxId := strings.Split(txPoint, ":")[0]
		preTxIndex := strings.Split(txPoint, ":")[1]
		//value:FtAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType,...
		ftUtxoValueListStr, err := m.contractFtUtxoStore.Get([]byte(preTxId))
		if err == nil {
			valueInfoList := strings.Split(string(ftUtxoValueListStr), ",")
			for _, valueInfo := range valueInfoList {
				valueInfoList := strings.Split(valueInfo, "@")
				if len(valueInfoList) < 9 {
					continue
				}
				index := valueInfoList[5]
				if preTxIndex == index {
					newValue := common.ConcatBytesOptimized([]string{
						valueInfoList[0],
						valueInfoList[1],
						valueInfoList[2],
						valueInfoList[3],
						valueInfoList[4],
						preTxId,
						valueInfoList[5],
						valueInfoList[6],
						valueInfoList[7],
					}, "@")
					// key: UsedtxID, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height,...
					usedFtIncomeMap[usedTxId] = append(usedFtIncomeMap[usedTxId], newValue)
					break
				}

			}
		}

		//key:usedTxId   value:ftAddress@CodeHash@Genesis@sensibleId@Amount@txId@Index@Value@height,...
		incomeList, err := m.mempoolAddressFtIncomeDB.GetFtUtxoByOutpoint(txPoint)
		if err == nil {
			for _, utxo := range incomeList {
				if utxo.Index == preTxIndex {
					newValue := common.ConcatBytesOptimized([]string{
						utxo.Address,
						utxo.CodeHash,
						utxo.Genesis,
						utxo.SensibleId,
						utxo.Amount,
						preTxId,
						utxo.Index,
						utxo.Value,
						"-1",
					}, "@")
					usedFtIncomeMap[usedTxId] = append(usedFtIncomeMap[usedTxId], newValue)
					break
				}
			}
		}
	}

	// 处理创世UTXO的消费
	if len(usedGenesisUtxoMap) > 0 {
		for txPoint, value := range usedGenesisUtxoMap {
			// 获取原始UTXO信息并添加@IsSpent标记
			spentValue := value + "@1" // 添加@IsSpent标记
			if err := m.mempoolContractFtGenesisUtxoStore.AddSimpleRecord(txPoint, []byte(spentValue)); err != nil {
				log.Printf("[Mempool] 更新FT内存池创世UTXO状态失败 %s: %v", txPoint, err)
			}

		}
	}

	if len(usedFtIncomeMap) > 0 {
		for usedTxId, utxoList := range usedFtIncomeMap {
			//value: ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType
			if err := m.mempoolUsedFtIncomeStore.AddSimpleRecord(usedTxId, []byte(strings.Join(utxoList, ","))); err != nil {
				log.Printf("[Mempool] 存储FT内存池已使用UTXO失败 %s: %v", usedTxId, err)
			}
		}
	}

	// 处理创世输出存储
	// if len(usedGenesisUtxoMap) > 0 {
	// 	//key:outpoint, value:sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}
	// 	for usedTxPoint, _ := range usedGenesisUtxoMap {
	// 		// 收集该交易的所有输出信息
	// 		var outputs []string
	// 		for i, out := range tx.TxOut {
	// 			pkScriptStr := hex.EncodeToString(out.PkScript)
	// 			ftInfo, _, _, err := blockchain.ParseContractFtInfo(pkScriptStr, m.chainCfg)
	// 			if err != nil || ftInfo == nil {
	// 				continue
	// 			}
	// 			//key: usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
	// 			outputInfo := common.ConcatBytesOptimized([]string{
	// 				ftInfo.SensibleId,
	// 				ftInfo.Name,
	// 				ftInfo.Symbol,
	// 				strconv.FormatUint(uint64(ftInfo.Decimal), 10),
	// 				ftInfo.CodeHash,
	// 				ftInfo.Genesis,
	// 				strconv.FormatUint(ftInfo.Amount, 10),
	// 				txId,
	// 				strconv.Itoa(i),
	// 				strconv.FormatInt(out.Value, 10),
	// 			}, "@")
	// 			outputs = append(outputs, outputInfo)
	// 		}

	// 		if len(outputs) > 0 {
	// 			outputValue := strings.Join(outputs, ",")
	// 			if err := m.mempoolContractFtGenesisOutputStore.AddRecord(usedTxPoint, "", []byte(outputValue)); err != nil {
	// 				log.Printf("[Mempool] 存储FT内存池创世输出失败 %s: %v", txId, err)
	// 			}
	// 		}
	// 	}

	// }

	if len(allTxPoints) > 0 {
		for _, usedTxPoint := range allTxPoints {
			// 收集该交易的所有输出信息
			var outputs []string
			for i, out := range tx.TxOut {
				pkScriptStr := hex.EncodeToString(out.PkScript)
				ftInfo, _, _, err := blockchain.ParseContractFtInfo(pkScriptStr, m.chainCfg)
				if err != nil || ftInfo == nil {
					continue
				}
				//key: usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
				outputInfo := common.ConcatBytesOptimized([]string{
					ftInfo.SensibleId,
					ftInfo.Name,
					ftInfo.Symbol,
					strconv.FormatUint(uint64(ftInfo.Decimal), 10),
					ftInfo.CodeHash,
					ftInfo.Genesis,
					strconv.FormatUint(ftInfo.Amount, 10),
					txId,
					strconv.Itoa(i),
					strconv.FormatInt(out.Value, 10),
				}, "@")
				outputs = append(outputs, outputInfo)
			}

			if len(outputs) > 0 {
				outputValue := strings.Join(outputs, ",")
				if err := m.mempoolContractFtGenesisOutputStore.AddSimpleRecord(usedTxPoint, []byte(outputValue)); err != nil {
					log.Printf("[Mempool] 存储FT内存池创世输出失败 %s: %v", txId, err)
				}
			}
		}
	}

	return nil
}

// ProcessNewBlockTxs 处理新区块中的FT交易，清理内存池记录
func (m *FtMempoolManager) ProcessNewBlockTxs(incomeUtxoList []common.FtUtxo, spendOutpointList []string, txList []string) error {
	// 删除VerifyTx
	for _, tx := range txList {
		err := m.mempoolVerifyTxStore.DeleteSimpleRecord(tx)
		if err != nil {
			log.Printf("删除VerifyTx失败 %s: %v", tx, err)
		}
	}
	if len(incomeUtxoList) == 0 {
		return nil
	}

	// 删除income
	for _, utxo := range incomeUtxoList {
		if utxo.ContractType == "ft" {
			err := m.mempoolAddressFtIncomeDB.DeleteRecord(utxo.UtxoId, utxo.Address)
			if err != nil {
				log.Printf("删除FT内存池收入记录失败 %s: %v", utxo.TxID, err)
			}

			//处理uncheckFtOutpointStore
			err = m.mempoolAddressFtIncomeValidStore.DeleteRecord(utxo.UtxoId, utxo.Address)
			if err != nil {
				log.Printf("[Mempool] 删除FT内存池有效UTXO索引失败 %s -> %s: %v", utxo.UtxoId, utxo.Address, err)
			}

			// 检查并删除FT信息存储中的记录
			parts := strings.Split(utxo.UtxoId, ":")
			if len(parts) == 2 {
				outpoint := common.ConcatBytesOptimized([]string{parts[0], parts[1]}, ":")

				// 检查并删除初始创世信息存储中的记录
				genesisKey := outpoint
				_, err = m.mempoolContractFtGenesisStore.GetSimpleRecord(genesisKey)
				if err == nil {
					if err := m.mempoolContractFtGenesisStore.DeleteSimpleRecord(genesisKey); err != nil {
						log.Printf("删除FT内存池初始创世信息失败 %s: %v", genesisKey, err)
					}
				}

				// 检查并删除新创世输出存储中的记录
				_, err = m.mempoolContractFtGenesisUtxoStore.GetSimpleRecord(genesisKey)
				if err == nil {
					if err := m.mempoolContractFtGenesisUtxoStore.DeleteSimpleRecord(genesisKey); err != nil {
						log.Printf("删除FT内存池new创世输出失败 %s: %v", genesisKey, err)
					}
				}
			}
		} else if utxo.ContractType == "unique" {
			err := m.mempoolUniqueFtIncomeStore.DeleteRecord(utxo.UtxoId, utxo.CodeHash+"@"+utxo.Genesis)
			if err != nil {
				log.Printf("删除FT内存池unique收入记录失败 %s: %v", utxo.UtxoId, err)
			}
		} else {
			continue
		}

	}

	for _, utxo := range incomeUtxoList {
		codeHashGenesis := common.ConcatBytesOptimized([]string{utxo.CodeHash, utxo.Genesis}, "@")
		err := m.mempoolContractFtInfoStore.DeleteSimpleRecord(codeHashGenesis)
		if err != nil {
			log.Printf("删除FT内存池创世信息失败 %s: %v", utxo.CodeHash+"@"+utxo.Genesis, err)
		}
	}

	// 删除spend
	for _, outpoint := range spendOutpointList {
		err := m.mempoolAddressFtSpendDB.DeleteFtSpendRecord(outpoint)
		if err != nil {
			log.Printf("删除FT内存池ft支出记录失败 %s: %v", outpoint, err)
		}
		err = m.mempoolUniqueFtSpendStore.DeleteUniqueSpendRecord(outpoint)
		if err != nil {
			log.Printf("删除FT内存池unique支出记录失败 %s: %v", outpoint, err)
		}
	}
	return nil
}

// CleanByHeight 通过区块高度清理FT内存池记录
func (m *FtMempoolManager) CleanByHeight(height int, bcClient interface{}) error {
	log.Printf("开始清理FT内存池，处理到区块高度: %d", height)

	// 尝试断言bcClient为blockchain.Client类型
	client, ok := bcClient.(*blockchain.FtClient)
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

	txList := make([]string, 0)

	// 提取incomeUtxo列表
	var incomeFtUtxoList []common.FtUtxo = make([]common.FtUtxo, 0)
	var spendOutpointList []string
	for _, tx := range block.Tx {
		txList = append(txList, tx.Txid)
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
			var ftUtxo *common.FtUtxo
			ftInfo, uniqueUtxoInfo, contractTypeStr, err := blockchain.ParseContractFtInfo(out.ScriptPubKey.Hex, m.chainCfg)
			if err != nil {
				return nil
			}
			if contractTypeStr == "ft" {
				ftUtxo = &common.FtUtxo{
					ContractType: contractTypeStr,
					UtxoId:       common.ConcatBytesOptimized([]string{tx.Txid, strconv.Itoa(k)}, ":"),
					TxID:         tx.Txid,
					Address:      ftInfo.Address,
					Value:        amount,
					CodeHash:     ftInfo.CodeHash,
					Genesis:      ftInfo.Genesis,
					SensibleId:   ftInfo.SensibleId,
					Name:         ftInfo.Name,
					Symbol:       ftInfo.Symbol,
					Amount:       strconv.FormatUint(ftInfo.Amount, 10),
					Decimal:      strconv.FormatUint(uint64(ftInfo.Decimal), 10),
				}
			} else if contractTypeStr == "unique" {
				ftUtxo = &common.FtUtxo{
					ContractType: contractTypeStr,
					UtxoId:       common.ConcatBytesOptimized([]string{tx.Txid, strconv.Itoa(k)}, ":"),
					TxID:         tx.Txid,
					Address:      address,
					Value:        amount,
					CodeHash:     uniqueUtxoInfo.CodeHash,
					Genesis:      uniqueUtxoInfo.Genesis,
					SensibleId:   uniqueUtxoInfo.SensibleId,
					CustomData:   uniqueUtxoInfo.CustomData,
				}
			} else {
				continue
			}

			if ftInfo == nil {
				continue
			}

			incomeFtUtxoList = append(incomeFtUtxoList, *ftUtxo)
		}
	}

	// 清理内存池记录
	return m.ProcessNewBlockTxs(incomeFtUtxoList, spendOutpointList, txList)
}

// InitializeMempool 在启动时从节点获取当前所有内存池交易并处理
func (m *FtMempoolManager) InitializeMempool(bcClient interface{}) {
	// 使用单独的goroutine执行，避免阻塞主程序
	go func() {
		log.Printf("开始初始化FT内存池数据...")

		// 断言为blockchain.Client
		client, ok := bcClient.(*blockchain.FtClient)
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
				txRaw, err := client.GetRawTransactionHex(txid)
				if err != nil {
					fmt.Println("GetRawTransaction error", err)
					continue
				}
				txRawByte, err := hex.DecodeString(txRaw)
				if err != nil {
					fmt.Println("DecodeString error", err)
					continue
				}
				msgTx, err := DeserializeTransaction(txRawByte)
				if err != nil {
					fmt.Println("DeserializeTransaction error", err)
					continue
				}

				// 先处理输出（创建新的UTXO）
				isFtTx, err := m.processFtOutputs(msgTx)
				if err != nil {
					log.Printf("处理FT交易输出失败 %s: %v", txid, err)
					continue
				}
				if isFtTx {
					fmt.Printf("内存池收到FT交易: %s\n", txid)
				}

				// 再处理输入（标记花费的UTXO）
				if err := m.processFtInputs(msgTx); err != nil {
					log.Printf("处理FT交易输入失败 %s: %v", txid, err)
					continue
				}

				if isFtTx {
					// 处理VerifyTx
					if err := m.processVerifyTx(msgTx); err != nil {
						log.Printf("处理VerifyTx失败 %s: %v", txid, err)
						continue
					}
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
	incomeDbPath := m.basePath + "/mempool_address_ft_income"
	spendDbPath := m.basePath + "/mempool_address_ft_spend"
	infoDbPath := m.basePath + "/mempool_contract_ft_info"
	genesisDbPath := m.basePath + "/mempool_contract_ft_genesis"
	genesisOutputDbPath := m.basePath + "/mempool_contract_ft_genesis_output"
	genesisUtxoDbPath := m.basePath + "/mempool_contract_ft_genesis_utxo"
	incomeValidDbPath := m.basePath + "/mempool_address_ft_income_valid"
	uncheckFtOutpointDbPath := m.basePath + "/mempool_uncheck_ft_outpoint"
	usedFtIncomeDbPath := m.basePath + "/mempool_used_ft_income"
	uniqueFtIncomeDbPath := m.basePath + "/mempool_unique_ft_income"
	uniqueFtSpendDbPath := m.basePath + "/mempool_unique_ft_spend"
	mempoolVerifyTxDbPath := m.basePath + "/mempool_verify_tx"

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
		if m.mempoolAddressFtIncomeDB != nil {
			m.mempoolAddressFtIncomeDB.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭支出数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolAddressFtSpendDB != nil {
			m.mempoolAddressFtSpendDB.Close()
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

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭收入有效数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolAddressFtIncomeValidStore != nil {
			m.mempoolAddressFtIncomeValidStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭未检查FT输出数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolUncheckFtOutpointStore != nil {
			m.mempoolUncheckFtOutpointStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭已使用收入数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolUsedFtIncomeStore != nil {
			m.mempoolUsedFtIncomeStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭Unique收入数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolUniqueFtIncomeStore != nil {
			m.mempoolUniqueFtIncomeStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭Unique支出数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolUniqueFtSpendStore != nil {
			m.mempoolUniqueFtSpendStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭VerifyTx数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolVerifyTxStore != nil {
			m.mempoolVerifyTxStore.Close()
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

	log.Printf("删除FT内存池收入有效数据库: %s", incomeValidDbPath)
	if err := os.RemoveAll(incomeValidDbPath); err != nil {
		log.Printf("删除FT内存池收入有效数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池未检查FT输出数据库: %s", uncheckFtOutpointDbPath)
	if err := os.RemoveAll(uncheckFtOutpointDbPath); err != nil {
		log.Printf("删除FT内存池未检查FT输出数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池已使用收入数据库: %s", usedFtIncomeDbPath)
	if err := os.RemoveAll(usedFtIncomeDbPath); err != nil {
		log.Printf("删除FT内存池已使用收入数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池Unique收入数据库: %s", uniqueFtIncomeDbPath)
	if err := os.RemoveAll(uniqueFtIncomeDbPath); err != nil {
		log.Printf("删除FT内存池Unique收入数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池Unique支出数据库: %s", uniqueFtSpendDbPath)
	if err := os.RemoveAll(uniqueFtSpendDbPath); err != nil {
		log.Printf("删除FT内存池Unique支出数据库失败: %v", err)
		return err
	}

	log.Printf("删除FT内存池Unique支出数据库: %s", mempoolVerifyTxDbPath)
	if err := os.RemoveAll(mempoolVerifyTxDbPath); err != nil {
		log.Printf("删除FT内存池Unique支出数据库失败: %v", err)
		return err
	}

	// 重新创建数据库
	log.Println("重新创建FT内存池数据库...")
	mempoolAddressFtIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
	if err != nil {
		log.Printf("重新创建FT内存池收入数据库失败: %v", err)
		return err
	}

	mempoolAddressFtSpendDB, err := storage.NewSimpleDB(spendDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		log.Printf("重新创建FT内存池支出数据库失败: %v", err)
		return err
	}

	mempoolContractFtInfoStore, err := storage.NewSimpleDB(infoDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		log.Printf("重新创建FT内存池信息数据库失败: %v", err)
		return err
	}

	mempoolContractFtGenesisStore, err := storage.NewSimpleDB(genesisDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		log.Printf("重新创建FT内存池创世数据库失败: %v", err)
		return err
	}

	mempoolContractFtGenesisOutputStore, err := storage.NewSimpleDB(genesisOutputDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		log.Printf("重新创建FT内存池创世输出数据库失败: %v", err)
		return err
	}

	mempoolContractFtGenesisUtxoStore, err := storage.NewSimpleDB(genesisUtxoDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		log.Printf("重新创建FT内存池创世UTXO数据库失败: %v", err)
		return err
	}

	mempoolAddressFtIncomeValidDB, err := storage.NewSimpleDB(incomeValidDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		log.Printf("重新创建FT内存池收入有效数据库失败: %v", err)
		return err
	}

	mempoolUncheckFtOutpointDB, err := storage.NewSimpleDB(uncheckFtOutpointDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidDB.Close()
		log.Printf("重新创建FT内存池未检查FT输出数据库失败: %v", err)
		return err
	}

	mempoolUsedFtIncomeDB, err := storage.NewSimpleDB(usedFtIncomeDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidDB.Close()
		mempoolUncheckFtOutpointDB.Close()
		log.Printf("重新创建FT内存池已使用收入数据库失败: %v", err)
		return err
	}

	mempoolUniqueFtIncomeDB, err := storage.NewSimpleDB(uniqueFtIncomeDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidDB.Close()
		mempoolUncheckFtOutpointDB.Close()
		mempoolUsedFtIncomeDB.Close()
		log.Printf("重新创建FT内存池Unique收入数据库失败: %v", err)
		return err
	}

	mempoolUniqueFtSpendDB, err := storage.NewSimpleDB(uniqueFtSpendDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidDB.Close()
		mempoolUncheckFtOutpointDB.Close()
		mempoolUsedFtIncomeDB.Close()
		mempoolUniqueFtIncomeDB.Close()
		log.Printf("重新创建FT内存池Unique支出数据库失败: %v", err)
		return err
	}

	mempoolVerifyTxDB, err := storage.NewSimpleDB(mempoolVerifyTxDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		mempoolContractFtGenesisUtxoStore.Close()
		mempoolAddressFtIncomeValidDB.Close()
		mempoolUncheckFtOutpointDB.Close()
		mempoolUsedFtIncomeDB.Close()
		mempoolUniqueFtIncomeDB.Close()
		mempoolUniqueFtSpendDB.Close()
		log.Printf("重新创建FT内存池Unique支出数据库失败: %v", err)
		return err
	}

	// 更新数据库引用
	m.mempoolAddressFtIncomeDB = mempoolAddressFtIncomeDB
	m.mempoolAddressFtSpendDB = mempoolAddressFtSpendDB
	m.mempoolContractFtInfoStore = mempoolContractFtInfoStore
	m.mempoolContractFtGenesisStore = mempoolContractFtGenesisStore
	m.mempoolContractFtGenesisOutputStore = mempoolContractFtGenesisOutputStore
	m.mempoolContractFtGenesisUtxoStore = mempoolContractFtGenesisUtxoStore
	m.mempoolAddressFtIncomeValidStore = mempoolAddressFtIncomeValidDB
	m.mempoolUncheckFtOutpointStore = mempoolUncheckFtOutpointDB
	m.mempoolUsedFtIncomeStore = mempoolUsedFtIncomeDB
	m.mempoolUniqueFtIncomeStore = mempoolUniqueFtIncomeDB
	m.mempoolUniqueFtSpendStore = mempoolUniqueFtSpendDB
	m.mempoolVerifyTxStore = mempoolVerifyTxDB

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

// GetMempoolAddressFtIncomeMap 获取内存池中所有地址的FT收入数据
// CodeHash@Genesis@sensibleId@Amount@Index@Value
func (m *FtMempoolManager) GetMempoolAddressFtIncomeMap() map[string]string {
	result := make(map[string]string)
	keyValues, err := m.mempoolAddressFtIncomeDB.GetAllKeyValues()
	if err != nil {
		log.Printf("获取所有键值对失败: %v", err)
		return result
	}

	for key, value := range keyValues {

		result[key] = value
	}

	return result
}

// GetMempoolAddressFtIncomeValidMap 获取内存池中所有地址的有效FT收入数据
// CodeHash@Genesis@sensibleId@Amount@Index@Value
func (m *FtMempoolManager) GetMempoolAddressFtIncomeValidMap() map[string]string {
	result := make(map[string]string)
	keyValues, err := m.mempoolAddressFtIncomeValidStore.GetAllKeyValues()
	if err != nil {
		log.Printf("获取所有键值对失败: %v", err)
		return result
	}

	for key, value := range keyValues {
		result[key] = value
	}

	return result
}
