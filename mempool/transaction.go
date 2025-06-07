package mempool

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/storage"
)

// MempoolManager 管理内存池交易
type MempoolManager struct {
	utxoStore       *storage.PebbleStore // 主UTXO存储，使用分片
	mempoolIncomeDB *storage.SimpleDB    // 内存池收入数据库
	mempoolSpendDB  *storage.SimpleDB    // 内存池支出数据库
	chainCfg        *chaincfg.Params
	zmqClient       *ZMQClient
	basePath        string // 数据目录基础路径
}

// NewMempoolManager 创建一个新的内存池管理器
func NewMempoolManager(basePath string, utxoStore *storage.PebbleStore, chainCfg *chaincfg.Params, zmqAddress string) *MempoolManager {
	mempoolIncomeDB, err := storage.NewSimpleDB(basePath + "/mempool_income")
	if err != nil {
		log.Printf("创建内存池收入数据库失败: %v", err)
		return nil
	}

	mempoolSpendDB, err := storage.NewSimpleDB(basePath + "/mempool_spend")
	if err != nil {
		log.Printf("创建内存池支出数据库失败: %v", err)
		mempoolIncomeDB.Close()
		return nil
	}

	m := &MempoolManager{
		utxoStore:       utxoStore,
		mempoolIncomeDB: mempoolIncomeDB,
		mempoolSpendDB:  mempoolSpendDB,
		chainCfg:        chainCfg,
		basePath:        basePath,
	}

	// 创建ZMQ客户端，不再传递db
	m.zmqClient = NewZMQClient(zmqAddress, nil)

	// 添加"rawtx"主题监听
	m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)

	return m
}

// Start 启动内存池管理器
func (m *MempoolManager) Start() error {
	return m.zmqClient.Start()
}

// Stop 停止内存池管理器
func (m *MempoolManager) Stop() {
	m.zmqClient.Stop()
	if m.mempoolIncomeDB != nil {
		m.mempoolIncomeDB.Close()
	}
	if m.mempoolSpendDB != nil {
		m.mempoolSpendDB.Close()
	}
}

// HandleRawTransaction 处理原始交易数据
func (m *MempoolManager) HandleRawTransaction(topic string, data []byte) error {
	// 1. 解析原始交易
	tx, err := DeserializeTransaction(data)
	if err != nil {
		return fmt.Errorf("解析交易失败: %w", err)
	}

	// 2. 获取交易ID
	//txHash := tx.TxHash().String()
	//log.Printf("处理原始交易: %s, 输入: %d, 输出: %d",	txHash, len(tx.TxIn), len(tx.TxOut))

	// 3. 处理交易输出，创建新的UTXO
	err = m.processOutputs(tx)
	if err != nil {
		return fmt.Errorf("处理交易输出失败: %w", err)
	}

	// 4. 处理交易输入，标记花费的UTXO
	err = m.processInputs(tx)
	if err != nil {
		return fmt.Errorf("处理交易输入失败: %w", err)
	}

	return nil
}

// processOutputs 处理交易输出，创建新的UTXO
func (m *MempoolManager) processOutputs(tx *wire.MsgTx) error {
	txHash := tx.TxHash().String()
	//fmt.Println(config.GlobalConfig.RPC.Chain, ">>>>>>>>>")
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txHash, _ = blockchain.GetNewHash(tx)
	}

	// 处理每个输出
	for i, out := range tx.TxOut {
		address := blockchain.GetAddressFromScript("", out.PkScript, m.chainCfg, config.GlobalConfig.RPC.Chain)
		// 解析输出脚本，提取地址
		// addresses, err := ExtractAddressesFromOutput(out, m.chainCfg)
		// if err != nil {
		// 	log.Printf("解析输出脚本失败 %s:%d: %v", txHash, i, err)
		// 	continue
		// }

		// // 如果没有提取到地址，跳过
		// if len(addresses) == 0 {
		// 	continue
		// }

		// 为每个地址创建UTXO索引
		outputIndex := strconv.Itoa(i)
		utxoID := txHash + ":" + outputIndex
		//fmt.Println("开始处理存入--------->", utxoID, address, out.Value)
		value := strconv.FormatInt(out.Value, 10)
		// 构建存储key:addr1_tx1:0的方式，value:utxo的金额
		// 存储UTXO -> 地址的映射到内存池收入数据库
		err := m.mempoolIncomeDB.AddRecord(utxoID, address, []byte(value))
		if err != nil {
			log.Printf("存储内存池UTXO索引失败 %s -> %s: %v", utxoID, address, err)
			continue
		}
		// list, err := m.mempoolIncomeDB.GetUtxoByKey(address)
		// if err != nil {
		// 	log.Printf("获取内存池收入金额失败 %s: %v", utxoID, err)
		// 	continue
		// }
		// for _, utxo := range list {
		// 	log.Println("获取存入", utxo.TxID, address, utxo.Amount)
		// }
	}

	return nil
}

// processInputs 处理交易输入，标记花费的UTXO
func (m *MempoolManager) processInputs(tx *wire.MsgTx) error {
	// 跳过挖矿交易
	if IsCoinbaseTx(tx) {
		return nil
	}

	// 处理每个输入
	for _, in := range tx.TxIn {
		// 构建被花费的UTXO ID
		prevTxHash := in.PreviousOutPoint.Hash.String()
		prevOutputIndex := strconv.Itoa(int(in.PreviousOutPoint.Index))
		spentUtxoID := prevTxHash + ":" + prevOutputIndex

		var utxoAddress string
		var utxoAmount string
		//fmt.Println(spentUtxoID)
		// 直接从主UTXO存储获取数据
		utxoData, err := m.utxoStore.Get([]byte(prevTxHash))
		if err == nil {
			// 处理开头的逗号
			utxoStr := string(utxoData)
			//fmt.Println("主库", utxoStr)
			if len(utxoStr) > 0 && utxoStr[0] == ',' {
				utxoStr = utxoStr[1:]
			}
			parts := strings.Split(utxoStr, ",")
			if len(parts) > int(in.PreviousOutPoint.Index) {
				// 解析地址和金额
				outputInfo := strings.Split(parts[in.PreviousOutPoint.Index], "@")
				if len(outputInfo) >= 2 {
					// 获取地址
					utxoAddress = outputInfo[0]
					// 获取金额
					// if amount, err := strconv.ParseUint(outputInfo[1], 10, 64); err == nil {
					// 	utxoAmount = amount
					// }
					utxoAmount = outputInfo[1]
				}
			}
			//fmt.Println("主库", utxoAddress, utxoAmount)
		} else if err == storage.ErrNotFound {
			// 主UTXO存储中没找到，尝试从内存池收入数据库查找
			utxoAddress, utxoAmount, err = m.mempoolIncomeDB.GetByUTXO(spentUtxoID)
			if err != nil {
				continue
			}
		} else {
			continue
		}
		// 记录到内存池支出数据库
		// 记录到内存池支出数据库，包含金额信息
		err = m.mempoolSpendDB.AddRecord(spentUtxoID, utxoAddress, []byte(utxoAmount))
		//fmt.Println("存储花费：", spentUtxoID, utxoAddress, utxoAmount, err)
		if err != nil {
			continue
		}

	}
	return nil
}

// DeserializeTransaction 将字节数组反序列化为交易
func DeserializeTransaction(data []byte) (*wire.MsgTx, error) {
	tx := wire.NewMsgTx(wire.TxVersion)
	err := tx.Deserialize(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// ExtractAddressesFromOutput 从交易输出中提取地址
func ExtractAddressesFromOutput(out *wire.TxOut, chainCfg *chaincfg.Params) ([]string, error) {
	// 解析输出脚本
	_, addresses, _, err := txscript.ExtractPkScriptAddrs(out.PkScript, chainCfg)
	if err != nil {
		return nil, err
	}

	// 转换为字符串数组
	result := make([]string, len(addresses))
	for i, addr := range addresses {
		result[i] = addr.String()
	}

	return result, nil
}

// IsCoinbaseTx 检查交易是否是挖矿交易
func IsCoinbaseTx(tx *wire.MsgTx) bool {
	// 挖矿交易只有一个输入
	if len(tx.TxIn) != 1 {
		return false
	}

	// 挖矿交易的前一个输出哈希为0
	zeroHash := &wire.OutPoint{
		Hash:  chainhash.Hash{},
		Index: 0xffffffff,
	}

	return tx.TxIn[0].PreviousOutPoint.Hash == zeroHash.Hash &&
		tx.TxIn[0].PreviousOutPoint.Index == zeroHash.Index
}

// getScriptType 获取脚本类型
func getScriptType(script []byte) string {
	// 获取脚本类型
	scriptClass := txscript.GetScriptClass(script)

	switch scriptClass {
	case txscript.PubKeyHashTy:
		return "p2pkh"
	case txscript.ScriptHashTy:
		return "p2sh"
	case txscript.WitnessV0PubKeyHashTy:
		return "p2wpkh"
	case txscript.WitnessV0ScriptHashTy:
		return "p2wsh"
	case txscript.PubKeyTy:
		return "p2pk"
	case txscript.MultiSigTy:
		return "multisig"
	case txscript.NullDataTy:
		return "nulldata"
	default:
		return "unknown"
	}
}

// ProcessNewBlockTxs 处理新区块中的交易，清理内存池记录
func (m *MempoolManager) ProcessNewBlockTxs(incomeUtxoList []common.Utxo, spendTxList []string) error {
	if len(incomeUtxoList) == 0 {
		return nil
	}

	//log.Printf("处理新区块中的 %d 个交易，清理内存池记录", len(incomeUtxoList))

	// 删除income
	for _, utxo := range incomeUtxoList {
		// 1. 从内存池收入数据库中删除相关记录
		//fmt.Println("delete", utxo.TxID, utxo.Address)

		err := m.mempoolIncomeDB.DeleteRecord(utxo.TxID, utxo.Address)
		if err != nil {
			log.Printf("删除内存池收入记录失败 %s: %v", utxo.TxID, err)
		}
		//log.Printf("已清理交易 %s 的内存池记录", txid)
	}
	// 删除spend
	for _, txid := range spendTxList {
		err := m.mempoolSpendDB.DeleteSpendRecord(txid)
		if err != nil {
			log.Printf("删除内存池支出记录失败 %s: %v", txid, err)
		}
	}
	return nil
}

// CleanByHeight 通过区块高度清理内存池记录
func (m *MempoolManager) CleanByHeight(height int, bcClient interface{}) error {
	log.Printf("开始清理内存池，处理到区块高度: %d", height)

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
	var incomeUtxoList []common.Utxo
	var spendTxList []string
	for _, tx := range block.Tx {
		for _, in := range tx.Vin {
			id := in.Txid
			if id == "" {
				id = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			spendTxList = append(spendTxList, common.ConcatBytesOptimized([]string{id, strconv.Itoa(int(in.Vout))}, ":"))
		}

		for k, out := range tx.Vout {
			address := blockchain.GetAddressFromScript(out.ScriptPubKey.Hex, nil, config.GlobalNetwork, config.GlobalConfig.RPC.Chain)
			txId := common.ConcatBytesOptimized([]string{tx.Txid, strconv.Itoa(k)}, ":")
			incomeUtxoList = append(incomeUtxoList, common.Utxo{TxID: txId, Address: address})
		}
	}
	// 清理内存池记录
	return m.ProcessNewBlockTxs(incomeUtxoList, spendTxList)
}

// InitializeMempool 在启动时从节点获取当前所有内存池交易并处理
// 该方法异步执行，避免阻塞主程序
func (m *MempoolManager) InitializeMempool(bcClient interface{}) {
	// 使用单独的goroutine执行，避免阻塞主程序
	go func() {
		log.Printf("开始初始化内存池数据...")

		// 断言为blockchain.Client
		client, ok := bcClient.(*blockchain.Client)
		if !ok {
			log.Printf("初始化内存池失败: 不支持的区块链客户端类型")
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
		batchSize := 500
		totalBatches := (len(txids) + batchSize - 1) / batchSize

		for batchIdx := 0; batchIdx < totalBatches; batchIdx++ {
			start := batchIdx * batchSize
			end := start + batchSize
			if end > len(txids) {
				end = len(txids)
			}

			// 处理当前批次
			currentBatch := txids[start:end]
			log.Printf("处理内存池交易批次 %d/%d (%d 个交易)", batchIdx+1, totalBatches, len(currentBatch))

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
				if err := m.processOutputs(msgTx); err != nil {
					log.Printf("处理交易输出失败 %s: %v", txid, err)
					continue
				}

				// 再处理输入（标记花费的UTXO）
				if err := m.processInputs(msgTx); err != nil {
					log.Printf("处理交易输入失败 %s: %v", txid, err)
					continue
				}
			}

			// 批次处理完毕，暂停一小段时间让其他程序有机会执行
			// 避免持续高负载
			time.Sleep(10 * time.Millisecond)
		}

		log.Printf("内存池数据初始化完成，共处理 %d 个交易", len(txids))
	}()
}

// GetUTXOsByAddress 查询地址在内存池中的未花费UTXO
// func (m *MempoolManager) GetUTXOsByAddress(address string) ([]struct {
// 	TxID   string
// 	Index  string
// 	Amount uint64
// }, error) {
// 	result := []struct {
// 		TxID   string
// 		Index  string
// 		Amount uint64
// 	}{}

// 	// 1. 使用后缀查询直接获取与地址相关的所有键
// 	keys, err := m.mempoolIncomeDB.GetKeysWithSuffix(address)
// 	if err != nil {
// 		if errors.Is(err, storage.ErrNotFound) {
// 			return result, nil // 地址没有内存池UTXO
// 		}
// 		return nil, fmt.Errorf("查询地址UTXO失败: %w", err)
// 	}

// 	// 2. 获取已花费的UTXO
// 	spentUtxos := make(map[string]struct{})

// 	// 查询地址在支出数据库中的索引
// 	spendKeys, err := m.mempoolSpendDB.GetKeysWithSuffix(address)
// 	if err == nil && len(spendKeys) > 0 {
// 		for _, key := range spendKeys {
// 			parts := strings.Split(key, "_")
// 			if len(parts) == 2 {
// 				spentUtxos[parts[0]] = struct{}{} // 记录已花费的utxoID
// 			}
// 		}
// 	}

// 	// 3. 处理每个未花费的UTXO
// 	for _, key := range keys {
// 		parts := strings.Split(key, "_")
// 		if len(parts) != 2 {
// 			continue // 格式无效
// 		}

// 		utxoID := parts[0] // txid:index

// 		// 检查是否已被花费
// 		if _, spent := spentUtxos[utxoID]; spent {
// 			continue // 跳过已花费的UTXO
// 		}

// 		// 获取UTXO值
// 		value, err := m.mempoolIncomeDB.Get([]byte(key))
// 		if err != nil {
// 			continue
// 		}

// 		// 解析值 (amount@script_type)
// 		valueStr := string(value)
// 		valueInfo := strings.Split(valueStr, "@")
// 		if len(valueInfo) < 1 {
// 			continue
// 		}

// 		amount, err := strconv.ParseUint(valueInfo[0], 10, 64)
// 		if err != nil {
// 			continue
// 		}

// 		// 跳过小额UTXO (dust)
// 		if amount <= 1000 {
// 			continue
// 		}

// 		// 解析UTXO ID
// 		utxoParts := strings.Split(utxoID, ":")
// 		if len(utxoParts) != 2 {
// 			continue
// 		}

// 		result = append(result, struct {
// 			TxID   string
// 			Index  string
// 			Amount uint64
// 		}{
// 			TxID:   utxoParts[0],
// 			Index:  utxoParts[1],
// 			Amount: amount,
// 		})
// 	}

// 	return result, nil
// }

// CleanAllMempool 清理所有内存池数据，用于完全重建
func (m *MempoolManager) CleanAllMempool() error {
	log.Println("正在通过删除物理文件重置内存池数据...")

	// 保存ZMQ地址，后面需要重建
	zmqAddress := ""
	if m.zmqClient != nil {
		zmqAddress = m.zmqClient.address
	}

	// 使用basePath和固定表名获取数据库文件路径
	incomeDbPath := m.basePath + "/mempool_income"
	spendDbPath := m.basePath + "/mempool_spend"

	// 不再尝试检测数据库状态，直接使用defer和recover处理可能的panic
	defer func() {
		if r := recover(); r != nil {
			log.Printf("清理过程中捕获到异常: %v，继续执行文件删除", r)
		}
	}()

	// 安全关闭数据库连接
	log.Println("关闭现有内存池数据库连接...")
	// 使用recover避免重复关闭导致的panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭收入数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolIncomeDB != nil {
			m.mempoolIncomeDB.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭支出数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolSpendDB != nil {
			m.mempoolSpendDB.Close()
		}
	}()

	// 删除物理文件
	log.Printf("删除内存池收入数据库: %s", incomeDbPath)
	if err := os.RemoveAll(incomeDbPath); err != nil {
		log.Printf("删除内存池收入数据库失败: %v", err)
		// 失败后重新创建连接
		newIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
		if err != nil {
			log.Printf("重新创建内存池收入数据库失败: %v", err)
		} else {
			m.mempoolIncomeDB = newIncomeDB
		}
		return err
	}

	log.Printf("删除内存池支出数据库: %s", spendDbPath)
	if err := os.RemoveAll(spendDbPath); err != nil {
		log.Printf("删除内存池支出数据库失败: %v", err)
		// 失败后重新创建连接
		newIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
		if err != nil {
			log.Printf("重新创建内存池收入数据库失败: %v", err)
		} else {
			m.mempoolIncomeDB = newIncomeDB
		}
		newSpendDB, err := storage.NewSimpleDB(spendDbPath)
		if err != nil {
			log.Printf("重新创建内存池支出数据库失败: %v", err)
		} else {
			m.mempoolSpendDB = newSpendDB
		}
		return err
	}

	// 重新创建数据库
	log.Println("重新创建内存池数据库...")
	newIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
	if err != nil {
		log.Printf("重新创建内存池收入数据库失败: %v", err)
		return err
	}

	newSpendDB, err := storage.NewSimpleDB(spendDbPath)
	if err != nil {
		newIncomeDB.Close()
		log.Printf("重新创建内存池支出数据库失败: %v", err)
		return err
	}

	// 更新数据库引用
	m.mempoolIncomeDB = newIncomeDB
	m.mempoolSpendDB = newSpendDB

	// 重新创建ZMQ客户端
	if zmqAddress != "" {
		log.Println("重新创建ZMQ客户端...")
		m.zmqClient = NewZMQClient(zmqAddress, nil)

		// 重新添加监听主题
		log.Println("重新添加ZMQ监听主题...")
		m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)
	}

	log.Println("内存池数据完全重置成功")
	return nil
}

// GetBasePath 返回内存池数据的基础路径
func (m *MempoolManager) GetBasePath() string {
	return m.basePath
}

// GetZmqAddress 返回ZMQ服务器地址
func (m *MempoolManager) GetZmqAddress() string {
	if m.zmqClient != nil {
		return m.zmqClient.address
	}
	return ""
}

// RebuildMempool 重建内存池数据（删除并重新初始化数据库和ZMQ监听）
func (m *MempoolManager) RebuildMempool() error {
	log.Println("正在通过删除物理文件重建内存池数据...")

	zmqAddress := ""
	if m.zmqClient != nil {
		zmqAddress = m.zmqClient.address
	}

	incomeDbPath := m.basePath + "/mempool_income"
	spendDbPath := m.basePath + "/mempool_spend"

	defer func() {
		if r := recover(); r != nil {
			log.Printf("重建过程中捕获到异常: %v，继续执行文件删除", r)
		}
	}()

	log.Println("关闭现有内存池数据库连接...")
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭收入数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolIncomeDB != nil {
			m.mempoolIncomeDB.Close()
		}
	}()
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("关闭支出数据库时发生错误: %v", r)
			}
		}()
		if m.mempoolSpendDB != nil {
			m.mempoolSpendDB.Close()
		}
	}()

	log.Printf("删除内存池收入数据库: %s", incomeDbPath)
	if err := os.RemoveAll(incomeDbPath); err != nil {
		log.Printf("删除内存池收入数据库失败: %v", err)
		return err
	}

	log.Printf("删除内存池支出数据库: %s", spendDbPath)
	if err := os.RemoveAll(spendDbPath); err != nil {
		log.Printf("删除内存池支出数据库失败: %v", err)
		return err
	}

	log.Println("重新创建内存池数据库...")
	newIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
	if err != nil {
		log.Printf("重新创建内存池收入数据库失败: %v", err)
		return err
	}
	newSpendDB, err := storage.NewSimpleDB(spendDbPath)
	if err != nil {
		newIncomeDB.Close()
		log.Printf("重新创建内存池支出数据库失败: %v", err)
		return err
	}
	m.mempoolIncomeDB = newIncomeDB
	m.mempoolSpendDB = newSpendDB

	if zmqAddress != "" {
		log.Println("重新创建ZMQ客户端...")
		m.zmqClient = NewZMQClient(zmqAddress, nil)
		log.Println("重新添加ZMQ监听主题...")
		m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)
	}

	log.Println("内存池数据完全重建成功")
	return nil
}
