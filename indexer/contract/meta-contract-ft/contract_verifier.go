package indexer

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/contract/meta-contract/decoder"
	"github.com/metaid/utxo_indexer/storage"
)

// FtVerifyManager 管理FT-UTXO验证
type FtVerifyManager struct {
	indexer           *ContractFtIndexer
	verifyInterval    time.Duration // 验证间隔时间
	stopChan          chan struct{} // 停止信号通道
	isRunning         bool
	mu                sync.RWMutex
	verifyBatchSize   int   // 每批验证的数量
	verifyWorkerCount int   // 验证工作协程数
	verifyCount       int64 // 已验证的UTXO数量
}

// NewFtVerifyManager 创建新的验证管理器
func NewFtVerifyManager(indexer *ContractFtIndexer, verifyInterval time.Duration, batchSize, workerCount int) *FtVerifyManager {
	return &FtVerifyManager{
		indexer:           indexer,
		verifyInterval:    verifyInterval,
		stopChan:          make(chan struct{}),
		verifyBatchSize:   batchSize,
		verifyWorkerCount: workerCount,
	}
}

// Start 启动验证管理器
func (m *FtVerifyManager) Start() error {
	m.mu.Lock()
	if m.isRunning {
		m.mu.Unlock()
		return fmt.Errorf("验证管理器已经在运行")
	}
	m.isRunning = true
	m.mu.Unlock()

	go m.verifyLoop()
	return nil
}

// Stop 停止验证管理器
func (m *FtVerifyManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunning {
		return
	}

	close(m.stopChan)
	m.isRunning = false
}

// verifyLoop 验证循环
func (m *FtVerifyManager) verifyLoop() {
	ticker := time.NewTicker(m.verifyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			if err := m.verifyFtUtxos(); err != nil {
				log.Printf("验证FT-UTXO失败: %v", err)
			}
		}
	}
}

// verifyFtUtxos 验证FT-UTXO
func (m *FtVerifyManager) verifyFtUtxos() error {
	// 获取未检查的UTXO数据
	uncheckData := make(map[string]string)

	// 遍历所有分片
	for _, db := range m.indexer.uncheckFtOutpointStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return fmt.Errorf("创建迭代器失败: %w", err)
		}
		defer iter.Close()

		// 收集一批数据
		count := 0
		for iter.First(); iter.Valid(); iter.Next() {
			//key: outpoint
			//value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
			key := string(iter.Key())
			value := string(iter.Value())
			uncheckData[key] = value
			count++

			if count >= m.verifyBatchSize {
				break
			}
		}

		if count >= m.verifyBatchSize {
			break
		}
	}

	//打印日志，uncheckData数量
	// log.Printf("验证FT-UTXO，uncheckData数量: %d", len(uncheckData))
	if len(uncheckData) == 0 {
		return nil
	}

	// 创建工作通道
	utxoChan := make(chan struct {
		key   string
		value string
	}, len(uncheckData))
	resultChan := make(chan error, len(uncheckData))

	// 启动工作协程
	for i := 0; i < m.verifyWorkerCount; i++ {
		go m.verifyWorker(utxoChan, resultChan)
	}

	// 发送UTXO到工作通道
	for key, value := range uncheckData {
		utxoChan <- struct {
			key   string
			value string
		}{key, value}
	}
	close(utxoChan)

	// 收集结果
	var errs []error
	for i := 0; i < len(uncheckData); i++ {
		if err := <-resultChan; err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("验证过程中出现错误: %v", errs)
	}
	return nil
}

// verifyWorker 验证工作协程
func (m *FtVerifyManager) verifyWorker(utxoChan <-chan struct {
	key   string
	value string
}, resultChan chan<- error) {
	for utxo := range utxoChan {
		if err := m.verifyUtxo(utxo.key, utxo.value); err != nil {
			resultChan <- fmt.Errorf("验证UTXO失败: %w", err)
			continue
		}
		resultChan <- nil
	}
}

// verifyUtxo 验证单个UTXO
func (m *FtVerifyManager) verifyUtxo(outpoint, utxoData string) error {
	// 解析UTXO数据h
	//value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
	utxoParts := strings.Split(utxoData, "@")
	if len(utxoParts) < 9 {
		return fmt.Errorf("无效的UTXO数据格式: %s", utxoData)
	}
	heightStr := utxoParts[8]
	height, err := strconv.Atoi(heightStr)
	if err != nil {
		return fmt.Errorf("无效块高：%s", heightStr)
	}

	lastHeight, err := m.indexer.GetLastIndexedHeight()
	if err != nil {
		return fmt.Errorf("获取上次索引高度失败: %w", err)
	}
	if height > lastHeight {
		return nil
	}

	// 解析outpoint获取txId
	txId := strings.Split(outpoint, ":")[0]

	// fmt.Printf("[BLOCK]验证UTXO: %s, %s\n", outpoint, utxoData)

	// 从usedFtIncomeStore获取使用该UTXO的交易
	usedData, err := m.indexer.usedFtIncomeStore.Get([]byte(txId))
	if err != nil {
		if err == storage.ErrNotFound {
			err = m.indexer.invalidFtOutpointStore.Set([]byte(outpoint), []byte(utxoData+"@not_used-usedFtIncomeStore_not_found"))
			if err != nil {
				return errors.New("设置无效的UTXO失败: " + err.Error())
			}
			// 如果找不到使用记录，说明UTXO未被使用，可以删除
			err = m.indexer.uncheckFtOutpointStore.Delete([]byte(outpoint))
			if err != nil {
				return errors.New("删除未检查的UTXO失败: " + err.Error())
			}

			return nil
		}
		return errors.New("获取使用记录失败: " + err.Error())
	}
	// fmt.Printf("找到[%s]的usedData: %s\n", txId, string(usedData))

	// 获取UTXO的关键信息
	ftAddress := utxoParts[0]
	codeHash := utxoParts[1]
	genesis := utxoParts[2]
	sensibleId := utxoParts[3]
	// if outpoint == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec:0" {
	// 	fmt.Printf("找到：sensibleId: %s, genesis: %s, codeHash: %s, ftAddress: %s\n", sensibleId, genesis, codeHash, ftAddress)
	// }
	if sensibleId == "000000000000000000000000000000000000000000000000000000000000000000000000" {
		// 匹配成功，将UTXO添加到addressFtIncomeValidStore
		if err := m.addToValidStore(ftAddress, utxoData); err != nil {
			return errors.New("添加有效UTXO数据失败: " + err.Error())
		}
		return m.indexer.uncheckFtOutpointStore.Delete([]byte(outpoint))
	}
	// fmt.Printf("sensibleId: %s\n", sensibleId)

	genesisTxId, genesisIndex, err := decoder.ParseSensibleId(sensibleId)
	if err != nil {
		return errors.New("解析sensibleId失败: " + err.Error())
	}
	tokenHash := ""
	tokenCodeHash := ""
	genesisHash := ""
	genesisCodeHash := ""

	usedGenesisOutpoint := genesisTxId + ":" + strconv.Itoa(int(genesisIndex))

	//从contractFtGenesisStore获取初始创世UTXO
	genesisUtxo, err := m.indexer.contractFtGenesisStore.Get([]byte(usedGenesisOutpoint))
	if err != nil {
		return err
	}
	// fmt.Printf("找到[%s]的genesisUtxo: %s\n", usedGenesisOutpoint, string(genesisUtxo))
	if len(genesisUtxo) > 0 {
		//如果有，就从contractFtGenesisOutputStore里面获取
		// key:usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
		genesisOutputs, err := m.indexer.contractFtGenesisOutputStore.Get([]byte(usedGenesisOutpoint))
		if err != nil {
			return errors.New(fmt.Sprintf("获取初始创世UTXO失败[%s][%s]: %s", outpoint, usedGenesisOutpoint, err.Error()))
		}
		// fmt.Printf("找到[%s]的genesisOutputs: %s\n", usedGenesisOutpoint, string(genesisOutputs))
		if len(genesisOutputs) > 0 {
			genesisOutputParts := strings.Split(string(genesisOutputs), ",")
			for _, genesisOutput := range genesisOutputParts {
				if genesisOutput == "" {
					continue
				}
				//sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value
				genesisOutputParts := strings.Split(genesisOutput, "@")
				if len(genesisOutputParts) < 10 {
					continue
				}
				/*
									 // token
					            if (txOutFtEntity.codeHash === txOutFt.codeHash) {
					              tokenHash = txOutFtEntity.genesis;
					              tokenCodeHash = txOutFtEntity.codeHash;
					              sensibleId = txOutFtEntity.sensibleId;
					            }
					            // new genesis
					            if (txOutFtEntity.value === '0') {
					              genesisHash = txOutFtEntity.genesis;
					              genesisCodeHash = txOutFtEntity.codeHash;
					              sensibleId = txOutFtEntity.sensibleId;
					            }
				*/
				if genesisOutputParts[4] == codeHash {
					tokenHash = genesisOutputParts[5]
					tokenCodeHash = genesisOutputParts[4]
					sensibleId = genesisOutputParts[0]
				}
				if genesisOutputParts[6] == "0" {
					genesisHash = genesisOutputParts[5]
					genesisCodeHash = genesisOutputParts[4]
					sensibleId = genesisOutputParts[0]
				}
			}
		}
	}

	hasMatch := false
	// 解析使用记录
	usedList := strings.Split(string(usedData), ",")
	for _, used := range usedList {
		if used == "" {
			continue
		}

		//value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
		usedParts := strings.Split(used, "@")
		if len(usedParts) < 9 {
			continue
		}

		/*
					if (usedTxo && usedTxo.length > 0) {
			      for (const txo of usedTxo) {
			        if (
			          (txo.genesis == txOut.genesis &&
			            txo.codeHash == txOut.codeHash &&
			            txo.sensibleId == txOut.sensibleId) ||
			          (txo.genesis == tokenHash &&
			            txo.codeHash == tokenCodeHash &&
			            txo.sensibleId === sensibleId) ||
			          (txo.genesis == genesisHash &&
			            txo.codeHash == genesisCodeHash &&
			            txo.sensibleId === sensibleId) ||
			          txo.txid == genesisTxId
			        ) {
			          return true;
			        }
			      }
			    }
			    return false;
		*/

		// 检查codeHash、genesis和sensibleId是否匹配
		if usedParts[1] == codeHash && usedParts[2] == genesis && usedParts[3] == sensibleId {
			fmt.Printf("[BLOCK]匹配intputs和output成功: %s\n", outpoint)
			// 匹配成功，将UTXO添加到addressFtIncomeValidStore
			if err := m.addToValidStore(ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[1] == tokenCodeHash && usedParts[2] == tokenHash && usedParts[3] == sensibleId {
			hasMatch = true
			fmt.Printf("[BLOCK]匹配intputs和token成功: %s\n", outpoint)
			// 匹配成功，将UTXO添加到addressFtIncomeValidStore
			if err := m.addToValidStore(ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[1] == genesisCodeHash && usedParts[2] == genesisHash && usedParts[3] == sensibleId {
			hasMatch = true
			fmt.Printf("[BLOCK]匹配intputs和genesis成功: %s\n", outpoint)
			// 匹配成功，将UTXO添加到addressFtIncomeValidStore
			if err := m.addToValidStore(ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[5] == genesisTxId {
			hasMatch = true
			fmt.Printf("[BLOCK]匹配intputs和genesisTxId成功: %s\n", outpoint)
			// 匹配成功，将UTXO添加到addressFtIncomeValidStore
			if err := m.addToValidStore(ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
	}
	if !hasMatch {
		//打印codeHash,genesis,sensibleId
		// fmt.Printf("[BLOCK]codeHash: %s, genesis: %s, sensibleId: %s\n", codeHash, genesis, sensibleId)
		//打印 tokenCodeHash,tokenHash,genesisHash,genesisCodeHash,genesisTxId
		// fmt.Printf("[BLOCK]tokenCodeHash: %s, tokenHash: %s, genesisHash: %s, genesisCodeHash: %s, genesisTxId: %s\n", tokenCodeHash, tokenHash, genesisHash, genesisCodeHash, genesisTxId)
		err = m.indexer.invalidFtOutpointStore.Set([]byte(outpoint), []byte(utxoData+"@not_used-not_match"))
		if err != nil {
			return errors.New("设置无效的UTXO失败: " + err.Error())
		}
	}

	// 删除已验证的UTXO
	return m.indexer.uncheckFtOutpointStore.Delete([]byte(outpoint))
}

// addToValidStore 添加UTXO到有效存储
// FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
func (m *FtVerifyManager) addToValidStore(ftAddress, utxoData string) error {
	// 从 utxoData 中提取 txId 和 index
	//value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
	utxoParts := strings.Split(utxoData, "@")
	if len(utxoParts) < 9 {
		return fmt.Errorf("无效的UTXO数据格式: %s", utxoData)
	}

	//newValue: CodeHash@Genesis@Amount@TxID@Index@Value@height
	newValue := common.ConcatBytesOptimized(
		[]string{
			utxoParts[1],
			utxoParts[2],
			utxoParts[4],
			utxoParts[5],
			utxoParts[6],
			utxoParts[7],
			utxoParts[8],
		},
		"@",
	)
	outpoint := utxoParts[5] + ":" + utxoParts[6]

	// 使用BulkMergeMapConcurrent方式合并数据
	mergeMap := make(map[string][]string)
	mergeMap[ftAddress] = []string{newValue}

	err := m.indexer.addressFtIncomeValidStore.BulkMergeMapConcurrent(&mergeMap, 1)
	if err != nil {
		return errors.New("合并更新有效UTXO数据失败: " + err.Error())
	}
	fmt.Printf("[BLOCK]加入有效UTXO:%s %s\n", ftAddress, outpoint)

	/* 原来的代码
	var validUtxos []string
	if err == nil {
		validUtxos = strings.Split(string(existingData), ",")
		// 检查是否已存在相同的 UTXO
		for _, existingUtxo := range validUtxos {
			if existingUtxo == "" {
				continue
			}
			//value: CodeHash@Genesis@Amount@TxID@Index@Value@height
			existingParts := strings.Split(existingUtxo, "@")
			if len(existingParts) < 7 {
				continue
			}
			existingTxId := existingParts[3]
			existingIndex := existingParts[4]
			existingOutpoint := existingTxId + ":" + existingIndex

			if existingOutpoint == currentOutpoint {
				// 已存在相同的 UTXO，直接返回
				return nil
			}
		}
	}

	// 添加新的UTXO
	validUtxos = append(validUtxos, newValue)

	// 更新存储
	err = m.indexer.addressFtIncomeValidStore.Set([]byte(ftAddress), []byte(strings.Join(validUtxos, ",")))
	if err != nil {
		return errors.New("更新有效UTXO数据失败: " + err.Error())
	}
	*/
	return nil
}
