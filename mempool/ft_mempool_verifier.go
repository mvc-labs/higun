package mempool

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/contract/meta-contract/decoder"
	"github.com/metaid/utxo_indexer/storage"
)

// FtMempoolVerifier 管理内存池中FT-UTXO的验证
type FtMempoolVerifier struct {
	mempoolManager    *FtMempoolManager
	verifyInterval    time.Duration // 验证间隔时间
	stopChan          chan struct{} // 停止信号通道
	isRunning         bool
	mu                sync.RWMutex
	verifyBatchSize   int   // 每批验证的数量
	verifyWorkerCount int   // 验证工作协程数
	verifyCount       int64 // 已验证的UTXO数量
}

// NewFtMempoolVerifier 创建新的内存池验证管理器
func NewFtMempoolVerifier(mempoolManager *FtMempoolManager, verifyInterval time.Duration, batchSize, workerCount int) *FtMempoolVerifier {
	return &FtMempoolVerifier{
		mempoolManager:    mempoolManager,
		verifyInterval:    verifyInterval,
		stopChan:          make(chan struct{}),
		verifyBatchSize:   batchSize,
		verifyWorkerCount: workerCount,
	}
}

// Start 启动验证管理器
func (m *FtMempoolVerifier) Start() error {
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
func (m *FtMempoolVerifier) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunning {
		return
	}

	close(m.stopChan)
	m.isRunning = false
}

// verifyLoop 验证循环
func (m *FtMempoolVerifier) verifyLoop() {
	ticker := time.NewTicker(m.verifyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			if err := m.verifyMempoolFtUtxos(); err != nil {
				log.Printf("验证内存池FT-UTXO失败: %v", err)
			}
		}
	}
}

// verifyMempoolFtUtxos 验证内存池中的FT-UTXO
func (m *FtMempoolVerifier) verifyMempoolFtUtxos() error {
	// 获取所有需要验证的UTXO
	uncheckData := make(map[string]string)

	// 获取所有未检查的UTXO
	utxoList, err := m.mempoolManager.mempoolUncheckFtOutpointStore.GetFtUtxo()
	if err != nil {
		return fmt.Errorf("获取未检查UTXO失败: %w", err)
	}

	// 收集一批数据
	count := 0
	for _, utxo := range utxoList {
		outpoint := utxo.TxID + ":" + utxo.Index
		//value:ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value
		utxoData := common.ConcatBytesOptimized([]string{
			utxo.Address,
			utxo.CodeHash,
			utxo.Genesis,
			utxo.SensibleId,
			utxo.Amount,
			utxo.Index,
			utxo.Value,
		}, "@")
		uncheckData[outpoint] = utxoData
		count++

		if count >= m.verifyBatchSize {
			break
		}
	}

	log.Printf("验证内存池FT-UTXO，uncheckData数量: %d", len(uncheckData))
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
func (m *FtMempoolVerifier) verifyWorker(utxoChan <-chan struct {
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
func (m *FtMempoolVerifier) verifyUtxo(outpoint, utxoData string) error {
	// 解析outpoint获取txId
	txId := strings.Split(outpoint, ":")[0]

	_, err := m.mempoolManager.mempoolVerifyTxStore.Get(txId + "_")
	if err != nil {
		if err == storage.ErrNotFound {
			return nil
		}
		return err
	}
	fmt.Printf("验证UTXO: %s, %s\n", outpoint, utxoData)

	// 从mempoolUsedFtIncomeStore获取使用该UTXO的交易
	usedData, err := m.mempoolManager.mempoolUsedFtIncomeStore.Get(txId)
	if err != nil {
		if err == storage.ErrNotFound {
			// 如果找不到使用记录，说明UTXO未被使用，可以删除
			return m.mempoolManager.mempoolUncheckFtOutpointStore.DeleteRecord(outpoint, "")
		}
		return err
	}

	// 解析UTXO数据
	//value: ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value
	utxoParts := strings.Split(utxoData, "@")
	if len(utxoParts) < 7 {
		return fmt.Errorf("无效的UTXO数据格式: %s", utxoData)
	}

	// 获取UTXO的关键信息
	ftAddress := utxoParts[0]
	codeHash := utxoParts[1]
	genesis := utxoParts[2]
	sensibleId := utxoParts[3]
	if sensibleId == "000000000000000000000000000000000000000000000000000000000000000000000000" {
		// 匹配成功，将UTXO添加到addressFtIncomeValidStore
		if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
			return errors.New("添加有效UTXO数据失败: " + err.Error())
		}
		return m.mempoolManager.mempoolUncheckFtOutpointStore.DeleteRecord(outpoint, "")
	}

	genesisTxId, genesisIndex, err := decoder.ParseSensibleId(sensibleId)
	if err != nil {
		return err
	}
	tokenHash := ""
	tokenCodeHash := ""
	genesisHash := ""
	genesisCodeHash := ""

	usedOutpoint := genesisTxId + ":" + strconv.Itoa(int(genesisIndex))

	//从contractFtGenesisStore获取初始创世UTXO
	genesisUtxo, _ := m.mempoolManager.contractFtGenesisStore.Get([]byte(usedOutpoint))
	// if err != nil {
	// 	return err
	// }
	//如果genesisUtxo没有数据，则从mempoolContractFtUtxoStore获取
	if len(genesisUtxo) == 0 {
		genesisUtxo, err = m.mempoolManager.mempoolContractFtGenesisStore.GetFtGenesisByKey(usedOutpoint)
		if err != nil {
			return err
		}
	}

	if len(genesisUtxo) > 0 {
		//如果有，就从contractFtGenesisOutputStore里面获取
		// key:usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
		genesisOutputs, err := m.mempoolManager.contractFtGenesisOutputStore.Get([]byte(usedOutpoint))
		if err != nil {
			return err
		}
		if len(genesisOutputs) == 0 {
			genesisOutputs, err = m.mempoolManager.mempoolContractFtGenesisOutputStore.GetFtGenesisOutputsByKey(usedOutpoint)
			if err != nil {
				return err
			}
		}
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

	// 解析使用记录
	usedList := strings.Split(string(usedData), ",")
	for _, used := range usedList {
		if used == "" {
			continue
		}
		// value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
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
			fmt.Printf("匹配intputs和output成功: %s, %s\n", outpoint, utxoData)
			// 匹配成功，将UTXO添加到addressFtIncomeValidStore
			if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[1] == tokenCodeHash && usedParts[2] == tokenHash && usedParts[3] == sensibleId {
			fmt.Printf("匹配intputs和token成功: %s, %s\n", outpoint, utxoData)
			// 匹配成功，将UTXO添加到addressFtIncomeValidStore
			if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[1] == genesisCodeHash && usedParts[2] == genesisHash && usedParts[3] == sensibleId {
			fmt.Printf("匹配intputs和genesis成功: %s, %s\n", outpoint, utxoData)
			// 匹配成功，将UTXO添加到addressFtIncomeValidStore
			if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[5] == genesisTxId {
			fmt.Printf("匹配intputs和genesisTxId成功: %s, %s\n", outpoint, utxoData)
			// 匹配成功，将UTXO添加到addressFtIncomeValidStore
			if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
	}

	// 删除已验证的UTXO
	return m.mempoolManager.mempoolUncheckFtOutpointStore.DeleteRecord(outpoint, "")
}

// addToValidStore 添加UTXO到有效存储
// value:ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value
func (m *FtMempoolVerifier) addToValidStore(outpoint, ftAddress, utxoData string) error {
	// value:ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value
	utxoParts := strings.Split(utxoData, "@")
	if len(utxoParts) < 7 {
		return fmt.Errorf("无效的UTXO数据格式: %s", utxoData)
	}
	//value:CodeHash@Genesis@sensibleId@Amount@Index@Value
	newValue := common.ConcatBytesOptimized([]string{
		utxoParts[1],
		utxoParts[2],
		utxoParts[3],
		utxoParts[4],
		utxoParts[5],
		utxoParts[6],
	}, "@")
	return m.mempoolManager.mempoolAddressFtIncomeValidStore.AddRecord(outpoint, ftAddress, []byte(newValue))
}
