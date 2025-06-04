package indexer

// import (
// 	"fmt"
// 	"log"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"time"

// 	"github.com/metaid/utxo_indexer/storage"
// )

// // FtVerifyManager 管理FT-UTXO验证
// type FtVerifyManager struct {
// 	indexer           *ContractFtIndexer
// 	verifyInterval    time.Duration // 验证间隔时间
// 	stopChan          chan struct{} // 停止信号通道
// 	isRunning         bool
// 	mu                sync.RWMutex
// 	lastVerifyHeight  int64  // 上次验证的高度
// 	verifyBatchSize   int    // 每批验证的数量
// 	verifyWorkerCount int    // 验证工作协程数
// 	currentAddress    string // 当前正在验证的地址
// 	verifyCount       int64  // 已验证的UTXO数量
// }

// // NewFtVerifyManager 创建新的验证管理器
// func NewFtVerifyManager(indexer *ContractFtIndexer, verifyInterval time.Duration, batchSize, workerCount int) *FtVerifyManager {
// 	return &FtVerifyManager{
// 		indexer:           indexer,
// 		verifyInterval:    verifyInterval,
// 		stopChan:          make(chan struct{}),
// 		verifyBatchSize:   batchSize,
// 		verifyWorkerCount: workerCount,
// 	}
// }

// // Start 启动验证管理器
// func (m *FtVerifyManager) Start() error {
// 	m.mu.Lock()
// 	if m.isRunning {
// 		m.mu.Unlock()
// 		return fmt.Errorf("验证管理器已经在运行")
// 	}
// 	m.isRunning = true
// 	m.mu.Unlock()

// 	// 恢复验证进度
// 	verifyData, err := m.indexer.addressFtVerifyStore.Get([]byte("verify_progress"))
// 	if err == nil {
// 		parts := strings.Split(string(verifyData), "@")
// 		if len(parts) >= 2 {
// 			m.currentAddress = parts[0]
// 			m.verifyCount, _ = strconv.ParseInt(parts[1], 10, 64)
// 		}
// 	}

// 	go m.verifyLoop()
// 	return nil
// }

// // Stop 停止验证管理器
// func (m *FtVerifyManager) Stop() {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()

// 	if !m.isRunning {
// 		return
// 	}

// 	close(m.stopChan)
// 	m.isRunning = false
// }

// // ResetVerifyProgress 重置验证进度
// func (m *FtVerifyManager) ResetVerifyProgress() error {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()

// 	m.currentAddress = ""
// 	m.verifyCount = 0
// 	return m.indexer.addressFtVerifyStore.Delete([]byte("verify_progress"))
// }

// // verifyLoop 验证循环
// func (m *FtVerifyManager) verifyLoop() {
// 	ticker := time.NewTicker(m.verifyInterval)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-m.stopChan:
// 			return
// 		case <-ticker.C:
// 			if err := m.verifyFtUtxos(); err != nil {
// 				log.Printf("验证FT-UTXO失败: %v", err)
// 			}
// 		}
// 	}
// }

// // verifyFtUtxos 验证FT-UTXO
// func (m *FtVerifyManager) verifyFtUtxos() error {
// 	// 获取所有需要验证的地址
// 	addresses, err := m.getAllAddresses()
// 	if err != nil {
// 		return fmt.Errorf("获取地址列表失败: %w", err)
// 	}

// 	// 按批次处理地址
// 	for i := 0; i < len(addresses); i += m.verifyBatchSize {
// 		end := i + m.verifyBatchSize
// 		if end > len(addresses) {
// 			end = len(addresses)
// 		}

// 		batchAddresses := addresses[i:end]
// 		if err := m.verifyAddressBatch(batchAddresses); err != nil {
// 			log.Printf("验证地址批次失败: %v", err)
// 			continue
// 		}
// 	}

// 	return nil
// }

// // // getAllAddresses 获取所有需要验证的地址
// // func (m *FtVerifyManager) getAllAddresses() ([]string, error) {
// // 	// 从addressFtVerifyStore获取上次验证的进度
// // 	verifyData, err := m.indexer.addressFtVerifyStore.Get([]byte("verify_progress"))
// // 	if err != nil && err != storage.ErrNotFound {
// // 		return nil, fmt.Errorf("获取验证进度失败: %w", err)
// // 	}

// // 	var startAddress string
// // 	var verifyCount int64
// // 	if err == nil {
// // 		// 解析验证进度数据
// // 		parts := strings.Split(string(verifyData), "@")
// // 		if len(parts) >= 2 {
// // 			startAddress = parts[0]
// // 			verifyCount, _ = strconv.ParseInt(parts[1], 10, 64)
// // 		}
// // 	}

// // 	// 获取所有地址
// // 	addresses := make([]string, 0)
// // 	// iter := m.indexer.addressFtIncomeStore.NewIterator()
// // 	// defer iter.Close()

// // 	// foundStart := startAddress == ""
// // 	// for iter.First(); iter.Valid(); iter.Next() {
// // 	// 	addr := string(iter.Key())
// // 	// 	if !foundStart {
// // 	// 		if addr == startAddress {
// // 	// 			foundStart = true
// // 	// 		} else {
// // 	// 			continue
// // 	// 		}
// // 	// 	}
// // 	// 	addresses = append(addresses, addr)
// // 	// }

// // 	m.currentAddress = startAddress
// // 	m.verifyCount = verifyCount
// // 	return addresses, nil
// // }

// // verifyAddressBatch 验证一批地址的FT-UTXO
// func (m *FtVerifyManager) verifyAddressBatch(addresses []string) error {
// 	// 创建工作通道
// 	addressChan := make(chan string, len(addresses))
// 	resultChan := make(chan error, len(addresses))

// 	// 启动工作协程
// 	for i := 0; i < m.verifyWorkerCount; i++ {
// 		go m.verifyWorker(addressChan, resultChan)
// 	}

// 	// 发送地址到工作通道
// 	for _, addr := range addresses {
// 		addressChan <- addr
// 	}
// 	close(addressChan)

// 	// 收集结果
// 	var errs []error
// 	for i := 0; i < len(addresses); i++ {
// 		if err := <-resultChan; err != nil {
// 			errs = append(errs, err)
// 		}
// 	}

// 	if len(errs) > 0 {
// 		return fmt.Errorf("验证过程中出现错误: %v", errs)
// 	}
// 	return nil
// }

// // verifyWorker 验证工作协程
// func (m *FtVerifyManager) verifyWorker(addressChan <-chan string, resultChan chan<- error) {
// 	for addr := range addressChan {
// 		if err := m.verifyAddress(addr); err != nil {
// 			resultChan <- fmt.Errorf("验证地址 %s 失败: %w", addr, err)
// 			continue
// 		}
// 		resultChan <- nil
// 	}
// }

// // verifyAddress 验证单个地址的FT-UTXO
// func (m *FtVerifyManager) verifyAddress(address string) error {
// 	// 从addressFtIncomeStore获取UTXO数据
// 	utxoData, err := m.indexer.addressFtIncomeStore.Get([]byte(address))
// 	if err != nil {
// 		if err == storage.ErrNotFound {
// 			return nil
// 		}
// 		return err
// 	}

// 	// 解析UTXO数据
// 	utxos := strings.Split(string(utxoData), ",")
// 	validUtxos := make([]string, 0, len(utxos))

// 	// 验证每个UTXO
// 	for _, utxo := range utxos {
// 		if utxo == "" {
// 			continue
// 		}

// 		if m.isValidUtxo(utxo) {
// 			validUtxos = append(validUtxos, utxo)
// 		}
// 		m.verifyCount++
// 	}

// 	// 将有效的UTXO存储到addressFtIncomeValidStore
// 	if len(validUtxos) > 0 {
// 		validData := strings.Join(validUtxos, ",")
// 		if err := m.indexer.addressFtIncomeValidStore.Set([]byte(address), []byte(validData)); err != nil {
// 			return fmt.Errorf("存储有效UTXO失败: %w", err)
// 		}
// 	}

// 	// 更新验证进度
// 	progressData := fmt.Sprintf("%s@%d@%d", address, m.verifyCount, time.Now().Unix())
// 	if err := m.indexer.addressFtVerifyStore.Set([]byte("verify_progress"), []byte(progressData)); err != nil {
// 		return fmt.Errorf("更新验证进度失败: %w", err)
// 	}

// 	return nil
// }

// // isValidUtxo 验证UTXO是否有效
// func (m *FtVerifyManager) isValidUtxo(utxo string) bool {
// 	// TODO: 实现UTXO验证逻辑
// 	// 1. 解析UTXO数据
// 	// 2. 检查UTXO是否满足验证条件
// 	// 3. 返回验证结果
// 	return false
// }
