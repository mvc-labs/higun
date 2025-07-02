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

// FtMempoolVerifier manages verification of FT-UTXO in mempool
type FtMempoolVerifier struct {
	mempoolManager    *FtMempoolManager
	verifyInterval    time.Duration // Verification interval
	stopChan          chan struct{} // Stop signal channel
	isRunning         bool
	mu                sync.RWMutex
	verifyBatchSize   int   // Number of verifications per batch
	verifyWorkerCount int   // Number of verification worker goroutines
	verifyCount       int64 // Number of verified UTXOs
}

// NewFtMempoolVerifier creates a new mempool verification manager
func NewFtMempoolVerifier(mempoolManager *FtMempoolManager, verifyInterval time.Duration, batchSize, workerCount int) *FtMempoolVerifier {
	return &FtMempoolVerifier{
		mempoolManager:    mempoolManager,
		verifyInterval:    verifyInterval,
		stopChan:          make(chan struct{}),
		verifyBatchSize:   batchSize,
		verifyWorkerCount: workerCount,
	}
}

// Start starts the verification manager
func (m *FtMempoolVerifier) Start() error {
	m.mu.Lock()
	if m.isRunning {
		m.mu.Unlock()
		return fmt.Errorf("verification manager is already running")
	}
	m.isRunning = true
	m.mu.Unlock()

	go m.verifyLoop()
	return nil
}

// Stop stops the verification manager
func (m *FtMempoolVerifier) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunning {
		return
	}

	close(m.stopChan)
	m.isRunning = false
}

// verifyLoop verification loop
func (m *FtMempoolVerifier) verifyLoop() {
	ticker := time.NewTicker(m.verifyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			if err := m.verifyMempoolFtUtxos(); err != nil {
				log.Printf("Failed to verify mempool FT-UTXO: %v", err)
			}
		}
	}
}

// verifyMempoolFtUtxos verifies FT-UTXO in mempool
func (m *FtMempoolVerifier) verifyMempoolFtUtxos() error {
	// Get all UTXOs that need verification
	uncheckData := make(map[string]string)

	if m.mempoolManager == nil {
		return fmt.Errorf("mempoolManager is nil")
	}
	if m.mempoolManager.mempoolUncheckFtOutpointStore == nil {
		return fmt.Errorf("mempoolUncheckFtOutpointStore is nil")
	}
	// Get all unchecked UTXOs
	utxoList, err := m.mempoolManager.mempoolUncheckFtOutpointStore.GetFtUtxo()
	if err != nil {
		return fmt.Errorf("Failed to get unchecked UTXOs: %w", err)
	}

	// Collect a batch of data
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

	// log.Printf("Verifying mempool FT-UTXO, uncheckData count: %d", len(uncheckData))
	if len(uncheckData) == 0 {
		return nil
	}

	// Create work channels
	utxoChan := make(chan struct {
		key   string
		value string
	}, len(uncheckData))
	resultChan := make(chan error, len(uncheckData))

	// Start worker goroutines
	for i := 0; i < m.verifyWorkerCount; i++ {
		go m.verifyWorker(utxoChan, resultChan)
	}

	// Send UTXOs to work channel
	for key, value := range uncheckData {
		utxoChan <- struct {
			key   string
			value string
		}{key, value}
	}
	close(utxoChan)

	// Collect results
	var errs []error
	for i := 0; i < len(uncheckData); i++ {
		if err := <-resultChan; err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Errors occurred during verification: %v", errs)
	}
	return nil
}

// verifyWorker verification worker goroutine
func (m *FtMempoolVerifier) verifyWorker(utxoChan <-chan struct {
	key   string
	value string
}, resultChan chan<- error) {
	for utxo := range utxoChan {
		if err := m.verifyUtxo(utxo.key, utxo.value); err != nil {
			resultChan <- fmt.Errorf("Failed to verify UTXO: %w", err)
			continue
		}
		resultChan <- nil
	}
}

// verifyUtxo verifies a single UTXO
func (m *FtMempoolVerifier) verifyUtxo(outpoint, utxoData string) error {
	// Parse outpoint to get txId
	txId := strings.Split(outpoint, ":")[0]

	_, err := m.mempoolManager.mempoolVerifyTxStore.GetSimpleRecord(txId)
	if err != nil {
		if strings.Contains(err.Error(), storage.ErrNotFound.Error()) {
			return nil
		}
		return errors.New("Failed to get VerifyTx [" + txId + "][" + outpoint + "]: " + err.Error())
	}
	fmt.Printf("[MEMPOOL]Verifying mempool UTXO: %s, %s\n", outpoint, utxoData)

	// Get transaction that uses this UTXO from mempoolUsedFtIncomeStore
	usedData, err := m.mempoolManager.mempoolUsedFtIncomeStore.GetSimpleRecord(txId)
	if err != nil {
		if strings.Contains(err.Error(), storage.ErrNotFound.Error()) {
			// If no usage record found, UTXO is unused and can be deleted
			return m.mempoolManager.mempoolUncheckFtOutpointStore.DeleteSimpleRecord(outpoint)
		}
		return err
	}

	// Parse UTXO data
	//value: ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value
	utxoParts := strings.Split(utxoData, "@")
	if len(utxoParts) < 7 {
		return fmt.Errorf("Invalid UTXO data format: %s", utxoData)
	}

	// Get key information of UTXO
	ftAddress := utxoParts[0]
	codeHash := utxoParts[1]
	genesis := utxoParts[2]
	utxoSensibleId := utxoParts[3]
	if utxoSensibleId == "000000000000000000000000000000000000000000000000000000000000000000000000" {
		// Match successful, add UTXO to addressFtIncomeValidStore
		if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
			return errors.New("Failed to add valid UTXO data: " + err.Error())
		}
		return m.mempoolManager.mempoolUncheckFtOutpointStore.DeleteSimpleRecord(outpoint)
	}

	genesisTxId, genesisIndex, err := decoder.ParseSensibleId(utxoSensibleId)
	if err != nil {
		return err
	}
	tokenHash := ""
	tokenCodeHash := ""
	genesisHash := ""
	genesisCodeHash := ""
	sensibleId := ""

	usedOutpoint := genesisTxId + ":" + strconv.Itoa(int(genesisIndex))

	//Get initial genesis UTXO from contractFtGenesisStore
	genesisUtxo, _ := m.mempoolManager.contractFtGenesisStore.Get([]byte(usedOutpoint))
	// if err != nil {
	// 	return err
	// }
	//If genesisUtxo has no data, get from mempoolContractFtUtxoStore
	if len(genesisUtxo) == 0 {
		// genesisUtxo, err = m.mempoolManager.mempoolContractFtGenesisStore.GetFtGenesisByKey(usedOutpoint)
		genesisUtxo, err = m.mempoolManager.mempoolContractFtGenesisStore.GetSimpleRecord(usedOutpoint)
		if err != nil {
			fmt.Printf("[MEMPOOL]Failed to get genesisUtxo in mempoolContractFtGenesisStore: %s, %s\n", outpoint, utxoData)
			return err
		}
	}

	if len(genesisUtxo) > 0 {
		//If exists, get from contractFtGenesisOutputStore
		// key:usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
		genesisOutputs, _ := m.mempoolManager.contractFtGenesisOutputStore.Get([]byte(usedOutpoint))
		// if err != nil {
		// 	fmt.Printf("[MEMPOOL]Failed to get genesisOutputs in contractFtGenesisOutputStore: %s, %s\n", outpoint, utxoData)
		// 	return err
		// }
		if len(genesisOutputs) == 0 {
			// genesisOutputs, err = m.mempoolManager.mempoolContractFtGenesisOutputStore.GetFtGenesisOutputsByKey(usedOutpoint)
			genesisOutputs, err = m.mempoolManager.mempoolContractFtGenesisOutputStore.GetSimpleRecord(usedOutpoint)

			if err != nil {
				fmt.Printf("[MEMPOOL]Failed to get genesisOutputs in mempoolContractFtGenesisOutputStore: %s, %s\n", outpoint, utxoData)
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

	// Parse usage records
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

		// Check if codeHash, genesis and sensibleId match
		if usedParts[1] == codeHash && usedParts[2] == genesis && usedParts[3] == utxoSensibleId {
			fmt.Printf("[MEMPOOL]Successfully matched inputs and output: %s, %s\n", outpoint, utxoData)
			// Match successful, add UTXO to addressFtIncomeValidStore
			if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[1] == tokenCodeHash && usedParts[2] == tokenHash && usedParts[3] == sensibleId {
			fmt.Printf("[MEMPOOL]Successfully matched inputs and token: %s, %s\n", outpoint, utxoData)
			// Match successful, add UTXO to addressFtIncomeValidStore
			if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[1] == genesisCodeHash && usedParts[2] == genesisHash && usedParts[3] == sensibleId {
			fmt.Printf("[MEMPOOL]Successfully matched inputs and genesis: %s, %s\n", outpoint, utxoData)
			// Match successful, add UTXO to addressFtIncomeValidStore
			if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[5] == genesisTxId {
			fmt.Printf("[MEMPOOL]Successfully matched inputs and genesisTxId: %s, %s\n", outpoint, utxoData)
			// Match successful, add UTXO to addressFtIncomeValidStore
			if err := m.addToValidStore(outpoint, ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
	}

	// Delete verified UTXO
	return m.mempoolManager.mempoolUncheckFtOutpointStore.DeleteSimpleRecord(outpoint)
}

// addToValidStore adds UTXO to valid storage
// value:ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value
func (m *FtMempoolVerifier) addToValidStore(outpoint, ftAddress, utxoData string) error {
	// value:ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value
	utxoParts := strings.Split(utxoData, "@")
	if len(utxoParts) < 7 {
		return fmt.Errorf("Invalid UTXO data format: %s", utxoData)
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
