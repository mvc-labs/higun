package indexer

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/contract/meta-contract/decoder"
	"github.com/metaid/utxo_indexer/storage"
)

// FtVerifyManager manages FT-UTXO verification
type FtVerifyManager struct {
	indexer           *ContractFtIndexer
	verifyInterval    time.Duration // Verification interval
	stopChan          chan struct{} // Stop signal channel
	isRunning         bool
	mu                sync.RWMutex
	verifyBatchSize   int   // Number of verifications per batch
	verifyWorkerCount int   // Number of verification worker goroutines
	verifyCount       int64 // Number of verified UTXOs
}

// NewFtVerifyManager creates a new verification manager
func NewFtVerifyManager(indexer *ContractFtIndexer, verifyInterval time.Duration, batchSize, workerCount int) *FtVerifyManager {
	return &FtVerifyManager{
		indexer:           indexer,
		verifyInterval:    verifyInterval,
		stopChan:          make(chan struct{}),
		verifyBatchSize:   batchSize,
		verifyWorkerCount: workerCount,
	}
}

// Start starts the verification manager
func (m *FtVerifyManager) Start() error {
	m.mu.Lock()
	if m.isRunning {
		m.mu.Unlock()
		return fmt.Errorf("Verification manager is already running")
	}
	m.isRunning = true
	m.mu.Unlock()

	go m.verifyLoop()
	return nil
}

// Stop stops the verification manager
func (m *FtVerifyManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunning {
		return
	}

	close(m.stopChan)
	m.isRunning = false
}

// verifyLoop verification loop
func (m *FtVerifyManager) verifyLoop() {
	ticker := time.NewTicker(m.verifyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			if err := m.verifyFtUtxos(); err != nil {
				log.Printf("Failed to verify FT-UTXO: %v", err)
			}
		}
	}
}

// verifyFtUtxos verifies FT-UTXO
func (m *FtVerifyManager) verifyFtUtxos() error {
	// Get unchecked UTXO data
	uncheckData := make(map[string]string)

	// Iterate through all shards
	for _, db := range m.indexer.uncheckFtOutpointStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Collect a batch of data
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

	//Print log, uncheckData count
	// log.Printf("Verifying FT-UTXO, uncheckData count: %d", len(uncheckData))
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
func (m *FtVerifyManager) verifyWorker(utxoChan <-chan struct {
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
func (m *FtVerifyManager) verifyUtxo(outpoint, utxoData string) error {
	// Parse UTXO data
	//value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
	utxoParts := strings.Split(utxoData, "@")
	if len(utxoParts) < 9 {
		return fmt.Errorf("Invalid UTXO data format: %s", utxoData)
	}
	heightStr := utxoParts[8]
	height, err := strconv.Atoi(heightStr)
	if err != nil {
		return fmt.Errorf("Invalid block height: %s", heightStr)
	}

	lastHeight, err := m.indexer.GetLastIndexedHeight()
	if err != nil {
		return fmt.Errorf("Failed to get last indexed height: %w", err)
	}
	if height > lastHeight {
		return nil
	}

	// Parse outpoint to get txId
	txId := strings.Split(outpoint, ":")[0]

	// fmt.Printf("[BLOCK]Verifying UTXO: %s, %s\n", outpoint, utxoData)

	// Get transaction that uses this UTXO from usedFtIncomeStore
	usedData, err := m.indexer.usedFtIncomeStore.Get([]byte(txId))
	if err != nil {
		if err == storage.ErrNotFound {
			err = m.indexer.invalidFtOutpointStore.Set([]byte(outpoint), []byte(utxoData+"@not_used-usedFtIncomeStore_not_found"))
			if err != nil {
				return errors.New("Failed to set invalid UTXO: " + err.Error())
			}
			// If no usage record found, UTXO is unused and can be deleted
			err = m.indexer.uncheckFtOutpointStore.Delete([]byte(outpoint))
			if err != nil {
				return errors.New("Failed to delete unchecked UTXO: " + err.Error())
			}

			return nil
		}
		return errors.New("Failed to get usage record: " + err.Error())
	}
	// fmt.Printf("Found usedData for [%s]: %s\n", txId, string(usedData))

	// Get key information of UTXO
	ftAddress := utxoParts[0]
	codeHash := utxoParts[1]
	genesis := utxoParts[2]
	utxoSensibleId := utxoParts[3]
	// if outpoint == "ab59d2bc50a8d6bcc7c1234c59af54e4ea6d0eab7d2b57009b6ca85f57c52dec:0" {
	// 	fmt.Printf("Found: sensibleId: %s, genesis: %s, codeHash: %s, ftAddress: %s\n", sensibleId, genesis, codeHash, ftAddress)
	// }
	if utxoSensibleId == "000000000000000000000000000000000000000000000000000000000000000000000000" {
		// Match successful, add UTXO to addressFtIncomeValidStore
		if err := m.addToValidStore(ftAddress, utxoData); err != nil {
			return errors.New("Failed to add valid UTXO data: " + err.Error())
		}
		return m.indexer.uncheckFtOutpointStore.Delete([]byte(outpoint))
	}
	// fmt.Printf("sensibleId: %s\n", sensibleId)

	genesisTxId, genesisIndex, err := decoder.ParseSensibleId(utxoSensibleId)
	if err != nil {
		return errors.New("Failed to parse sensibleId: " + err.Error())
	}
	tokenHash := ""
	tokenCodeHash := ""
	genesisHash := ""
	genesisCodeHash := ""
	sensibleId := ""

	usedGenesisOutpoint := genesisTxId + ":" + strconv.Itoa(int(genesisIndex))

	//Get initial genesis UTXO from contractFtGenesisStore
	genesisUtxo, err := m.indexer.contractFtGenesisStore.Get([]byte(usedGenesisOutpoint))
	if err != nil {
		return err
	}
	// fmt.Printf("Found genesisUtxo for [%s]: %s\n", usedGenesisOutpoint, string(genesisUtxo))
	if len(genesisUtxo) > 0 {
		//If exists, get from contractFtGenesisOutputStore
		// key:usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
		genesisOutputs, err := m.indexer.contractFtGenesisOutputStore.Get([]byte(usedGenesisOutpoint))
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to get initial genesis UTXO [%s][%s]: %s", outpoint, usedGenesisOutpoint, err.Error()))
		}
		// fmt.Printf("Found genesisOutputs for [%s]: %s\n", usedGenesisOutpoint, string(genesisOutputs))
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
	// Parse usage records
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

		// Check if codeHash, genesis and sensibleId match
		if usedParts[1] == codeHash && usedParts[2] == genesis && usedParts[3] == utxoSensibleId {
			hasMatch = true
			fmt.Printf("[BLOCK]Successfully matched inputs and output: %s\n", outpoint)
			// Match successful, add UTXO to addressFtIncomeValidStore
			if err := m.addToValidStore(ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[1] == tokenCodeHash && usedParts[2] == tokenHash && usedParts[3] == sensibleId {
			hasMatch = true
			fmt.Printf("[BLOCK]Successfully matched inputs and token: %s\n", outpoint)
			// Match successful, add UTXO to addressFtIncomeValidStore
			if err := m.addToValidStore(ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[1] == genesisCodeHash && usedParts[2] == genesisHash && usedParts[3] == sensibleId {
			hasMatch = true
			fmt.Printf("[BLOCK]Successfully matched inputs and genesis: %s\n", outpoint)
			// Match successful, add UTXO to addressFtIncomeValidStore
			if err := m.addToValidStore(ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
		if usedParts[5] == genesisTxId {
			hasMatch = true
			fmt.Printf("[BLOCK]Successfully matched inputs and genesisTxId: %s\n", outpoint)
			// Match successful, add UTXO to addressFtIncomeValidStore
			if err := m.addToValidStore(ftAddress, utxoData); err != nil {
				return err
			}
			break
		}
	}
	if !hasMatch {
		fmt.Printf("[BLOCK][Failed]Match failed: %s\n", outpoint)
		//Print codeHash,genesis,sensibleId
		fmt.Printf("[BLOCK][Failed]codeHash: %s, genesis: %s, utxoSensibleId: %s\n", codeHash, genesis, utxoSensibleId)
		//Print tokenCodeHash,tokenHash,genesisHash,genesisCodeHash,genesisTxId
		fmt.Printf("[BLOCK][Failed]tokenCodeHash: %s, tokenHash: %s, sensibleId: %s, genesisHash: %s, genesisCodeHash: %s, genesisTxId: %s\n", tokenCodeHash, tokenHash, sensibleId, genesisHash, genesisCodeHash, genesisTxId)
		err = m.indexer.invalidFtOutpointStore.Set([]byte(outpoint), []byte(utxoData+"@not_used-not_match"))
		if err != nil {
			return errors.New("Failed to set invalid UTXO: " + err.Error())
		}
	}

	// Delete verified UTXO
	return m.indexer.uncheckFtOutpointStore.Delete([]byte(outpoint))
}

// addToValidStore adds UTXO to valid storage
// FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
func (m *FtVerifyManager) addToValidStore(ftAddress, utxoData string) error {
	// Extract txId and index from utxoData
	//value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
	utxoParts := strings.Split(utxoData, "@")
	if len(utxoParts) < 9 {
		return fmt.Errorf("Invalid UTXO data format: %s", utxoData)
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

	// Use BulkMergeMapConcurrent method to merge data
	mergeMap := make(map[string][]string)
	mergeMap[ftAddress] = []string{newValue}

	err := m.indexer.addressFtIncomeValidStore.BulkMergeMapConcurrent(&mergeMap, 1)
	if err != nil {
		return errors.New("Failed to merge and update valid UTXO data: " + err.Error())
	}
	fmt.Printf("[BLOCK]Added valid UTXO: %s %s\n", ftAddress, outpoint)

	/* Original code
	var validUtxos []string
	if err == nil {
		validUtxos = strings.Split(string(existingData), ",")
		// Check if same UTXO already exists
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
				// Same UTXO already exists, return directly
				return nil
			}
		}
	}

	// Add new UTXO
	validUtxos = append(validUtxos, newValue)

	// Update storage
	err = m.indexer.addressFtIncomeValidStore.Set([]byte(ftAddress), []byte(strings.Join(validUtxos, ",")))
	if err != nil {
		return errors.New("Failed to update valid UTXO data: " + err.Error())
	}
	*/
	return nil
}
