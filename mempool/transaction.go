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

// MempoolManager manages mempool transactions
type MempoolManager struct {
	utxoStore       *storage.PebbleStore // Main UTXO storage, using sharding
	mempoolIncomeDB *storage.SimpleDB    // Mempool income database
	mempoolSpendDB  *storage.SimpleDB    // Mempool spend database
	chainCfg        *chaincfg.Params
	zmqClient       *ZMQClient
	basePath        string // Data directory base path
}

// NewMempoolManager creates a new mempool manager
func NewMempoolManager(basePath string, utxoStore *storage.PebbleStore, chainCfg *chaincfg.Params, zmqAddress string) *MempoolManager {
	mempoolIncomeDB, err := storage.NewSimpleDB(basePath + "/mempool_income")
	if err != nil {
		log.Printf("Failed to create mempool income database: %v", err)
		return nil
	}

	mempoolSpendDB, err := storage.NewSimpleDB(basePath + "/mempool_spend")
	if err != nil {
		log.Printf("Failed to create mempool spend database: %v", err)
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

	// Create ZMQ client, no longer passing db
	m.zmqClient = NewZMQClient(zmqAddress, nil)

	// Add "rawtx" topic monitoring
	m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)

	return m
}

// Start starts the mempool manager
func (m *MempoolManager) Start() error {
	return m.zmqClient.Start()
}

// Stop stops the mempool manager
func (m *MempoolManager) Stop() {
	m.zmqClient.Stop()
	if m.mempoolIncomeDB != nil {
		m.mempoolIncomeDB.Close()
	}
	if m.mempoolSpendDB != nil {
		m.mempoolSpendDB.Close()
	}
}

// HandleRawTransaction processes raw transaction data
func (m *MempoolManager) HandleRawTransaction(topic string, data []byte) error {
	// 1. Parse raw transaction
	tx, err := DeserializeTransaction(data)
	if err != nil {
		return fmt.Errorf("Failed to parse transaction: %w", err)
	}

	// 2. Get transaction ID
	//txHash := tx.TxHash().String()
	//log.Printf("Processing raw transaction: %s, inputs: %d, outputs: %d",	txHash, len(tx.TxIn), len(tx.TxOut))

	// 3. Process transaction outputs, create new UTXOs
	err = m.processOutputs(tx)
	if err != nil {
		return fmt.Errorf("Failed to process transaction outputs: %w", err)
	}

	// 4. Process transaction inputs, mark spent UTXOs
	err = m.processInputs(tx)
	if err != nil {
		return fmt.Errorf("Failed to process transaction inputs: %w", err)
	}

	return nil
}

// processOutputs processes transaction outputs, creates new UTXOs
func (m *MempoolManager) processOutputs(tx *wire.MsgTx) error {
	txHash := tx.TxHash().String()
	//fmt.Println(config.GlobalConfig.RPC.Chain, ">>>>>>>>>")
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txHash, _ = blockchain.GetNewHash(tx)
	}

	// Process each output
	for i, out := range tx.TxOut {
		address := blockchain.GetAddressFromScript("", out.PkScript, m.chainCfg, config.GlobalConfig.RPC.Chain)
		// Parse output script, extract addresses
		// addresses, err := ExtractAddressesFromOutput(out, m.chainCfg)
		// if err != nil {
		// 	log.Printf("Failed to parse output script %s:%d: %v", txHash, i, err)
		// 	continue
		// }

		// // If no addresses extracted, skip
		// if len(addresses) == 0 {
		// 	continue
		// }

		// Create UTXO index for each address
		outputIndex := strconv.Itoa(i)
		utxoID := txHash + ":" + outputIndex
		//fmt.Println("Start processing deposit--------->", utxoID, address, out.Value)
		value := strconv.FormatInt(out.Value, 10)
		// Build storage key: addr1_tx1:0 format, value: UTXO amount
		// Store UTXO -> address mapping to mempool income database
		//err := m.mempoolIncomeDB.AddRecord(utxoID, address, []byte(value))
		key := common.ConcatBytesOptimized([]string{address, utxoID}, "_")
		err := m.mempoolIncomeDB.AddMempolRecord(key, []byte(value))
		if err != nil {
			log.Printf("Failed to store mempool UTXO index %s -> %s: %v", utxoID, address, err)
			continue
		}
		// list, err := m.mempoolIncomeDB.GetUtxoByKey(address)
		// if err != nil {
		// 	log.Printf("Failed to get mempool income amount %s: %v", utxoID, err)
		// 	continue
		// }
		// for _, utxo := range list {
		// 	log.Println("Get deposit", utxo.TxID, address, utxo.Amount)
		// }
	}

	return nil
}

// processInputs processes transaction inputs, marks spent UTXOs
func (m *MempoolManager) processInputs(tx *wire.MsgTx) error {
	// Skip mining transactions
	if IsCoinbaseTx(tx) {
		return nil
	}

	// Process each input
	for _, in := range tx.TxIn {
		// Build spent UTXO ID
		prevTxHash := in.PreviousOutPoint.Hash.String()
		prevOutputIndex := strconv.Itoa(int(in.PreviousOutPoint.Index))
		spentUtxoID := prevTxHash + ":" + prevOutputIndex
		// var utxoAddress string
		// var utxoAmount string
		// //fmt.Println(spentUtxoID)
		// // Get data directly from main UTXO storage
		// utxoData, err := m.utxoStore.Get([]byte(prevTxHash))
		// if err == nil {
		// 	// Handle leading comma
		// 	utxoStr := string(utxoData)
		// 	//fmt.Println("Main DB", utxoStr)
		// 	if len(utxoStr) > 0 && utxoStr[0] == ',' {
		// 		utxoStr = utxoStr[1:]
		// 	}
		// 	parts := strings.Split(utxoStr, ",")
		// 	if len(parts) > int(in.PreviousOutPoint.Index) {
		// 		// Parse address and amount
		// 		outputInfo := strings.Split(parts[in.PreviousOutPoint.Index], "@")
		// 		if len(outputInfo) >= 2 {
		// 			// Get address
		// 			utxoAddress = outputInfo[0]
		// 			// Get amount
		// 			// if amount, err := strconv.ParseUint(outputInfo[1], 10, 64); err == nil {
		// 			// 	utxoAmount = amount
		// 			// }
		// 			utxoAmount = outputInfo[1]
		// 		}
		// 	}
		// 	//fmt.Println("Main DB", utxoAddress, utxoAmount)
		// } else if err == storage.ErrNotFound {
		// 	// Not found in main UTXO storage, try to find in mempool income database
		// 	utxoAddress, utxoAmount, err = m.mempoolIncomeDB.GetByUTXO(spentUtxoID)
		// 	if err != nil {
		// 		// If not found in mempool income database either, get from node
		// 		utxoAddress, utxoAmount, _ = getInfoFromNode(spentUtxoID)
		// 	}
		// } else {
		// 	// If not found in mempool income database either, get from node
		// 	utxoAddress, utxoAmount, _ = getInfoFromNode(spentUtxoID)
		// }
		// Record to mempool spend database
		// Record to mempool spend database, including amount information
		// if utxoAddress == "" || utxoAmount == "" {
		// 	log.Printf("Invalid UTXO information, skipping: %s", spentUtxoID)
		// 	continue
		// }
		//err = m.mempoolSpendDB.AddRecord(spentUtxoID, utxoAddress, []byte(utxoAmount))
		//fmt.Println("Store spend:", spentUtxoID, utxoAddress, utxoAmount, err)
		m.mempoolSpendDB.AddMempolRecord(spentUtxoID, []byte(""))
	}
	return nil
}
func getInfoFromNode(spentUtxoID string) (string, string, error) {
	hashArr := strings.Split(spentUtxoID, ":")
	if len(hashArr) != 2 {
		return "", "", fmt.Errorf("Invalid UTXO ID format: %s", spentUtxoID)
	}
	txHashStr := hashArr[0]
	outputIndex, err := strconv.Atoi(hashArr[1])
	if err != nil {
		return "", "", fmt.Errorf("Invalid output index: %s", hashArr[1])
	}
	// Use global RPC client to get transaction details
	txHash, err := chainhash.NewHashFromStr(txHashStr)
	if err != nil {
		return "", "", fmt.Errorf("Invalid transaction hash: %s", txHashStr)
	}
	tx, err := blockchain.RpcClient.GetRawTransaction(txHash)
	if err != nil {
		return "", "", fmt.Errorf("Failed to get transaction details: %w", err)
	}
	// Ensure transaction has enough outputs
	if outputIndex < 0 || outputIndex >= len(tx.MsgTx().TxOut) {
		return "", "", fmt.Errorf("Output index out of range: %d", outputIndex)
	}
	// Get output script and amount
	out := tx.MsgTx().TxOut[outputIndex]
	address := blockchain.GetAddressFromScript("", out.PkScript, config.GlobalNetwork, config.GlobalConfig.RPC.Chain)
	// Parse amount
	amount := strconv.FormatInt(out.Value, 10)
	return address, amount, nil
}

// DeserializeTransaction deserializes byte array to transaction
func DeserializeTransaction(data []byte) (*wire.MsgTx, error) {
	tx := wire.NewMsgTx(wire.TxVersion)
	err := tx.Deserialize(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// ExtractAddressesFromOutput extracts addresses from transaction output
func ExtractAddressesFromOutput(out *wire.TxOut, chainCfg *chaincfg.Params) ([]string, error) {
	// Parse output script
	_, addresses, _, err := txscript.ExtractPkScriptAddrs(out.PkScript, chainCfg)
	if err != nil {
		return nil, err
	}

	// Convert to string array
	result := make([]string, len(addresses))
	for i, addr := range addresses {
		result[i] = addr.String()
	}

	return result, nil
}

// IsCoinbaseTx checks if transaction is a mining transaction
func IsCoinbaseTx(tx *wire.MsgTx) bool {
	// Mining transaction has only one input
	if len(tx.TxIn) != 1 {
		return false
	}

	// Mining transaction's previous output hash is 0
	zeroHash := &wire.OutPoint{
		Hash:  chainhash.Hash{},
		Index: 0xffffffff,
	}

	return tx.TxIn[0].PreviousOutPoint.Hash == zeroHash.Hash &&
		tx.TxIn[0].PreviousOutPoint.Index == zeroHash.Index
}

// ProcessNewBlockTxs processes transactions in new blocks, cleans mempool records
func (m *MempoolManager) ProcessNewBlockTxs(incomeUtxoList []common.Utxo, spendTxList []string) error {
	if len(incomeUtxoList) == 0 {
		return nil
	}

	//log.Printf("Processing %d transactions in new block, cleaning mempool records", len(incomeUtxoList))

	// Delete income
	for _, utxo := range incomeUtxoList {
		// 1. Delete related records from mempool income database
		//fmt.Println("delete", utxo.TxID, utxo.Address)

		err := m.mempoolIncomeDB.DeleteRecord(utxo.TxID, utxo.Address)
		if err != nil {
			log.Printf("Failed to delete mempool income record %s: %v", utxo.TxID, err)
		}
		//log.Printf("Cleaned mempool record for transaction %s", txid)
	}
	// Delete spend
	for _, txid := range spendTxList {
		err := m.mempoolSpendDB.DeleteSpendRecord(txid)
		if err != nil {
			log.Printf("Failed to delete mempool spend record %s: %v", txid, err)
		}
	}
	return nil
}

// CleanByHeight cleans mempool records by block height
func (m *MempoolManager) CleanByHeight(height int, bcClient interface{}) error {
	log.Printf("Starting to clean mempool, processing to block height: %d", height)

	// Try to assert bcClient as blockchain.Client type
	client, ok := bcClient.(*blockchain.Client)
	if !ok {
		return fmt.Errorf("Unsupported blockchain client type")
	}

	// Get block hash at this height
	blockHash, err := client.GetBlockHash(int64(height))
	if err != nil {
		return fmt.Errorf("Failed to get block hash: %w", err)
	}

	// Get block details
	block, err := client.GetBlock(blockHash)
	if err != nil {
		return fmt.Errorf("Failed to get block information: %w", err)
	}

	// Extract incomeUtxo list
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
	// Clean mempool records
	return m.ProcessNewBlockTxs(incomeUtxoList, spendTxList)
}

// InitializeMempool fetches and processes all current mempool transactions from the node at startup
// This method runs asynchronously to avoid blocking the main program
func (m *MempoolManager) InitializeMempool(bcClient interface{}) {
	// Use a separate goroutine to avoid blocking the main program
	go func() {
		log.Printf("Starting mempool data initialization...")

		// Assert as blockchain.Client
		client, ok := bcClient.(*blockchain.Client)
		if !ok {
			log.Printf("Failed to initialize mempool: unsupported blockchain client type")
			return
		}

		// Get all transaction IDs in the mempool
		txids, err := client.GetRawMempool()
		if err != nil {
			log.Printf("Failed to get mempool transaction list: %v", err)
			return
		}

		log.Printf("Fetched %d mempool transactions from node, start processing...", len(txids))

		// Process transactions in batches, 500 per batch to avoid excessive memory usage
		batchSize := 500
		totalBatches := (len(txids) + batchSize - 1) / batchSize

		for batchIdx := 0; batchIdx < totalBatches; batchIdx++ {
			start := batchIdx * batchSize
			end := start + batchSize
			if end > len(txids) {
				end = len(txids)
			}

			// Process current batch
			currentBatch := txids[start:end]
			log.Printf("Processing mempool transaction batch %d/%d (%d transactions)", batchIdx+1, totalBatches, len(currentBatch))

			for _, txid := range currentBatch {
				// Get transaction details
				tx, err := client.GetRawTransaction(txid)
				if err != nil {
					log.Printf("Failed to get transaction details %s: %v", txid, err)
					continue
				}

				// Use existing transaction processing methods
				msgTx := tx.MsgTx()

				// Process outputs first (create new UTXOs)
				if err := m.processOutputs(msgTx); err != nil {
					log.Printf("Failed to process transaction outputs %s: %v", txid, err)
					continue
				}

				// Then process inputs (mark spent UTXOs)
				if err := m.processInputs(msgTx); err != nil {
					log.Printf("Failed to process transaction inputs %s: %v", txid, err)
					continue
				}
			}

			// After batch is processed, pause briefly to allow other programs to execute
			// Avoid sustained high load
			time.Sleep(10 * time.Millisecond)
		}

		log.Printf("Mempool data initialization complete, processed %d transactions in total", len(txids))
	}()
}

// GetUTXOsByAddress queries unspent UTXOs for an address in the mempool
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

// 	// 1. Use suffix query to directly get all keys related to the address
// 	keys, err := m.mempoolIncomeDB.GetKeysWithSuffix(address)
// 	if err != nil {
// 		if errors.Is(err, storage.ErrNotFound) {
// 			return result, nil // Address has no mempool UTXOs
// 		}
// 		return nil, fmt.Errorf("Failed to query address UTXOs: %w", err)
// 	}

// 	// 2. Get spent UTXOs
// 	spentUtxos := make(map[string]struct{})

// 	// Query address index in spend database
// 	spendKeys, err := m.mempoolSpendDB.GetKeysWithSuffix(address)
// 	if err == nil && len(spendKeys) > 0 {
// 		for _, key := range spendKeys {
// 			parts := strings.Split(key, "_")
// 			if len(parts) == 2 {
// 				spentUtxos[parts[0]] = struct{}{} // Record spent utxoID
// 			}
// 		}
// 	}

// 	// 3. Process each unspent UTXO
// 	for _, key := range keys {
// 		parts := strings.Split(key, "_")
// 		if len(parts) != 2 {
// 			continue // Invalid format
// 		}

// 		utxoID := parts[0] // txid:index

// 		// Check if already spent
// 		if _, spent := spentUtxos[utxoID]; spent {
// 			continue // Skip spent UTXO
// 		}

// 		// Get UTXO value
// 		value, err := m.mempoolIncomeDB.Get([]byte(key))
// 		if err != nil {
// 			continue
// 		}

// 		// Parse value (amount@script_type)
// 		valueStr := string(value)
// 		valueInfo := strings.Split(valueStr, "@")
// 		if len(valueInfo) < 1 {
// 			continue
// 		}

// 		amount, err := strconv.ParseUint(valueInfo[0], 10, 64)
// 		if err != nil {
// 			continue
// 		}

// 		// Skip small UTXOs (dust)
// 		if amount <= 1000 {
// 			continue
// 		}

// 		// Parse UTXO ID
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

// CleanAllMempool cleans all mempool data for complete rebuild
func (m *MempoolManager) CleanAllMempool() error {
	log.Println("Resetting mempool data by deleting physical files...")

	// Save ZMQ address for later reconstruction
	zmqAddress := ""
	if m.zmqClient != nil {
		zmqAddress = m.zmqClient.address
	}

	// Use basePath and fixed table names to get database file paths
	incomeDbPath := m.basePath + "/mempool_income"
	spendDbPath := m.basePath + "/mempool_spend"

	// No longer try to detect database status, directly use defer and recover to handle possible panics
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Exception caught during cleanup: %v, continuing with file deletion", r)
		}
	}()

	// Safely close database connections
	log.Println("Closing existing mempool database connections...")
	// Use recover to avoid panic from repeated closing
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing income database: %v", r)
			}
		}()
		if m.mempoolIncomeDB != nil {
			m.mempoolIncomeDB.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing spend database: %v", r)
			}
		}()
		if m.mempoolSpendDB != nil {
			m.mempoolSpendDB.Close()
		}
	}()

	// Delete physical files
	log.Printf("Deleting mempool income database: %s", incomeDbPath)
	if err := os.RemoveAll(incomeDbPath); err != nil {
		log.Printf("Failed to delete mempool income database: %v", err)
		// Recreate connection after failure
		newIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
		if err != nil {
			log.Printf("Failed to recreate mempool income database: %v", err)
		} else {
			m.mempoolIncomeDB = newIncomeDB
		}
		return err
	}

	log.Printf("Deleting mempool spend database: %s", spendDbPath)
	if err := os.RemoveAll(spendDbPath); err != nil {
		log.Printf("Failed to delete mempool spend database: %v", err)
		// Recreate connection after failure
		newIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
		if err != nil {
			log.Printf("Failed to recreate mempool income database: %v", err)
		} else {
			m.mempoolIncomeDB = newIncomeDB
		}
		newSpendDB, err := storage.NewSimpleDB(spendDbPath)
		if err != nil {
			log.Printf("Failed to recreate mempool spend database: %v", err)
		} else {
			m.mempoolSpendDB = newSpendDB
		}
		return err
	}

	// Recreate databases
	log.Println("Recreating mempool databases...")
	newIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
	if err != nil {
		log.Printf("Failed to recreate mempool income database: %v", err)
		return err
	}

	newSpendDB, err := storage.NewSimpleDB(spendDbPath)
	if err != nil {
		newIncomeDB.Close()
		log.Printf("Failed to recreate mempool spend database: %v", err)
		return err
	}

	// Update database references
	m.mempoolIncomeDB = newIncomeDB
	m.mempoolSpendDB = newSpendDB

	// Recreate ZMQ client
	if zmqAddress != "" {
		log.Println("Recreating ZMQ client...")
		m.zmqClient = NewZMQClient(zmqAddress, nil)

		// Re-add listening topics
		log.Println("Re-adding ZMQ listening topics...")
		m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)
	}

	log.Println("Mempool data completely reset")
	return nil
}

// GetBasePath returns the base path for mempool data
func (m *MempoolManager) GetBasePath() string {
	return m.basePath
}

// GetZmqAddress returns the ZMQ server address
func (m *MempoolManager) GetZmqAddress() string {
	if m.zmqClient != nil {
		return m.zmqClient.address
	}
	return ""
}

// RebuildMempool rebuilds the mempool data (deletes and reinitializes the database and ZMQ listening)
func (m *MempoolManager) RebuildMempool() error {
	log.Println("Resetting mempool data by deleting physical files...")

	zmqAddress := ""
	if m.zmqClient != nil {
		zmqAddress = m.zmqClient.address
	}

	incomeDbPath := m.basePath + "/mempool_income"
	spendDbPath := m.basePath + "/mempool_spend"

	defer func() {
		if r := recover(); r != nil {
			log.Printf("Exception caught during rebuild: %v, continuing with file deletion", r)
		}
	}()

	log.Println("Closing existing mempool database connections...")
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing income database: %v", r)
			}
		}()
		if m.mempoolIncomeDB != nil {
			m.mempoolIncomeDB.Close()
		}
	}()
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing spend database: %v", r)
			}
		}()
		if m.mempoolSpendDB != nil {
			m.mempoolSpendDB.Close()
		}
	}()

	log.Printf("Deleting mempool income database: %s", incomeDbPath)
	if err := os.RemoveAll(incomeDbPath); err != nil {
		log.Printf("Failed to delete mempool income database: %v", err)
		return err
	}

	log.Printf("Deleting mempool spend database: %s", spendDbPath)
	if err := os.RemoveAll(spendDbPath); err != nil {
		log.Printf("Failed to delete mempool spend database: %v", err)
		return err
	}

	log.Println("Recreating mempool databases...")
	newIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
	if err != nil {
		log.Printf("Failed to recreate mempool income database: %v", err)
		return err
	}
	newSpendDB, err := storage.NewSimpleDB(spendDbPath)
	if err != nil {
		newIncomeDB.Close()
		log.Printf("Failed to recreate mempool spend database: %v", err)
		return err
	}
	m.mempoolIncomeDB = newIncomeDB
	m.mempoolSpendDB = newSpendDB

	if zmqAddress != "" {
		log.Println("Recreating ZMQ client...")
		m.zmqClient = NewZMQClient(zmqAddress, nil)
		log.Println("Re-adding ZMQ listening topics...")
		m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)
	}

	log.Println("Mempool data completely rebuilt")
	return nil
}
