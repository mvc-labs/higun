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

// FtMempoolManager manages FT mempool transactions
type FtMempoolManager struct {
	contractFtUtxoStore      *storage.PebbleStore // Stores contract data key: txID, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType,...
	mempoolAddressFtIncomeDB *storage.SimpleDB    // Mempool income database key: outpoint+address value: CodeHash@Genesis@sensibleId@Amount@Index@Value
	mempoolAddressFtSpendDB  *storage.SimpleDB    // Mempool spend database key: outpoint+address value: CodeHash@Genesis@sensibleId@Amount@Index@Value

	contractFtInfoStore                 *storage.PebbleStore // Stores contract info key: codeHash@genesis, value: sensibleId@name@symbol@decimal
	contractFtGenesisStore              *storage.PebbleStore // Stores contract genesis info key: outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
	contractFtGenesisOutputStore        *storage.PebbleStore // Stores used contract genesis output info key: usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
	contractFtGenesisUtxoStore          *storage.PebbleStore // Stores contract genesis UTXO info key: outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}
	mempoolContractFtInfoStore          *storage.SimpleDB    // Mempool contract info database
	mempoolContractFtGenesisStore       *storage.SimpleDB    // Mempool contract genesis info database key: outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
	mempoolContractFtGenesisOutputStore *storage.SimpleDB    // Mempool contract genesis output info database key: usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
	mempoolContractFtGenesisUtxoStore   *storage.SimpleDB    // Mempool contract genesis UTXO info database

	mempoolAddressFtIncomeValidStore *storage.SimpleDB // Mempool contract data database key: outpoint+address value: CodeHash@Genesis@sensibleId@Amount@Index@Value
	mempoolUncheckFtOutpointStore    *storage.SimpleDB // Mempool unchecked FT outpoint database key: outpoint value: ftAddress@codeHash@genesis@sensibleId@ftAmount@index@value
	mempoolUsedFtIncomeStore         *storage.SimpleDB // Mempool used FT income database key: usedTxId, value: FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height,...

	mempoolUniqueFtIncomeStore *storage.SimpleDB // Mempool unique FT income database key: outpoint+ftCodehashGenesis, value: codeHash@genesis@sensibleId@customData@Index@Value
	mempoolUniqueFtSpendStore  *storage.SimpleDB // Mempool unique FT spend database key: outpoint+ftCodehashGenesis, value: CodeHash@Genesis@sensibleId@customData@Index@Value

	mempoolVerifyTxStore *storage.SimpleDB // key: txId, value: ""
	chainCfg             *chaincfg.Params
	zmqClient            *ZMQClient
	basePath             string // Data directory base path
}

// NewFtMempoolManager creates a new FT mempool manager
func NewFtMempoolManager(basePath string,
	contractFtUtxoStore *storage.PebbleStore,
	contractFtInfoStore *storage.PebbleStore,
	contractFtGenesisStore *storage.PebbleStore,
	contractFtGenesisOutputStore *storage.PebbleStore,
	contractFtGenesisUtxoStore *storage.PebbleStore,
	chainCfg *chaincfg.Params, zmqAddress string) *FtMempoolManager {
	// Create mempool databases
	mempoolAddressFtIncomeDB, err := storage.NewSimpleDB(basePath + "/mempool_address_ft_income")
	if err != nil {
		log.Printf("Failed to create FT mempool income database: %v", err)
		return nil
	}

	mempoolAddressFtSpendDB, err := storage.NewSimpleDB(basePath + "/mempool_address_ft_spend")
	if err != nil {
		log.Printf("Failed to create FT mempool spend database: %v", err)
		mempoolAddressFtIncomeDB.Close()
		return nil
	}

	mempoolContractFtInfoStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_info")
	if err != nil {
		log.Printf("Failed to create FT mempool info database: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		return nil
	}

	mempoolContractFtGenesisStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_genesis")
	if err != nil {
		log.Printf("Failed to create FT mempool genesis database: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		return nil
	}

	mempoolContractFtGenesisOutputStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_genesis_output")
	if err != nil {
		log.Printf("Failed to create FT mempool genesis output database: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		return nil
	}

	mempoolContractFtGenesisUtxoStore, err := storage.NewSimpleDB(basePath + "/mempool_contract_ft_genesis_utxo")
	if err != nil {
		log.Printf("Failed to create FT mempool genesis UTXO database: %v", err)
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		return nil
	}

	mempoolAddressFtIncomeValidStore, err := storage.NewSimpleDB(basePath + "/mempool_address_ft_income_valid")
	if err != nil {
		log.Printf("Failed to create FT mempool income valid database: %v", err)
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
		log.Printf("Failed to create FT mempool unchecked FT outpoint database: %v", err)
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
		log.Printf("Failed to create FT mempool used FT income database: %v", err)
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
		log.Printf("Failed to create FT mempool unique FT income database: %v", err)
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
		log.Printf("Failed to create FT mempool unique FT spend database: %v", err)
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
		log.Printf("Failed to create FT mempool verify Tx database: %v", err)
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

	// Create ZMQ client
	m.zmqClient = NewZMQClient(zmqAddress, nil)

	// Add "rawtx" topic listener
	m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)

	return m
}

// Start starts the FT mempool manager
func (m *FtMempoolManager) Start() error {
	return m.zmqClient.Start()
}

// Stop stops the FT mempool manager
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

// HandleRawTransaction handles raw transaction data
func (m *FtMempoolManager) HandleRawTransaction(topic string, data []byte) error {
	// 1. Parse raw transaction
	tx, err := DeserializeTransaction(data)
	if err != nil {
		return fmt.Errorf("Failed to parse transaction: %w", err)
	}
	// fmt.Printf("ZMQ received transaction: %s\n", tx.TxHash().String())

	txHash := tx.TxHash().String()
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txHash, _ = blockchain.GetNewHash(tx)
	}

	// 2. Process transaction outputs, create new FT UTXO
	isFtTx, err := m.processFtOutputs(tx)
	if err != nil {
		return fmt.Errorf("Failed to process FT transaction outputs: %w", err)
	}
	if isFtTx {
		fmt.Printf("ZMQ received FT transaction: %s\n", txHash)
	}

	// 3. Process transaction inputs, mark spent FT UTXO
	err = m.processFtInputs(tx)
	if err != nil {
		return fmt.Errorf("Failed to process FT transaction inputs: %w", err)
	}

	if isFtTx {
		// 4. Process VerifyTx
		err = m.processVerifyTx(tx)
		if err != nil {
			return fmt.Errorf("Failed to process VerifyTx: %w", err)
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

	// First check if it exists in the main storage
	fmt.Printf("processVerifyTx txHash: %s\n", txHash)
	_, err := m.mempoolVerifyTxStore.GetSimpleRecord(txHash)
	if err != nil {
		if strings.Contains(err.Error(), storage.ErrNotFound.Error()) {
			// Not found in main storage, add to mempool storage
			err = m.mempoolVerifyTxStore.AddSimpleRecord(txHash, []byte(txHash))
			if err != nil {
				return fmt.Errorf("Failed to store VerifyTx: %w", err)
			} else {
				fmt.Printf("Successfully stored VerifyTx: %s\n", txHash)
			}
		} else {
			return fmt.Errorf("Failed to get VerifyTx: %w", err)
		}
	}

	return nil
}

// processFtOutputs processes FT transaction outputs and creates new UTXO
func (m *FtMempoolManager) processFtOutputs(tx *wire.MsgTx) (bool, error) {
	txHash := tx.TxHash().String()
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txHash, _ = blockchain.GetNewHash(tx)
	}
	isFtTx := false

	// Process each output
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

			// Create FT UTXO index for each address
			ftAddress := ftInfo.Address
			valueStr := strconv.FormatInt(out.Value, 10)
			// Store FT UTXO info, format: CodeHash@Genesis@sensible@Amount@Value
			ftAmount := strconv.FormatUint(ftInfo.Amount, 10)
			mempoolFtUtxo := common.ConcatBytesOptimized([]string{ftInfo.CodeHash, ftInfo.Genesis, ftInfo.SensibleId, ftAmount, outputIndex, valueStr}, "@")
			//codeHash@genesis@sensibleId@ftAmount@index@value
			err = m.mempoolAddressFtIncomeDB.AddRecord(utxoID, ftAddress, []byte(mempoolFtUtxo))
			if err != nil {
				log.Printf("[Mempool] Failed to store FT mempool UTXO index %s -> %s: %v", utxoID, ftAddress, err)
				continue
			}

			// Process uncheckFtOutpointStore
			uncheckFtOutpointKey := utxoID
			// ftAddress@codeHash@genesis@sensibleId@ftAmount@index@value
			newMempoolFtUtxo := common.ConcatBytesOptimized([]string{ftAddress, mempoolFtUtxo}, "@")
			err = m.mempoolUncheckFtOutpointStore.AddSimpleRecord(uncheckFtOutpointKey, []byte(newMempoolFtUtxo))
			if err != nil {
				log.Printf("[Mempool] Failed to store FT mempool unchecked UTXO index %s -> %s: %v", utxoID, ftAddress, err)
				continue
			}

			// 1. Process FT info storage
			ftInfoKey := common.ConcatBytesOptimized([]string{ftInfo.CodeHash, ftInfo.Genesis}, "@")
			// First check if it exists in the main storage
			_, err = m.contractFtInfoStore.Get([]byte(ftInfoKey))
			if err == storage.ErrNotFound {
				// Not found in main storage, add to mempool storage
				// key: codeHash@genesis, value: sensibleId@name@symbol@decimal
				ftInfoValue := common.ConcatBytesOptimized([]string{ftInfo.SensibleId, ftInfo.Name, ftInfo.Symbol, strconv.FormatUint(uint64(ftInfo.Decimal), 10)}, "@")
				err = m.mempoolContractFtInfoStore.AddSimpleRecord(ftInfoKey, []byte(ftInfoValue))
				if err != nil {
					log.Printf("[Mempool] Failed to store FT mempool info %s: %v", ftInfoKey, err)
				}
			}

			// 2. Process initial genesis info storage
			if ftInfo.SensibleId == "000000000000000000000000000000000000000000000000000000000000000000000000" {
				genesisKey := common.ConcatBytesOptimized([]string{txHash, outputIndex}, ":")
				// First check if it exists in the main storage
				_, err = m.contractFtGenesisStore.Get([]byte(genesisKey))
				if err == storage.ErrNotFound {
					// Not found in main storage, add to mempool storage
					// key: outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
					genesisValue := common.ConcatBytesOptimized([]string{ftInfo.SensibleId, ftInfo.Name, ftInfo.Symbol, strconv.FormatUint(uint64(ftInfo.Decimal), 10), ftInfo.CodeHash, ftInfo.Genesis}, "@")
					err = m.mempoolContractFtGenesisStore.AddSimpleRecord(genesisKey, []byte(genesisValue))
					if err != nil {
						log.Printf("[Mempool] Failed to store FT mempool genesis info %s: %v", genesisKey, err)
					}
				}
			}

			// 3. Process new genesis UTXO storage
			if ftInfo.Amount == 0 {
				genesisUtxoKey := common.ConcatBytesOptimized([]string{txHash, outputIndex}, ":")
				// First check if it exists in the main storage
				_, err = m.contractFtGenesisUtxoStore.Get([]byte(genesisUtxoKey))
				if err == storage.ErrNotFound {
					// Not found in main storage, add to mempool storage
					// key: outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}
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
						log.Printf("[Mempool] Failed to store FT mempool genesis UTXO %s: %v", genesisUtxoKey, err)
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
			// key: outpoint+ftCodehashGenesis, value: codeHash@genesis@sensibleId@customData@value
			err = m.mempoolUniqueFtIncomeStore.AddRecord(utxoID, ftCodehashGenesis, []byte(mempoolUniquetUtxo))
			if err != nil {
				log.Printf("[Mempool] Failed to store FT mempool unique UTXO index %s -> %s: %v", utxoID, "", err)
				continue
			}

		} else {
			continue
		}

	}

	return isFtTx, nil
}

// processFtInputs processes FT transaction inputs
func (m *FtMempoolManager) processFtInputs(tx *wire.MsgTx) error {
	// Skip coinbase transaction
	if IsCoinbaseTx(tx) {
		return nil
	}

	// Used to store genesis transaction ID and its output info
	var usedGenesisUtxoMap = make(map[string]string)
	var txPointUsedMap = make(map[string]string)
	var allTxPoints []string
	var usedFtIncomeMap = make(map[string][]string)

	txId := tx.TxHash().String()
	if config.GlobalConfig.RPC.Chain == "mvc" {
		txId, _ = blockchain.GetNewHash(tx)
	}

	// First collect all input points
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
		// Get data directly from main UTXO storage
		utxoData, err := m.contractFtUtxoStore.Get([]byte(prevTxHash))
		if err == nil {
			// Handle leading comma
			utxoStr := string(utxoData)
			//fmt.Println("Main storage", utxoStr)
			if len(utxoStr) > 0 && utxoStr[0] == ',' {
				utxoStr = utxoStr[1:]
			}
			parts := strings.Split(utxoStr, ",")
			if len(parts) > int(in.PreviousOutPoint.Index) {

				ftUtxoPart := ""
				for _, part := range parts {
					//FtAddress@CodeHash@Genesis@Amount@Index@Value
					outputInfo := strings.Split(part, "@")
					// Parse address and amount
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

				// Get address
				ftUtxoAddress = outputInfo[0]
				ftUtxoCodeHash = outputInfo[1]
				ftUtxoGenesis = outputInfo[2]
				ftUtxoSensibleId = outputInfo[3]
				ftUtxoAmount = outputInfo[4]
				ftUtxoIndex = outputInfo[5]
				ftUtxoValue = outputInfo[6]
				ftUtxoContractType = outputInfo[8]
			}
			//fmt.Println("Main storage", utxoAddress, utxoAmount)
		} else if err == storage.ErrNotFound {
			// Not found in main UTXO storage, try to find from mempool income database
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

		// Record to mempool spend database
		// Record to mempool spend database, including amount info
		if ftUtxoContractType == "ft" {
			recordKey := ftUtxoAddress
			//CodeHash@Genesis@sensibleId@Amount@Value
			mempoolFtUtxo := common.ConcatBytesOptimized([]string{ftUtxoCodeHash, ftUtxoGenesis, ftUtxoSensibleId, ftUtxoAmount, ftUtxoIndex, ftUtxoValue}, "@")
			err = m.mempoolAddressFtSpendDB.AddRecord(spentUtxoID, recordKey, []byte(mempoolFtUtxo))
			//fmt.Println("Store spend:", spentUtxoID, utxoAddress, utxoAmount, err)
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
	// Query if all input points exist in contractFtGenesisUtxoStore
	for _, txPoint := range allTxPoints {
		value, err := m.contractFtGenesisUtxoStore.Get([]byte(txPoint))
		if err == nil {
			// Found genesis UTXO, record transaction outpoint
			//key:outpoint, value:sensibleId@name@symbol@decimal@codeHash@genesis@amount@index@value{@IsSpent}
			usedGenesisUtxoMap[txPoint] = string(value)
		}

		// Check if it exists in mempool
		mempoolValue, err := m.mempoolContractFtGenesisUtxoStore.GetSimpleRecord(txPoint)
		if err == nil {
			// If it's a genesis UTXO, record transaction ID
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

	// Process genesis UTXO consumption
	if len(usedGenesisUtxoMap) > 0 {
		for txPoint, value := range usedGenesisUtxoMap {
			// Get original UTXO info and add @IsSpent flag
			spentValue := value + "@1" // Add @IsSpent flag
			if err := m.mempoolContractFtGenesisUtxoStore.AddSimpleRecord(txPoint, []byte(spentValue)); err != nil {
				log.Printf("[Mempool] Failed to update FT mempool genesis UTXO status %s: %v", txPoint, err)
			}

		}
	}

	if len(usedFtIncomeMap) > 0 {
		for usedTxId, utxoList := range usedFtIncomeMap {
			//value: ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType
			if err := m.mempoolUsedFtIncomeStore.AddSimpleRecord(usedTxId, []byte(strings.Join(utxoList, ","))); err != nil {
				log.Printf("[Mempool] Failed to store FT mempool used UTXO %s: %v", usedTxId, err)
			}
		}
	}

	// Process genesis output storage
	if len(allTxPoints) > 0 {
		for _, usedTxPoint := range allTxPoints {
			// Collect all output info for this transaction
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
					log.Printf("[Mempool] Failed to store FT mempool genesis output %s: %v", usedTxPoint, err)
				}
			}
		}
	}

	return nil
}

// ProcessNewBlockTxs processes FT transactions in new blocks and cleans up mempool records
func (m *FtMempoolManager) ProcessNewBlockTxs(incomeUtxoList []common.FtUtxo, spendOutpointList []string, txList []string) error {
	// Delete VerifyTx
	for _, tx := range txList {
		err := m.mempoolVerifyTxStore.DeleteSimpleRecord(tx)
		if err != nil {
			log.Printf("Failed to delete VerifyTx %s: %v", tx, err)
		}
	}
	if len(incomeUtxoList) == 0 {
		return nil
	}

	// Delete income
	for _, utxo := range incomeUtxoList {
		if utxo.ContractType == "ft" {
			err := m.mempoolAddressFtIncomeDB.DeleteRecord(utxo.UtxoId, utxo.Address)
			if err != nil {
				log.Printf("Failed to delete FT mempool income record %s: %v", utxo.TxID, err)
			}

			// Process uncheckFtOutpointStore
			err = m.mempoolAddressFtIncomeValidStore.DeleteRecord(utxo.UtxoId, utxo.Address)
			if err != nil {
				log.Printf("[Mempool] Failed to delete FT mempool valid UTXO index %s -> %s: %v", utxo.UtxoId, utxo.Address, err)
			}

			// Check and delete records in FT info storage
			parts := strings.Split(utxo.UtxoId, ":")
			if len(parts) == 2 {
				outpoint := common.ConcatBytesOptimized([]string{parts[0], parts[1]}, ":")

				// Check and delete records in initial genesis info storage
				genesisKey := outpoint
				_, err = m.mempoolContractFtGenesisStore.GetSimpleRecord(genesisKey)
				if err == nil {
					if err := m.mempoolContractFtGenesisStore.DeleteSimpleRecord(genesisKey); err != nil {
						log.Printf("[Mempool] Failed to delete FT mempool genesis info %s: %v", genesisKey, err)
					}
				}

				// 检查并删除新创世输出存储中的记录
				_, err = m.mempoolContractFtGenesisUtxoStore.GetSimpleRecord(genesisKey)
				if err == nil {
					if err := m.mempoolContractFtGenesisUtxoStore.DeleteSimpleRecord(genesisKey); err != nil {
						log.Printf("[Mempool] Failed to delete FT mempool genesis UTXO %s: %v", genesisKey, err)
					}
				}
			}
		} else if utxo.ContractType == "unique" {
			err := m.mempoolUniqueFtIncomeStore.DeleteRecord(utxo.UtxoId, utxo.CodeHash+"@"+utxo.Genesis)
			if err != nil {
				log.Printf("Failed to delete FT mempool unique income record %s: %v", utxo.UtxoId, err)
			}
		} else {
			continue
		}

	}

	for _, utxo := range incomeUtxoList {
		codeHashGenesis := common.ConcatBytesOptimized([]string{utxo.CodeHash, utxo.Genesis}, "@")
		err := m.mempoolContractFtInfoStore.DeleteSimpleRecord(codeHashGenesis)
		if err != nil {
			log.Printf("Failed to delete FT mempool genesis info %s: %v", utxo.CodeHash+"@"+utxo.Genesis, err)
		}
	}

	// 删除spend
	for _, outpoint := range spendOutpointList {
		err := m.mempoolAddressFtSpendDB.DeleteFtSpendRecord(outpoint)
		if err != nil {
			log.Printf("Failed to delete FT mempool ft spend record %s: %v", outpoint, err)
		}
		err = m.mempoolUniqueFtSpendStore.DeleteUniqueSpendRecord(outpoint)
		if err != nil {
			log.Printf("Failed to delete FT mempool unique spend record %s: %v", outpoint, err)
		}
	}
	return nil
}

// CleanByHeight cleans FT mempool records by block height
func (m *FtMempoolManager) CleanByHeight(height int, bcClient interface{}) error {
	log.Printf("Start cleaning FT mempool, processing to block height: %d", height)

	// Try to assert bcClient as blockchain.FtClient type
	client, ok := bcClient.(*blockchain.FtClient)
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

	txList := make([]string, 0)

	// Extract incomeUtxo list
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
			// Since ParseFtOutput is not defined, temporarily skip FT output judgment
			address := blockchain.GetAddressFromScript(out.ScriptPubKey.Hex, nil, m.chainCfg, config.GlobalConfig.RPC.Chain)
			amount := strconv.FormatInt(int64(math.Round(out.Value*1e8)), 10)

			// Parse FT related information
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

	// Clean mempool records
	return m.ProcessNewBlockTxs(incomeFtUtxoList, spendOutpointList, txList)
}

// InitializeMempool fetches and processes all current FT mempool transactions from the node at startup
func (m *FtMempoolManager) InitializeMempool(bcClient interface{}) {
	// Use a separate goroutine to avoid blocking the main program
	go func() {
		log.Printf("Starting FT mempool data initialization...")

		// Assert as blockchain.FtClient
		client, ok := bcClient.(*blockchain.FtClient)
		if !ok {
			log.Printf("Failed to initialize FT mempool: unsupported blockchain client type")
			return
		}

		// Get all transaction IDs in the mempool
		txids, err := client.GetRawMempool()
		if err != nil {
			log.Printf("Failed to get mempool transaction list: %v", err)
			return
		}

		log.Printf("Fetched %d mempool transactions from node, start processing...", len(txids))

		// Process transactions in batches, 100 per batch to avoid excessive memory usage
		batchSize := 100
		totalBatches := (len(txids) + batchSize - 1) / batchSize

		for batchIdx := 0; batchIdx < totalBatches; batchIdx++ {
			start := batchIdx * batchSize
			end := start + batchSize
			if end > len(txids) {
				end = len(txids)
			}

			// Process current batch
			currentBatch := txids[start:end]
			log.Printf("Processing FT mempool transaction batch %d/%d (%d transactions)", batchIdx+1, totalBatches, len(currentBatch))

			for _, txid := range currentBatch {
				// Get transaction details
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

				// Process outputs first (create new UTXOs)
				isFtTx, err := m.processFtOutputs(msgTx)
				if err != nil {
					log.Printf("Failed to process FT transaction outputs %s: %v", txid, err)
					continue
				}
				if isFtTx {
					fmt.Printf("Mempool received FT transaction: %s\n", txid)
				}

				// Then process inputs (mark spent UTXOs)
				if err := m.processFtInputs(msgTx); err != nil {
					log.Printf("Failed to process FT transaction inputs %s: %v", txid, err)
					continue
				}

				if isFtTx {
					// Process VerifyTx
					if err := m.processVerifyTx(msgTx); err != nil {
						log.Printf("Failed to process VerifyTx %s: %v", txid, err)
						continue
					}
				}
			}

			// After batch is processed, pause briefly to allow other programs to execute
			time.Sleep(10 * time.Millisecond)
		}

		log.Printf("FT mempool data initialization complete, processed %d transactions in total", len(txids))
	}()
}

// CleanAllMempool cleans all FT mempool data for complete rebuild
func (m *FtMempoolManager) CleanAllMempool() error {
	log.Println("Resetting FT mempool data by deleting physical files...")

	// Save ZMQ address for later reconstruction
	zmqAddress := ""
	if m.zmqClient != nil {
		zmqAddress = m.zmqClient.address
	}

	// Get database file paths using basePath and fixed table names
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

	// No longer try to detect database status, use defer and recover to handle possible panics
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Exception caught during cleanup: %v, continuing with file deletion", r)
		}
	}()

	// Safely close database connections
	log.Println("Closing existing FT mempool database connections...")
	// Use recover to avoid panic from repeated close operations
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing income database: %v", r)
			}
		}()
		if m.mempoolAddressFtIncomeDB != nil {
			m.mempoolAddressFtIncomeDB.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing spend database: %v", r)
			}
		}()
		if m.mempoolAddressFtSpendDB != nil {
			m.mempoolAddressFtSpendDB.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing info database: %v", r)
			}
		}()
		if m.mempoolContractFtInfoStore != nil {
			m.mempoolContractFtInfoStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing genesis database: %v", r)
			}
		}()
		if m.mempoolContractFtGenesisStore != nil {
			m.mempoolContractFtGenesisStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing genesis output database: %v", r)
			}
		}()
		if m.mempoolContractFtGenesisOutputStore != nil {
			m.mempoolContractFtGenesisOutputStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing genesis UTXO database: %v", r)
			}
		}()
		if m.mempoolContractFtGenesisUtxoStore != nil {
			m.mempoolContractFtGenesisUtxoStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing income valid database: %v", r)
			}
		}()
		if m.mempoolAddressFtIncomeValidStore != nil {
			m.mempoolAddressFtIncomeValidStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing unchecked FT output database: %v", r)
			}
		}()
		if m.mempoolUncheckFtOutpointStore != nil {
			m.mempoolUncheckFtOutpointStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing used income database: %v", r)
			}
		}()
		if m.mempoolUsedFtIncomeStore != nil {
			m.mempoolUsedFtIncomeStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing Unique income database: %v", r)
			}
		}()
		if m.mempoolUniqueFtIncomeStore != nil {
			m.mempoolUniqueFtIncomeStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing Unique spend database: %v", r)
			}
		}()
		if m.mempoolUniqueFtSpendStore != nil {
			m.mempoolUniqueFtSpendStore.Close()
		}
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Error occurred while closing VerifyTx database: %v", r)
			}
		}()
		if m.mempoolVerifyTxStore != nil {
			m.mempoolVerifyTxStore.Close()
		}
	}()

	// Delete physical files
	log.Printf("Deleting FT mempool income database: %s", incomeDbPath)
	if err := os.RemoveAll(incomeDbPath); err != nil {
		log.Printf("Failed to delete FT mempool income database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool spend database: %s", spendDbPath)
	if err := os.RemoveAll(spendDbPath); err != nil {
		log.Printf("Failed to delete FT mempool spend database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool info database: %s", infoDbPath)
	if err := os.RemoveAll(infoDbPath); err != nil {
		log.Printf("Failed to delete FT mempool info database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool genesis database: %s", genesisDbPath)
	if err := os.RemoveAll(genesisDbPath); err != nil {
		log.Printf("Failed to delete FT mempool genesis database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool genesis output database: %s", genesisOutputDbPath)
	if err := os.RemoveAll(genesisOutputDbPath); err != nil {
		log.Printf("Failed to delete FT mempool genesis output database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool genesis UTXO database: %s", genesisUtxoDbPath)
	if err := os.RemoveAll(genesisUtxoDbPath); err != nil {
		log.Printf("Failed to delete FT mempool genesis UTXO database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool income valid database: %s", incomeValidDbPath)
	if err := os.RemoveAll(incomeValidDbPath); err != nil {
		log.Printf("Failed to delete FT mempool income valid database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool unchecked FT output database: %s", uncheckFtOutpointDbPath)
	if err := os.RemoveAll(uncheckFtOutpointDbPath); err != nil {
		log.Printf("Failed to delete FT mempool unchecked FT output database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool used income database: %s", usedFtIncomeDbPath)
	if err := os.RemoveAll(usedFtIncomeDbPath); err != nil {
		log.Printf("Failed to delete FT mempool used income database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool Unique income database: %s", uniqueFtIncomeDbPath)
	if err := os.RemoveAll(uniqueFtIncomeDbPath); err != nil {
		log.Printf("Failed to delete FT mempool Unique income database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool Unique spend database: %s", uniqueFtSpendDbPath)
	if err := os.RemoveAll(uniqueFtSpendDbPath); err != nil {
		log.Printf("Failed to delete FT mempool Unique spend database: %v", err)
		return err
	}

	log.Printf("Deleting FT mempool Unique spend database: %s", mempoolVerifyTxDbPath)
	if err := os.RemoveAll(mempoolVerifyTxDbPath); err != nil {
		log.Printf("Failed to delete FT mempool Unique spend database: %v", err)
		return err
	}

	// Recreate databases
	log.Println("Recreating FT mempool databases...")
	mempoolAddressFtIncomeDB, err := storage.NewSimpleDB(incomeDbPath)
	if err != nil {
		log.Printf("Failed to recreate FT mempool income database: %v", err)
		return err
	}

	mempoolAddressFtSpendDB, err := storage.NewSimpleDB(spendDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		log.Printf("Failed to recreate FT mempool spend database: %v", err)
		return err
	}

	mempoolContractFtInfoStore, err := storage.NewSimpleDB(infoDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		log.Printf("Failed to recreate FT mempool info database: %v", err)
		return err
	}

	mempoolContractFtGenesisStore, err := storage.NewSimpleDB(genesisDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		log.Printf("Failed to recreate FT mempool genesis database: %v", err)
		return err
	}

	mempoolContractFtGenesisOutputStore, err := storage.NewSimpleDB(genesisOutputDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		log.Printf("Failed to recreate FT mempool genesis output database: %v", err)
		return err
	}

	mempoolContractFtGenesisUtxoStore, err := storage.NewSimpleDB(genesisUtxoDbPath)
	if err != nil {
		mempoolAddressFtIncomeDB.Close()
		mempoolAddressFtSpendDB.Close()
		mempoolContractFtInfoStore.Close()
		mempoolContractFtGenesisStore.Close()
		mempoolContractFtGenesisOutputStore.Close()
		log.Printf("Failed to recreate FT mempool genesis UTXO database: %v", err)
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
		log.Printf("Failed to recreate FT mempool income valid database: %v", err)
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
		log.Printf("Failed to recreate FT mempool unchecked FT output database: %v", err)
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
		log.Printf("Failed to recreate FT mempool used income database: %v", err)
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
		log.Printf("Failed to recreate FT mempool Unique income database: %v", err)
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
		log.Printf("Failed to recreate FT mempool Unique spend database: %v", err)
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
		log.Printf("Failed to recreate FT mempool Unique spend database: %v", err)
		return err
	}

	// Update database references
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

	// Recreate ZMQ client
	if zmqAddress != "" {
		log.Println("Recreating ZMQ client...")
		m.zmqClient = NewZMQClient(zmqAddress, nil)

		// Re-add topic listeners
		log.Println("Re-adding ZMQ topic listeners...")
		m.zmqClient.AddTopic("rawtx", m.HandleRawTransaction)
	}

	log.Println("FT mempool data completely reset successfully")
	return nil
}

// GetBasePath returns the base path for FT mempool data
func (m *FtMempoolManager) GetBasePath() string {
	return m.basePath
}

// GetZmqAddress returns the ZMQ server address
func (m *FtMempoolManager) GetZmqAddress() string {
	if m.zmqClient != nil {
		return m.zmqClient.address
	}
	return ""
}

// GetMempoolAddressFtIncomeMap gets FT income data for all addresses in mempool
// CodeHash@Genesis@sensibleId@Amount@Index@Value
func (m *FtMempoolManager) GetMempoolAddressFtIncomeMap() map[string]string {
	result := make(map[string]string)
	keyValues, err := m.mempoolAddressFtIncomeDB.GetAllKeyValues()
	if err != nil {
		log.Printf("Failed to get all key-value pairs: %v", err)
		return result
	}

	for key, value := range keyValues {

		result[key] = value
	}

	return result
}

// GetMempoolAddressFtIncomeValidMap gets valid FT income data for all addresses in mempool
// CodeHash@Genesis@sensibleId@Amount@Index@Value
func (m *FtMempoolManager) GetMempoolAddressFtIncomeValidMap() map[string]string {
	result := make(map[string]string)
	keyValues, err := m.mempoolAddressFtIncomeValidStore.GetAllKeyValues()
	if err != nil {
		log.Printf("Failed to get all key-value pairs: %v", err)
		return result
	}

	for key, value := range keyValues {
		result[key] = value
	}

	return result
}
