package mempool

import (
	"encoding/json"
	"strings"

	indexer "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"

	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/storage"
)

// Ensure FtMempoolManager implements ft.MempoolManager interface
var _ indexer.FtMempoolManager = (*FtMempoolManager)(nil)

// GetFtUTXOsByAddress gets mempool FT UTXO for specified address
func (m *FtMempoolManager) GetFtUTXOsByAddress(address string, codeHash string, genesis string) (incomeUtxoList []common.FtUtxo, spendUtxoList []common.FtUtxo, err error) {
	// Get raw UTXO data
	return m.getRawFtUTXOsByAddress(address, codeHash, genesis)
}

func (m *FtMempoolManager) GetFtInfoByCodeHashGenesis(codeHash string, genesis string) (*common.FtInfoModel, error) {
	return m.getFtInfoByCodeHashGenesis(codeHash, genesis)
}

// getRawFtUTXOsByAddress internal method, gets raw FT UTXO data including income and spent UTXOs
func (m *FtMempoolManager) getRawFtUTXOsByAddress(address string, codeHash string, genesis string) (incomeUtxoList []common.FtUtxo, spendUtxoList []common.FtUtxo, err error) {
	// Map for deduplication
	incomeMap := make(map[string]struct{})
	spentMap := make(map[string]struct{})

	// 1. Get income UTXOs
	// incomeList, err := m.mempoolAddressFtIncomeDB.GetFtUtxoByKey(address)
	incomeList, err := m.mempoolAddressFtIncomeValidStore.GetFtUtxoByKey(address)
	if err == nil {
		for _, utxo := range incomeList {
			if _, ok := incomeMap[utxo.UtxoId]; !ok {
				incomeUtxoList = append(incomeUtxoList, utxo)
				incomeMap[utxo.UtxoId] = struct{}{}
			}
		}
	}
	// 2. Get spent UTXOs
	spendList, err := m.mempoolAddressFtSpendDB.GetFtUtxoByKey(address)
	if err == nil {
		for _, utxo := range spendList {
			if _, ok := spentMap[utxo.UtxoId]; !ok {
				spendUtxoList = append(spendUtxoList, utxo)
				spentMap[utxo.UtxoId] = struct{}{}
			}
		}
	}
	return
}

func (m *FtMempoolManager) getFtInfoByCodeHashGenesis(codeHash string, genesis string) (*common.FtInfoModel, error) {
	codeHashGenesis := common.ConcatBytesOptimized([]string{codeHash, genesis}, "@")
	valueInfo, err := m.mempoolContractFtInfoStore.GetSimpleRecord(codeHashGenesis)
	if err != nil {
		return nil, err
	}
	ftInfo := &common.FtInfoModel{}
	err = json.Unmarshal([]byte(valueInfo), ftInfo)
	if err != nil {
		return nil, err
	}
	return ftInfo, nil
}

// GetVerifyTx gets verification transaction information
func (m *FtMempoolManager) GetVerifyTx(txId string, page, pageSize int) ([]string, int, error) {
	if txId != "" {
		// If txId is provided, only return information for that transaction
		value, err := m.mempoolVerifyTxStore.GetSimpleRecord(txId)
		if err != nil {
			if strings.Contains(err.Error(), storage.ErrNotFound.Error()) {
				return []string{}, 0, nil
			}
			return nil, 0, err
		}
		return []string{string(value)}, 1, nil
	}

	// Get all verification transactions
	values, err := m.mempoolVerifyTxStore.GetAll()
	if err != nil {
		return nil, 0, err
	}

	total := len(values)
	if total == 0 {
		return []string{}, 0, nil
	}

	// Calculate pagination
	start := (page - 1) * pageSize
	if start >= total {
		return []string{}, total, nil
	}

	end := start + pageSize
	if end > total {
		end = total
	}

	return values[start:end], total, nil
}

// GetUncheckFtUtxo gets unchecked FT UTXO list
func (m *FtMempoolManager) GetUncheckFtUtxo() ([]common.FtUtxo, error) {
	return m.mempoolUncheckFtOutpointStore.GetFtUtxo()
}

// GetMempoolAddressFtSpendMap gets FT spend data for addresses in mempool
func (m *FtMempoolManager) GetMempoolAddressFtSpendMap(address string) (map[string]string, error) {
	// If address is empty, get all data
	if address == "" {
		// Get FT spend data for all addresses
		allData, err := m.mempoolAddressFtSpendDB.GetAllKeyValues()
		if err != nil {
			return nil, err
		}

		// Convert string array to map
		result := make(map[string]string)
		for key, data := range allData {
			result[key] = data
		}
		return result, nil
	}

	// Get FT spend data for specified address
	spendList, err := m.mempoolAddressFtSpendDB.GetFtUtxoByKey(address)
	if err != nil {
		return nil, err
	}

	// Convert data to map format
	result := make(map[string]string)
	for _, utxo := range spendList {
		key := utxo.TxID + ":" + utxo.Index
		value := common.ConcatBytesOptimized([]string{
			utxo.CodeHash,
			utxo.Genesis,
			utxo.SensibleId,
			utxo.Amount,
			utxo.Index,
			utxo.Value,
		}, "@")
		result[key] = value
	}

	return result, nil
}

// GetMempoolUniqueFtSpendMap gets spend data for unique FT in mempool
func (m *FtMempoolManager) GetMempoolUniqueFtSpendMap(codeHashGenesis string) (map[string]string, error) {
	// If codeHashGenesis is empty, get all data
	if codeHashGenesis == "" {
		// Get spend data for all unique FTs
		allData, err := m.mempoolUniqueFtSpendStore.GetAllKeyValues()
		if err != nil {
			return nil, err
		}

		// Convert string array to map
		result := make(map[string]string)
		for _, data := range allData {
			result[data] = data
		}
		return result, nil
	}

	// Get spend data for specified unique FT
	spendList, err := m.mempoolUniqueFtSpendStore.GetFtUtxoByKey(codeHashGenesis)
	if err != nil {
		return nil, err
	}

	// Convert data to map format
	result := make(map[string]string)
	for _, utxo := range spendList {
		key := utxo.TxID + ":" + utxo.Index
		value := common.ConcatBytesOptimized([]string{
			utxo.CodeHash,
			utxo.Genesis,
			utxo.SensibleId,
			utxo.CustomData,
			utxo.Index,
			utxo.Value,
		}, "@")
		result[key] = value
	}

	return result, nil
}
