package mempool

import (
	"encoding/json"
	"strings"

	indexer "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"

	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/storage"
)

// 确保FtMempoolManager实现了ft.MempoolManager接口
var _ indexer.FtMempoolManager = (*FtMempoolManager)(nil)

// GetFtUTXOsByAddress 获取指定地址的内存池FT UTXO
func (m *FtMempoolManager) GetFtUTXOsByAddress(address string, codeHash string, genesis string) (incomeUtxoList []common.FtUtxo, spendUtxoList []common.FtUtxo, err error) {
	// 获取原始UTXO数据
	return m.getRawFtUTXOsByAddress(address, codeHash, genesis)
}

func (m *FtMempoolManager) GetFtInfoByCodeHashGenesis(codeHash string, genesis string) (*common.FtInfoModel, error) {
	return m.getFtInfoByCodeHashGenesis(codeHash, genesis)
}

// getRawFtUTXOsByAddress 内部方法，获取原始FT UTXO数据，包括收入和花费的UTXO
func (m *FtMempoolManager) getRawFtUTXOsByAddress(address string, codeHash string, genesis string) (incomeUtxoList []common.FtUtxo, spendUtxoList []common.FtUtxo, err error) {
	// 用于去重的map
	incomeMap := make(map[string]struct{})
	spentMap := make(map[string]struct{})

	// 1. 获取收入UTXO
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
	// 2. 获取已花费的UTXO
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

// GetVerifyTx 获取验证交易信息
func (m *FtMempoolManager) GetVerifyTx(txId string, page, pageSize int) ([]string, int, error) {
	if txId != "" {
		// 如果提供了 txId，只返回该交易的信息
		value, err := m.mempoolVerifyTxStore.GetSimpleRecord(txId)
		if err != nil {
			if strings.Contains(err.Error(), storage.ErrNotFound.Error()) {
				return []string{}, 0, nil
			}
			return nil, 0, err
		}
		return []string{string(value)}, 1, nil
	}

	// 获取所有验证交易
	values, err := m.mempoolVerifyTxStore.GetAll()
	if err != nil {
		return nil, 0, err
	}

	total := len(values)
	if total == 0 {
		return []string{}, 0, nil
	}

	// 计算分页
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

// GetUncheckFtUtxo 获取未检查的FT UTXO列表
func (m *FtMempoolManager) GetUncheckFtUtxo() ([]common.FtUtxo, error) {
	return m.mempoolUncheckFtOutpointStore.GetFtUtxo()
}

// GetMempoolAddressFtSpendMap 获取内存池中地址的FT支出数据
func (m *FtMempoolManager) GetMempoolAddressFtSpendMap(address string) (map[string]string, error) {
	// 如果地址为空，获取所有数据
	if address == "" {
		// 获取所有地址的FT支出数据
		allData, err := m.mempoolAddressFtSpendDB.GetAllKeyValues()
		if err != nil {
			return nil, err
		}

		// 将字符串数组转换为map
		result := make(map[string]string)
		for key, data := range allData {
			result[key] = data
		}
		return result, nil
	}

	// 获取指定地址的FT支出数据
	spendList, err := m.mempoolAddressFtSpendDB.GetFtUtxoByKey(address)
	if err != nil {
		return nil, err
	}

	// 将数据转换为map格式
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

// GetMempoolUniqueFtSpendMap 获取内存池中唯一FT的支出数据
func (m *FtMempoolManager) GetMempoolUniqueFtSpendMap(codeHashGenesis string) (map[string]string, error) {
	// 如果codeHashGenesis为空，获取所有数据
	if codeHashGenesis == "" {
		// 获取所有唯一FT的支出数据
		allData, err := m.mempoolUniqueFtSpendStore.GetAllKeyValues()
		if err != nil {
			return nil, err
		}

		// 将字符串数组转换为map
		result := make(map[string]string)
		for _, data := range allData {
			result[data] = data
		}
		return result, nil
	}

	// 获取指定唯一FT的支出数据
	spendList, err := m.mempoolUniqueFtSpendStore.GetFtUtxoByKey(codeHashGenesis)
	if err != nil {
		return nil, err
	}

	// 将数据转换为map格式
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
