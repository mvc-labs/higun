package mempool

import (
	"encoding/json"

	indexer "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"

	"github.com/metaid/utxo_indexer/common"
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
	incomeList, err := m.mempoolAddressFtIncomeDB.GetFtUtxoByKey(address)
	if err == nil {
		for _, utxo := range incomeList {
			if _, ok := incomeMap[utxo.TxID]; !ok {
				incomeUtxoList = append(incomeUtxoList, utxo)
				incomeMap[utxo.TxID] = struct{}{}
			}
		}
	}
	// 2. 获取已花费的UTXO
	spendList, err := m.mempoolAddressFtSpendDB.GetFtUtxoByKey(address)
	if err == nil {
		for _, utxo := range spendList {
			if _, ok := spentMap[utxo.TxID]; !ok {
				spendUtxoList = append(spendUtxoList, utxo)
				spentMap[utxo.TxID] = struct{}{}
			}
		}
	}
	return
}

func (m *FtMempoolManager) getFtInfoByCodeHashGenesis(codeHash string, genesis string) (*common.FtInfoModel, error) {
	codeHashGenesis := common.ConcatBytesOptimized([]string{codeHash, genesis}, "@")
	valueInfo, err := m.mempoolContractFtInfoStore.Get(codeHashGenesis)
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
