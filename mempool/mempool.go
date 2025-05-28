package mempool

import (
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/indexer"
)

// 确保MempoolManager实现了indexer.MempoolManager接口
var _ indexer.MempoolManager = (*MempoolManager)(nil)

// UTXOResult 定义了UTXO查询结果
type UTXOResult struct {
	Income []struct {
		TxID   string
		Index  string
		Amount uint64
	}
	Spent []struct {
		TxID   string
		Index  string
		Amount uint64
	}
	// 添加金额统计
	TotalIncome uint64 // 总收入（聪）
	TotalSpent  uint64 // 总花费（聪）
}

// getRawUTXOsByAddress 内部方法，获取原始UTXO数据，包括收入和花费的UTXO
func (m *MempoolManager) getRawUTXOsByAddress(address string) (incomeUtxoList []common.Utxo, spendUtxoList []common.Utxo, err error) {

	// 用于去重的map
	incomeMap := make(map[string]struct{})
	spentMap := make(map[string]struct{})

	// 1. 获取收入UTXO
	incomeList, err := m.mempoolIncomeDB.GetUtxoByKey(address)
	if err == nil {
		for _, utxo := range incomeList {
			if _, ok := incomeMap[utxo.TxID]; !ok {
				incomeUtxoList = append(incomeUtxoList, utxo)
				incomeMap[utxo.TxID] = struct{}{}
			}
		}
	}
	// 2. 获取已花费的UTXO
	spendList, err := m.mempoolSpendDB.GetUtxoByKey(address)
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

// GetUTXOsByAddress 获取指定地址的内存池UTXO
func (m *MempoolManager) GetUTXOsByAddress(address string) (incomeUtxoList []common.Utxo, spendUtxoList []common.Utxo, err error) {
	// 获取原始UTXO数据
	return m.getRawUTXOsByAddress(address)
}
