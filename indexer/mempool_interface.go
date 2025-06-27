package indexer

import "github.com/metaid/utxo_indexer/common"

// UTXOInfo 定义了UTXO的基本信息
type UTXOInfo struct {
	TxID    string `json:"tx_id"`
	Index   string `json:"index"`
	Amount  uint64 `json:"amount"`
	IsSpent bool   `json:"is_spent"` // 标记是否已花费
}

// MempoolManager 定义了内存池管理器需要实现的接口
type MempoolManager interface {
	// GetUTXOsByAddress 获取指定地址的内存池UTXO
	GetUTXOsByAddress(address string) (incomeUtxoList []common.Utxo, err error)
	GetSpendUTXOs(txPoints []string) (spendMap map[string]struct{}, err error) // 获取指定交易点的内存池支出UTXO
	BatchDeleteIncom(list []string) (err error)                                // 批量删除内存池收入记录
	BatchDeleteSpend(list []string) (err error)                                // 批量删除内存池支出记录
}
