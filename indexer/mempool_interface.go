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
	GetUTXOsByAddress(address string) (incomeUtxoList []common.Utxo, spendUtxoList []common.Utxo, err error)
}
