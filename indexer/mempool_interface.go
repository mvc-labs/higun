package indexer

import "github.com/metaid/utxo_indexer/common"

// UTXOInfo defines basic information of UTXO
type UTXOInfo struct {
	TxID    string `json:"tx_id"`
	Index   string `json:"index"`
	Amount  uint64 `json:"amount"`
	IsSpent bool   `json:"is_spent"` // Mark whether it has been spent
}

// MempoolManager defines the interface that mempool manager needs to implement
type MempoolManager interface {
	// GetUTXOsByAddress gets mempool UTXOs for the specified address
	GetUTXOsByAddress(address string) (incomeUtxoList []common.Utxo, err error)
	GetSpendUTXOs(txPoints []string) (spendMap map[string]struct{}, err error) // Get mempool spend UTXOs for specified transaction points
	BatchDeleteIncom(list []string) (err error)                                // Batch delete mempool income records
	BatchDeleteSpend(list []string) (err error)                                // Batch delete mempool spend records
}
