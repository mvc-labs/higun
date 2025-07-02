package indexer

import "github.com/metaid/utxo_indexer/common"

// FtMempoolManager defines the interface that FT mempool manager needs to implement
type FtMempoolManager interface {
	// GetFtUTXOsByAddress gets FT mempool UTXO for specified address
	GetFtUTXOsByAddress(address string, codeHash string, genesis string) (incomeUtxoList []common.FtUtxo, spendUtxoList []common.FtUtxo, err error)

	// GetFtInfoByCodeHashGenesis gets FT information through contract code hash and genesis transaction ID
	GetFtInfoByCodeHashGenesis(codeHash string, genesis string) (*common.FtInfoModel, error)

	// GetMempoolAddressFtSpendMap gets FT spend data for addresses in mempool
	GetMempoolAddressFtSpendMap(address string) (map[string]string, error)

	// GetMempoolUniqueFtSpendMap gets spend data for unique FT in mempool
	GetMempoolUniqueFtSpendMap(codeHashGenesis string) (map[string]string, error)

	// // StartMempoolZmq starts mempool ZMQ
	// StartMempoolZmq() error

	// // InitializeMempool initializes mempool
	// InitializeMempool(bcClient interface{})
}
