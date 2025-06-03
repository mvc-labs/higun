package indexer

import "github.com/metaid/utxo_indexer/common"

// FtMempoolManager 定义了FT内存池管理器需要实现的接口
type FtMempoolManager interface {
	// GetFtUTXOsByAddress 获取指定地址的FT内存池UTXO
	GetFtUTXOsByAddress(address string, codeHash string, genesis string) (incomeUtxoList []common.FtUtxo, spendUtxoList []common.FtUtxo, err error)

	// GetFtInfoByCodeHashGenesis 通过合约代码哈希和创世交易ID获取FT信息
	GetFtInfoByCodeHashGenesis(codeHash string, genesis string) (*common.FtInfoModel, error)
}
