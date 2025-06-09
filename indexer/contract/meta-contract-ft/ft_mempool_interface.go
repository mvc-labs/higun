package indexer

import "github.com/metaid/utxo_indexer/common"

// FtMempoolManager 定义了FT内存池管理器需要实现的接口
type FtMempoolManager interface {
	// GetFtUTXOsByAddress 获取指定地址的FT内存池UTXO
	GetFtUTXOsByAddress(address string, codeHash string, genesis string) (incomeUtxoList []common.FtUtxo, spendUtxoList []common.FtUtxo, err error)

	// GetFtInfoByCodeHashGenesis 通过合约代码哈希和创世交易ID获取FT信息
	GetFtInfoByCodeHashGenesis(codeHash string, genesis string) (*common.FtInfoModel, error)

	// GetMempoolAddressFtSpendMap 获取内存池中地址的FT支出数据
	GetMempoolAddressFtSpendMap(address string) (map[string]string, error)

	// GetMempoolUniqueFtSpendMap 获取内存池中唯一FT的支出数据
	GetMempoolUniqueFtSpendMap(codeHashGenesis string) (map[string]string, error)

	// // StartMempoolZmq 启动内存池ZMQ
	// StartMempoolZmq() error

	// // InitializeMempool 初始化内存池
	// InitializeMempool(bcClient interface{})
}
