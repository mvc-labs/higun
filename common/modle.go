package common

type Utxo struct {
	TxID    string
	Address string
	Amount  string
}

type FtUtxo struct {
	UtxoId   string
	Index    string
	TxID     string
	Address  string
	Value    string
	Amount   string
	CodeHash string
	Genesis  string
}

// FtInfo 结构体定义
type FtInfoModel struct {
	CodeHash   string
	Genesis    string
	SensibleId string
	Name       string
	Symbol     string
	Decimal    uint8
}
