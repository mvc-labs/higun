package decoder

import (
	"encoding/hex"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"

	scriptDecoder "github.com/mvc-labs/metacontract-script-decoder"
)

type FTUtxoInfo struct {
	CodeType   uint32
	CodeHash   string
	GenesisId  string
	Genesis    string
	SensibleId string // GenesisTx outpoint
	Name       string // ft name
	Symbol     string // ft symbol
	Amount     uint64 // ft amount
	Decimal    uint8  // ft decimal
	Address    string
}

// IsFTContract 检查是否是 FT 合约
func IsFTContract(script []byte) bool {
	txoData := &scriptDecoder.TxoData{}
	isValid := scriptDecoder.DecodeMvcTxo(script, txoData)
	if !isValid {
		return false
	}
	return txoData.CodeType == scriptDecoder.CodeType_FT
}

// ExtractFTInfo 提取 FT 合约信息
func ExtractFTInfo(script []byte) (*scriptDecoder.TxoData, error) {
	txoData := &scriptDecoder.TxoData{}
	isValid := scriptDecoder.DecodeMvcTxo(script, txoData)
	if !isValid || txoData.CodeType != scriptDecoder.CodeType_FT {
		return nil, nil
	}
	return txoData, nil
}

func ExtractFTUtxoInfo(script []byte, param *chaincfg.Params) (*FTUtxoInfo, error) {
	txoData, err := ExtractFTInfo(script)
	if err != nil {
		return nil, err
	}
	if txoData == nil {
		return nil, nil
	}

	// 验证FT数据是否存在
	if txoData.FT == nil {
		return nil, nil
	}

	// 转换为FTUtxoInfo结构
	ftUtxoInfo := &FTUtxoInfo{
		CodeType:   txoData.CodeType,
		CodeHash:   hex.EncodeToString(txoData.CodeHash[:]),
		GenesisId:  hex.EncodeToString(txoData.GenesisId[:]),
		SensibleId: hex.EncodeToString(txoData.FT.SensibleId),
		Name:       txoData.FT.Name,
		Symbol:     txoData.FT.Symbol,
		Amount:     txoData.FT.Amount,
		Decimal:    txoData.FT.Decimal,
	}

	// 根据GenesisIdLen设置Genesis字段
	if txoData.GenesisIdLen == 40 {
		// 如果GenesisIdLen为40，则后20字节是genesis
		ftUtxoInfo.Genesis = hex.EncodeToString(txoData.GenesisId[20:])
	} else if txoData.GenesisIdLen == 20 {
		// 如果GenesisIdLen为20，则整个GenesisId就是genesis
		ftUtxoInfo.Genesis = hex.EncodeToString(txoData.GenesisId[:20])
	}

	// 如果有地址信息,添加到结果中
	if txoData.HasAddress {
		address, err := PkhToAddress(hex.EncodeToString(txoData.AddressPkh[:]), param)
		if err != nil {
			return nil, err
		}
		ftUtxoInfo.Address = address
	}

	return ftUtxoInfo, nil
}

func PkhToAddress(pkh string, params *chaincfg.Params) (string, error) {
	pkhBytes, err := hex.DecodeString(pkh)
	if err != nil {
		return "", err
	}
	// 创建P2PKH地址
	addr, err := btcutil.NewAddressPubKeyHash(pkhBytes, params)
	if err != nil {
		return "", err
	}

	return addr.String(), nil
}
