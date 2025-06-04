package decoder

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

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

type UniqueUtxoInfo struct {
	CodeType   uint32
	CodeHash   string
	GenesisId  string
	Genesis    string
	SensibleId string // GenesisTx outpoint
	CustomData string // unique custom data
}

// ContractType 表示合约类型
type ContractType int

const (
	ContractTypeUnknown ContractType = iota
	ContractTypeFT
	ContractTypeUnique
)

// GetContractType 判断脚本类型
func GetContractType(script []byte) ContractType {
	if IsFTContract(script) {
		return ContractTypeFT
	}
	if IsUniqueContract(script) {
		return ContractTypeUnique
	}
	return ContractTypeUnknown
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

// IsUniqueContract 检查是否是 Unique 合约
func IsUniqueContract(script []byte) bool {
	txoData := &scriptDecoder.TxoData{}
	isValid := scriptDecoder.DecodeMvcTxo(script, txoData)
	if !isValid {
		return false
	}
	return txoData.CodeType == scriptDecoder.CodeType_UNIQUE
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

// ExtractFTInfo 提取 FT 合约信息
func ExtractUniqueInfo(script []byte) (*scriptDecoder.TxoData, error) {
	txoData := &scriptDecoder.TxoData{}
	isValid := scriptDecoder.DecodeMvcTxo(script, txoData)
	if !isValid || txoData.CodeType != scriptDecoder.CodeType_UNIQUE {
		return nil, nil
	}
	return txoData, nil
}

func ExtractUniqueUtxoInfo(script []byte, param *chaincfg.Params) (*UniqueUtxoInfo, error) {
	txoData, err := ExtractUniqueInfo(script)
	if err != nil {
		return nil, err
	}
	if txoData == nil {
		return nil, nil
	}

	if txoData.Uniq == nil {
		return nil, nil
	}

	// 转换为UniqueUtxoInfo结构
	uniqueUtxoInfo := &UniqueUtxoInfo{
		CodeType:   txoData.CodeType,
		CodeHash:   hex.EncodeToString(txoData.CodeHash[:]),
		GenesisId:  hex.EncodeToString(txoData.GenesisId[:]),
		SensibleId: hex.EncodeToString(txoData.Uniq.SensibleId),
		CustomData: hex.EncodeToString(txoData.Uniq.CustomData),
	}

	// 根据GenesisIdLen设置Genesis字段
	if txoData.GenesisIdLen == 40 {
		// 如果GenesisIdLen为40，则后20字节是genesis
		uniqueUtxoInfo.Genesis = hex.EncodeToString(txoData.GenesisId[20:])
	} else if txoData.GenesisIdLen == 20 {
		// 如果GenesisIdLen为20，则整个GenesisId就是genesis
		uniqueUtxoInfo.Genesis = hex.EncodeToString(txoData.GenesisId[:20])
	}
	return uniqueUtxoInfo, nil
}

// ParseSensibleId 解析 sensibleId 字符串，返回 genesisTxId 和 genesisOutputIndex
func ParseSensibleId(sensibleId string) (string, uint32, error) {
	// 将 hex 字符串转换为字节数组
	sensibleIDBuf, err := hex.DecodeString(sensibleId)
	if err != nil {
		return "", 0, err
	}

	// 检查长度是否足够
	if len(sensibleIDBuf) < 36 {
		return "", 0, fmt.Errorf("sensibleId length too short")
	}

	// 获取前32字节作为 genesisTxId，并反转字节顺序
	genesisTxId := make([]byte, 32)
	copy(genesisTxId, sensibleIDBuf[:32])
	for i, j := 0, len(genesisTxId)-1; i < j; i, j = i+1, j-1 {
		genesisTxId[i], genesisTxId[j] = genesisTxId[j], genesisTxId[i]
	}

	// 获取后4字节作为 genesisOutputIndex
	genesisOutputIndex := binary.LittleEndian.Uint32(sensibleIDBuf[32:36])

	return hex.EncodeToString(genesisTxId), genesisOutputIndex, nil
}
