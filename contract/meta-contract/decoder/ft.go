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

// ContractType represents contract type
type ContractType int

const (
	ContractTypeUnknown ContractType = iota
	ContractTypeFT
	ContractTypeUnique
)

// GetContractType determines script type
func GetContractType(script []byte) ContractType {
	if IsFTContract(script) {
		return ContractTypeFT
	}
	if IsUniqueContract(script) {
		return ContractTypeUnique
	}
	return ContractTypeUnknown
}

// IsFTContract checks if it's an FT contract
func IsFTContract(script []byte) bool {
	txoData := &scriptDecoder.TxoData{}
	isValid := scriptDecoder.DecodeMvcTxo(script, txoData)
	if !isValid {
		return false
	}
	return txoData.CodeType == scriptDecoder.CodeType_FT
}

// IsUniqueContract checks if it's a Unique contract
func IsUniqueContract(script []byte) bool {
	txoData := &scriptDecoder.TxoData{}
	isValid := scriptDecoder.DecodeMvcTxo(script, txoData)
	if !isValid {
		return false
	}
	return txoData.CodeType == scriptDecoder.CodeType_UNIQUE
}

// ExtractFTInfo extracts FT contract information
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

	// Verify if FT data exists
	if txoData.FT == nil {
		return nil, nil
	}

	// Convert to FTUtxoInfo structure
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

	// Set Genesis field based on GenesisIdLen
	if txoData.GenesisIdLen == 40 {
		// If GenesisIdLen is 40, the last 20 bytes are genesis
		ftUtxoInfo.Genesis = hex.EncodeToString(txoData.GenesisId[20:])
	} else if txoData.GenesisIdLen == 20 {
		// If GenesisIdLen is 20, the entire GenesisId is genesis
		ftUtxoInfo.Genesis = hex.EncodeToString(txoData.GenesisId[:20])
	}

	// If address information exists, add to result
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
	// Create P2PKH address
	addr, err := btcutil.NewAddressPubKeyHash(pkhBytes, params)
	if err != nil {
		return "", err
	}

	return addr.String(), nil
}

// ExtractFTInfo extracts FT contract information
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

	// Convert to UniqueUtxoInfo structure
	uniqueUtxoInfo := &UniqueUtxoInfo{
		CodeType:   txoData.CodeType,
		CodeHash:   hex.EncodeToString(txoData.CodeHash[:]),
		GenesisId:  hex.EncodeToString(txoData.GenesisId[:]),
		SensibleId: hex.EncodeToString(txoData.Uniq.SensibleId),
		CustomData: hex.EncodeToString(txoData.Uniq.CustomData),
	}

	// Set Genesis field based on GenesisIdLen
	if txoData.GenesisIdLen == 40 {
		// If GenesisIdLen is 40, the last 20 bytes are genesis
		uniqueUtxoInfo.Genesis = hex.EncodeToString(txoData.GenesisId[20:])
	} else if txoData.GenesisIdLen == 20 {
		// If GenesisIdLen is 20, the entire GenesisId is genesis
		uniqueUtxoInfo.Genesis = hex.EncodeToString(txoData.GenesisId[:20])
	}
	return uniqueUtxoInfo, nil
}

// ParseSensibleId parses sensibleId string, returns genesisTxId and genesisOutputIndex
func ParseSensibleId(sensibleId string) (string, uint32, error) {
	// Convert hex string to byte array
	sensibleIDBuf, err := hex.DecodeString(sensibleId)
	if err != nil {
		return "", 0, err
	}

	// Check if length is sufficient
	if len(sensibleIDBuf) < 36 {
		return "", 0, fmt.Errorf("sensibleId length too short")
	}

	// Get first 32 bytes as genesisTxId and reverse byte order
	genesisTxId := make([]byte, 32)
	copy(genesisTxId, sensibleIDBuf[:32])
	for i, j := 0, len(genesisTxId)-1; i < j; i, j = i+1, j-1 {
		genesisTxId[i], genesisTxId[j] = genesisTxId[j], genesisTxId[i]
	}

	// Get last 4 bytes as genesisOutputIndex
	genesisOutputIndex := binary.LittleEndian.Uint32(sensibleIDBuf[32:36])

	return hex.EncodeToString(genesisTxId), genesisOutputIndex, nil
}
