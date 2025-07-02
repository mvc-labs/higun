package blockchain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strings"

	chaincfg2 "github.com/bitcoinsv/bsvd/chaincfg"
	wire2 "github.com/bitcoinsv/bsvd/wire"

	bsvwire "github.com/bitcoinsv/bsvd/wire"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

type TxOut struct {
	n          uint
	amount     []byte
	lockScript []byte
	scriptType int64 //TODO
}
type TxIn struct {
	inType     int
	TxID       []byte
	Vout       []byte
	scriptSig  []byte
	sequence   []byte
	lockScript []byte
}
type RawTransaction struct {
	TxID          string
	Size          uint64
	Hex           string
	BlockHash     string
	BlockHeight   uint64
	Confirmations uint64
	Blocktime     int64
	inSize        uint64
	outSize       uint64

	Version  []byte
	Vins     []TxIn
	Vouts    []TxOut
	LockTime []byte
	Witness  bool
}

func GetNewHash(msgTx *wire.MsgTx) (newHash string, err error) {
	buffer := new(bytes.Buffer)
	err = msgTx.Serialize(buffer)
	if err != nil {
		return
	}
	transaction, err := DecodeRawTransaction(buffer.Bytes())
	if err != nil {
		return
	}
	newHash = transaction.TxID
	return
}
func GetNewHash2(msgTx *bsvwire.MsgTx) (newHash string, err error) {
	buffer := new(bytes.Buffer)
	err = msgTx.Serialize(buffer)
	if err != nil {
		return
	}
	transaction, err := DecodeRawTransaction(buffer.Bytes())
	if err != nil {
		return
	}
	newHash = transaction.TxID
	return
}
func GetTxID(hexString string) string {
	code, _ := hex.DecodeString(hexString)
	dHash := DoubleHashB(code)
	return hex.EncodeToString(reverseBytes(dHash))
}

// DoubleHashB calculates hash(hash(b)) and returns the resulting bytes.
func DoubleHashB(b []byte) []byte {
	first := sha256.Sum256(b)
	second := sha256.Sum256(first[:])
	return second[:]
}
func reverseBytes(s []byte) []byte {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}
func Uint32ToLittleEndianBytes(data uint32) []byte {
	tmp := [4]byte{}
	binary.LittleEndian.PutUint32(tmp[:], data)
	return tmp[:]
}
func SHA256(message []byte) []byte {
	hash := sha256.New()
	hash.Write(message)
	bytes := hash.Sum(nil)
	return bytes
}

func DecodeRawTransaction(txBytes []byte) (*RawTransaction, error) {
	limit := len(txBytes)
	if limit == 0 {
		return nil, errors.New("invalid transaction data")
	}
	var rawTx RawTransaction
	index := 0
	if index+4 > limit {
		return nil, errors.New("invalid transaction data length")
	}
	rawTx.Version = txBytes[index : index+4]
	index += 4

	if index+2 > limit {
		return nil, errors.New("invalid transaction data length")
	}
	if index+1 > limit {
		return nil, errors.New("invalid transaction data length")
	}

	icount, lenth := DecodeVarIntForTx(txBytes[index : index+9])
	numOfVins := icount
	rawTx.inSize = uint64(numOfVins)
	index += lenth

	if numOfVins == 0 {
		return nil, errors.New("invalid transaction data")
	}
	for i := 0; i < numOfVins; i++ {
		var tmpTxIn TxIn

		if index+32 > limit {
			return nil, errors.New("invalid transaction data length")
		}
		tmpTxIn.TxID = txBytes[index : index+32]
		index += 32

		if index+4 > limit {
			return nil, errors.New("invalid transaction data length")
		}
		tmpTxIn.Vout = txBytes[index : index+4]
		index += 4

		if index+1 > limit {
			return nil, errors.New("invalid transaction data length")
		}

		vnumber := txBytes[index : index+9]
		icount, lenth = DecodeVarIntForTx(vnumber)
		scriptLen := icount
		index += lenth

		tmpTxIn.scriptSig = txBytes[index : index+scriptLen]
		index += scriptLen

		tmpTxIn.sequence = txBytes[index : index+4]
		index += 4
		rawTx.Vins = append(rawTx.Vins, tmpTxIn)
	}

	if index+1 > limit {
		return nil, errors.New("invalid transaction data length")
	}

	icount, lenth = DecodeVarIntForTx(txBytes[index : index+9])
	numOfVouts := icount
	rawTx.outSize = uint64(numOfVouts)
	index += lenth

	if numOfVouts == 0 {
		return nil, errors.New("invalid transaction data")
	}

	for i := 0; i < numOfVouts; i++ {
		var tmpTxOut TxOut
		tmpTxOut.n = uint((i))
		if index+8 > limit {
			return nil, errors.New("invalid transaction data length")
		}
		tmpTxOut.amount = txBytes[index : index+8]
		index += 8

		if index+1 > limit {
			return nil, errors.New("invalid transaction data length")
		}

		vnumber := txBytes[index : index+9]
		icount, lenth = DecodeVarIntForTx(vnumber)
		lockScriptLen := icount
		index += lenth

		if lockScriptLen == 0 {
			return nil, errors.New("invalid transaction data")
		}
		if index+int(lockScriptLen) > limit {
			return nil, errors.New("invalid transaction data length")
		}
		tmpTxOut.lockScript = txBytes[index : index+int(lockScriptLen)]
		index += int(lockScriptLen)
		rawTx.Vouts = append(rawTx.Vouts, tmpTxOut)
	}

	if index+4 > limit {
		return nil, errors.New("invalid transaction data length")
	}
	rawTx.LockTime = txBytes[index : index+4]
	index += 4

	if index != limit {
		return nil, errors.New("too much transaction data")
	}
	//rawTx.TxID = util.GetTxID(hex.EncodeToString(txBytes))

	if uint64(binary.LittleEndian.Uint32(rawTx.Version)) < 10 {
		rawTx.TxID = GetTxID(hex.EncodeToString(txBytes))
	} else {
		newRawTxByte := GetTxNewRawByte(&rawTx)
		rawTx.TxID = GetTxID(hex.EncodeToString(newRawTxByte))
	}
	return &rawTx, nil
}

func DecodeVarIntForTx(buf []byte) (int, int) {
	//if len(buf) != 9 {
	//	return 0, 0
	//}
	if buf[0] <= 0xfc { //252 uint8_t
		return int(buf[0]), 1
	} else if buf[0] == 0xfd { //253 0xFD followed by the length as uint16_t
		return (int(buf[2]) * int(math.Pow(256, 1))) + int(buf[1]), 3
	} else if buf[0] == 0xfe { //254 0xFE followed by the length as uint32_t
		count := (int(buf[4]) * int(math.Pow(256, 3))) +
			(int(buf[3]) * int(math.Pow(256, 2))) +
			(int(buf[2]) * int(math.Pow(256, 1))) +
			int(buf[1])
		return count, 5
	} else if buf[0] == 0xff { //255 0xFF followed by the length as uint64_t
		count := (int(buf[8]) * int(math.Pow(256, 7))) +
			int(buf[7])*int(math.Pow(256, 6)) +
			int(buf[6])*int(math.Pow(256, 5)) +
			int(buf[5])*int(math.Pow(256, 4)) +
			int(buf[4])*int(math.Pow(256, 3)) +
			int(buf[3])*int(math.Pow(256, 2)) +
			int(buf[2])*int(math.Pow(256, 1)) +
			//int(buf[1])*int(math.Pow(256, 1))
			int(buf[1])
		return count, 9
	}
	return 0, 0
}
func GetTxNewRawByte(transaction *RawTransaction) []byte {
	var (
		newRawTxByte   []byte
		newInputsByte  []byte
		newInputs2Byte []byte
		newOutputsByte []byte
	)
	newRawTxByte = append(newRawTxByte, transaction.Version...)
	newRawTxByte = append(newRawTxByte, transaction.LockTime...)
	newRawTxByte = append(newRawTxByte, Uint32ToLittleEndianBytes(uint32(transaction.inSize))...)
	newRawTxByte = append(newRawTxByte, Uint32ToLittleEndianBytes(uint32(transaction.outSize))...)

	for _, in := range transaction.Vins {
		newInputsByte = append(newInputsByte, in.TxID...)
		newInputsByte = append(newInputsByte, in.Vout...)
		newInputsByte = append(newInputsByte, in.sequence...)

		newInputs2Byte = append(newInputs2Byte, SHA256(in.scriptSig)...)
	}
	newRawTxByte = append(newRawTxByte, SHA256(newInputsByte)...)
	newRawTxByte = append(newRawTxByte, SHA256(newInputs2Byte)...)

	for _, out := range transaction.Vouts {
		newOutputsByte = append(newOutputsByte, out.amount...)
		newOutputsByte = append(newOutputsByte, SHA256(out.lockScript)...)
	}
	newRawTxByte = append(newRawTxByte, SHA256(newOutputsByte)...)
	return newRawTxByte
}
func GetAddressFromScript(pubKey string, pkScript []byte, params *chaincfg.Params, chainName string) (address string) {
	var scriptBytes []byte
	var err error
	if pubKey != "" {
		scriptBytes, err = hex.DecodeString(pubKey)
		if err != nil {
			address = "errAddress"
			return
		}
	} else {
		scriptBytes = pkScript
	}
	_, addrs, _, err := txscript.ExtractPkScriptAddrs(scriptBytes, params)
	if err == nil && len(addrs) > 0 {
		if chainName == "mvc" {
			address = addrs[0].EncodeAddress()
			return
		} else {
			address = addrs[0].String()
			return
		}
	} else {
		address = "errAddress"
		return
	}
}

// DeserializeTransaction deserializes byte array to transaction
func DeserializeTransaction(data []byte) (*wire.MsgTx, error) {
	tx := wire.NewMsgTx(wire.TxVersion)
	err := tx.Deserialize(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// DeserializeTransaction deserializes byte array to transaction
func DeserializeMvcTransaction(data []byte) (*wire.MsgTx, error) {
	// First convert byte array to hexadecimal string
	txHex := hex.EncodeToString(data)

	// Use parseMvcTx to parse transaction
	mvcTx, err := parseMvcTx(txHex)
	if err != nil {
		return nil, err
	}

	// Create new wire.MsgTx
	btcTx := wire.NewMsgTx(wire.TxVersion)

	// Copy version number
	btcTx.Version = mvcTx.Version

	// Copy inputs
	for _, in := range mvcTx.TxIn {
		btcTx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{
				Hash:  chainhash.Hash(in.PreviousOutPoint.Hash),
				Index: in.PreviousOutPoint.Index,
			},
			SignatureScript: in.SignatureScript,
			Sequence:        in.Sequence,
		})
	}

	// Copy outputs
	for _, out := range mvcTx.TxOut {
		btcTx.AddTxOut(&wire.TxOut{
			Value:    out.Value,
			PkScript: out.PkScript,
		})
	}

	// Copy lock time
	btcTx.LockTime = mvcTx.LockTime

	return btcTx, nil
}

func GetMvcNetParams(net string) *chaincfg2.Params {
	var (
		netParams *chaincfg2.Params = &chaincfg2.MainNetParams
	)
	switch strings.ToLower(net) {
	case "mainnet", "livenet":
		netParams = &chaincfg2.MainNetParams
		break
	case "testnet":
		netParams = &chaincfg2.TestNet3Params
		break
	}
	return netParams
}

func parseMvcTx(txRaw string) (*wire2.MsgTx, error) {
	txRawByte, _ := hex.DecodeString(txRaw)
	tx := wire2.NewMsgTx(2)
	err := tx.Deserialize(bytes.NewReader(txRawByte))
	if err != nil {
		return nil, err
	}
	return tx, nil
}
