package indexer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/storage"
)

type FtBalance struct {
	Confirmed               int64  `json:"confirmed"`
	ConfirmedString         string `json:"confirmedString"`
	UnconfirmedIncome       int64  `json:"unconfirmedIncome"`
	UnconfirmedIncomeString string `json:"unconfirmedIncomeString"`
	UnconfirmedSpend        int64  `json:"unconfirmedSpend"`
	UnconfirmedSpendString  string `json:"unconfirmedSpendString"`
	Balance                 int64  `json:"balance"`
	BalanceString           string `json:"balanceString"`
	UTXOCount               int64  `json:"utxoCount"`
	CodeHash                string `json:"codeHash"`
	Genesis                 string `json:"genesis"`
	SensibleId              string `json:"sensibleId"`
	Name                    string `json:"name"`
	Symbol                  string `json:"symbol"`
	Decimal                 uint8  `json:"decimal"`
	FtAddress               string `json:"ftAddress"`
}

type FtUTXO struct {
	CodeHash      string `json:"code_hash"`
	Genesis       string `json:"genesis"`
	Name          string `json:"name"`
	Symbol        string `json:"symbol"`
	SensibleId    string `json:"sensible_id"`
	Decimal       uint8  `json:"decimal"`
	Txid          string `json:"txid"`
	TxIndex       string `json:"txIndex"`
	ValueString   string `json:"valueString"`
	SatoshiString string `json:"satoshiString"`
	Value         int64  `json:"value"`
	Satoshi       int64  `json:"satoshi"`
	Height        int64  `json:"height"`
	Address       string `json:"address"`
	Flag          string `json:"flag"`
}

// FtInfo 结构体定义
type FtInfo struct {
	CodeHash   string `json:"codeHash"`
	Genesis    string `json:"genesis"`
	SensibleId string `json:"sensibleId"`
	Name       string `json:"name"`
	Symbol     string `json:"symbol"`
	Decimal    uint8  `json:"decimal"`
}

func (i *ContractFtIndexer) GetFtBalance(address, codeHash, genesis string) (balanceResults []*FtBalance, err error) {
	addrKey := []byte(address)
	spendMap := make(map[string]struct{})
	defer func() {
		if spendMap != nil {
			spendMap = nil
		}
	}()

	// 获取已花费的FT UTXO
	spendData, _, err := i.addressFtSpendStore.GetWithShard(addrKey)
	if err == nil {
		for _, spendValue := range strings.Split(string(spendData), ",") {
			if spendValue == "" {
				continue
			}
			spendValueStrs := strings.Split(spendValue, "@")
			if len(spendValueStrs) != 6 {
				continue
			}
			outpoint := spendValueStrs[0] + ":" + spendValueStrs[1]
			spendMap[outpoint] = struct{}{}
		}
	}

	// 获取内存池中的UTXO
	var mempoolIncomeList, mempoolSpendList []common.FtUtxo
	if i.mempoolMgr != nil {
		mempoolIncomeList, mempoolSpendList, err = i.mempoolMgr.GetFtUTXOsByAddress(address, codeHash, genesis)
		if err != nil {
			return nil, fmt.Errorf("获取内存池UTXO失败: %w", err)
		}
	}

	// 处理内存池中的已花费UTXO
	for _, utxo := range mempoolSpendList {
		key := utxo.TxID + ":" + utxo.Index
		spendMap[key] = struct{}{}
	}

	// 获取FT收入数据
	data, _, err := i.addressFtIncomeStore.GetWithShard(addrKey)
	fmt.Println("data", string(data))
	if err != nil {
		fmt.Println("err", err)
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	// 按codeHash和genesis分类统计
	balanceMap := make(map[string]*FtBalance)
	parts := strings.Split(string(data), ",")

	for _, part := range parts {
		if part == "" {
			continue
		}
		incomes := strings.Split(part, "@")
		if len(incomes) < 6 {
			continue
		}

		// 解析数据
		currCodeHash := incomes[0]
		currGenesis := incomes[1]
		currAmount := incomes[2]
		currTxID := incomes[3]
		currIndex := incomes[4]
		currValue := incomes[5]
		_ = currValue

		// 如果指定了codeHash和genesis，则只处理匹配的
		if codeHash != "" && codeHash != currCodeHash {
			continue
		}
		if genesis != "" && genesis != currGenesis {
			continue
		}

		// 检查是否已花费
		key := currTxID + ":" + currIndex
		if _, exists := spendMap[key]; exists {
			continue
		}

		// 获取或创建余额记录
		balanceKey := currCodeHash + "@" + currGenesis
		balance, exists := balanceMap[balanceKey]
		if !exists {
			// 获取FT信息
			ftInfo, err := i.GetFtInfo(currCodeHash + "@" + currGenesis)
			if err != nil {
				continue
			}
			balance = &FtBalance{
				CodeHash:   currCodeHash,
				Genesis:    currGenesis,
				SensibleId: ftInfo.SensibleId,
				Name:       ftInfo.Name,
				Symbol:     ftInfo.Symbol,
				Decimal:    ftInfo.Decimal,
				FtAddress:  address,
			}
			balanceMap[balanceKey] = balance
		}

		// 更新余额
		amount, err := strconv.ParseInt(currAmount, 10, 64)
		if err != nil {
			continue
		}
		balance.Confirmed += amount
		balance.ConfirmedString = strconv.FormatInt(balance.Confirmed, 10)
		balance.UTXOCount++
	}

	// 处理内存池中的收入UTXO
	for _, utxo := range mempoolIncomeList {
		// 如果指定了codeHash和genesis，则只处理匹配的
		if codeHash != "" && codeHash != utxo.CodeHash {
			continue
		}
		if genesis != "" && genesis != utxo.Genesis {
			continue
		}

		// 检查是否已花费
		key := utxo.TxID + ":" + utxo.Index
		if _, exists := spendMap[key]; exists {
			continue
		}

		// 获取或创建余额记录
		balanceKey := utxo.CodeHash + "@" + utxo.Genesis
		balance, exists := balanceMap[balanceKey]
		if !exists {
			// 获取FT信息
			ftInfo, err := i.GetFtInfo(balanceKey)
			if err != nil {
				continue
			}
			balance = &FtBalance{
				CodeHash:   utxo.CodeHash,
				Genesis:    utxo.Genesis,
				SensibleId: ftInfo.SensibleId,
				Name:       ftInfo.Name,
				Symbol:     ftInfo.Symbol,
				Decimal:    ftInfo.Decimal,
				FtAddress:  address,
			}
			balanceMap[balanceKey] = balance
		}

		// 更新未确认收入余额
		amount, err := strconv.ParseInt(utxo.Amount, 10, 64)
		if err != nil {
			continue
		}
		balance.UnconfirmedIncome += amount
		balance.UnconfirmedIncomeString = strconv.FormatInt(balance.UnconfirmedIncome, 10)
		balance.UTXOCount++
	}

	// 处理内存池中的已花费UTXO
	for _, utxo := range mempoolSpendList {
		// 如果指定了codeHash和genesis，则只处理匹配的
		if codeHash != "" && codeHash != utxo.CodeHash {
			continue
		}
		if genesis != "" && genesis != utxo.Genesis {
			continue
		}

		// 获取或创建余额记录
		balanceKey := utxo.CodeHash + "@" + utxo.Genesis
		balance, exists := balanceMap[balanceKey]
		if !exists {
			// 获取FT信息
			ftInfo, err := i.GetFtInfo(balanceKey)
			if err != nil {
				continue
			}
			balance = &FtBalance{
				CodeHash:   utxo.CodeHash,
				Genesis:    utxo.Genesis,
				SensibleId: ftInfo.SensibleId,
				Name:       ftInfo.Name,
				Symbol:     ftInfo.Symbol,
				Decimal:    ftInfo.Decimal,
				FtAddress:  address,
			}
			balanceMap[balanceKey] = balance
		}

		// 更新未确认支出余额
		amount, err := strconv.ParseInt(utxo.Amount, 10, 64)
		if err != nil {
			continue
		}
		balance.UnconfirmedSpend += amount
		balance.UnconfirmedSpendString = strconv.FormatInt(balance.UnconfirmedSpend, 10)
	}

	// 计算最终余额并转换map为slice
	for _, balance := range balanceMap {
		// 计算总余额：已确认 + 未确认收入 - 未确认支出
		balance.Balance = balance.Confirmed + balance.UnconfirmedIncome - balance.UnconfirmedSpend
		balance.BalanceString = strconv.FormatInt(balance.Balance, 10)
		balanceResults = append(balanceResults, balance)
	}

	return balanceResults, nil
}

func (i *ContractFtIndexer) GetFtUTXOs(address, codeHash, genesis string) (utxos []*FtUTXO, err error) {
	addrKey := []byte(address)
	spendMap := make(map[string]struct{})
	defer func() {
		if spendMap != nil {
			spendMap = nil
		}
	}()

	// 获取已花费的FT UTXO
	spendData, _, err := i.addressFtSpendStore.GetWithShard(addrKey)
	if err == nil {
		for _, spendValue := range strings.Split(string(spendData), ",") {
			if spendValue == "" {
				continue
			}
			spendValueStrs := strings.Split(spendValue, "@")
			if len(spendValueStrs) != 6 {
				continue
			}
			outpoint := spendValueStrs[0] + ":" + spendValueStrs[1]
			spendMap[outpoint] = struct{}{}
		}
	}

	// 获取内存池中的UTXO
	var mempoolIncomeList, mempoolSpendList []common.FtUtxo
	if i.mempoolMgr != nil {
		mempoolIncomeList, mempoolSpendList, err = i.mempoolMgr.GetFtUTXOsByAddress(address, codeHash, genesis)
		if err != nil {
			return nil, fmt.Errorf("获取内存池UTXO失败: %w", err)
		}
	}

	// 处理内存池中的已花费UTXO
	for _, utxo := range mempoolSpendList {
		key := utxo.TxID + ":" + utxo.Index
		spendMap[key] = struct{}{}
	}

	// 获取FT收入数据
	data, _, err := i.addressFtIncomeStore.GetWithShard(addrKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	parts := strings.Split(string(data), ",")
	for _, part := range parts {
		if part == "" {
			continue
		}
		incomes := strings.Split(part, "@")
		if len(incomes) < 6 {
			continue
		}

		// 解析数据
		currCodeHash := incomes[0]
		currGenesis := incomes[1]
		currAmount := incomes[2]
		currTxID := incomes[3]
		currIndex := incomes[4]
		currValue := incomes[5]

		// 如果指定了codeHash和genesis，则只处理匹配的
		if codeHash != "" && codeHash != currCodeHash {
			continue
		}
		if genesis != "" && genesis != currGenesis {
			continue
		}

		// 检查是否已花费
		key := currTxID + ":" + currIndex
		if _, exists := spendMap[key]; exists {
			continue
		}

		// 获取FT信息
		ftInfo, err := i.GetFtInfo(currCodeHash + "@" + currGenesis)
		if err != nil {
			continue
		}

		// 解析金额
		amount, err := strconv.ParseInt(currAmount, 10, 64)
		if err != nil {
			continue
		}
		value, err := strconv.ParseInt(currValue, 10, 64)
		if err != nil {
			continue
		}

		utxos = append(utxos, &FtUTXO{
			Txid:          currTxID,
			TxIndex:       currIndex,
			Value:         amount,
			ValueString:   currAmount,
			Satoshi:       value,
			SatoshiString: currValue,
			CodeHash:      currCodeHash,
			Genesis:       currGenesis,
			SensibleId:    ftInfo.SensibleId,
			Name:          ftInfo.Name,
			Symbol:        ftInfo.Symbol,
			Decimal:       ftInfo.Decimal,
			Address:       address,
			Height:        -1, // 已确认的UTXO
			Flag:          "confirmed",
		})
	}

	// 处理内存池中的收入UTXO
	for _, utxo := range mempoolIncomeList {
		// 如果指定了codeHash和genesis，则只处理匹配的
		if codeHash != "" && codeHash != utxo.CodeHash {
			continue
		}
		if genesis != "" && genesis != utxo.Genesis {
			continue
		}

		// 检查是否已花费
		key := utxo.TxID + ":" + utxo.Index
		if _, exists := spendMap[key]; exists {
			continue
		}

		// 获取FT信息
		ftInfo, err := i.GetFtInfo(utxo.CodeHash + "@" + utxo.Genesis)
		if err != nil {
			continue
		}

		// 解析金额
		amount, err := strconv.ParseInt(utxo.Amount, 10, 64)
		if err != nil {
			continue
		}
		value, err := strconv.ParseInt(utxo.Value, 10, 64)
		if err != nil {
			continue
		}

		utxos = append(utxos, &FtUTXO{
			Txid:          utxo.TxID,
			TxIndex:       utxo.Index,
			Value:         amount,
			ValueString:   utxo.Amount,
			Satoshi:       value,
			SatoshiString: utxo.Value,
			CodeHash:      utxo.CodeHash,
			Genesis:       utxo.Genesis,
			SensibleId:    ftInfo.SensibleId,
			Name:          ftInfo.Name,
			Symbol:        ftInfo.Symbol,
			Decimal:       ftInfo.Decimal,
			Address:       address,
			Height:        0, // 内存池中的UTXO
			Flag:          "unconfirmed",
		})
	}

	return utxos, nil
}

func (i *ContractFtIndexer) GetDbFtUtxoByTx(tx string) ([]byte, error) {
	return i.contractFtUtxoStore.Get([]byte(tx))
}

func (i *ContractFtIndexer) GetDbAddressFtIncomeByTx(address string) ([]byte, error) {
	return i.addressFtIncomeStore.Get([]byte(address))
}

func (i *ContractFtIndexer) GetDbAddressFtSpendByTx(address string) ([]byte, error) {
	return i.addressFtSpendStore.Get([]byte(address))
}

// GetFtInfo 获取FT信息
func (i *ContractFtIndexer) GetFtInfo(key string) (*FtInfo, error) {
	// 从contractFtInfoStore获取FT信息
	data, err := i.contractFtInfoStore.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("获取FT信息失败: %w", err)
	}

	fmt.Println("data", string(data))
	// 解析FT信息
	parts := strings.Split(string(data), "@")
	if len(parts) < 4 {
		return nil, fmt.Errorf("FT信息格式错误")
	}
	decimal, err := strconv.ParseUint(parts[3], 10, 8)
	if err != nil {
		return nil, fmt.Errorf("解析decimal失败: %w", err)
	}

	return &FtInfo{
		CodeHash:   strings.Split(key, "@")[0],
		Genesis:    strings.Split(key, "@")[1],
		SensibleId: parts[0],
		Name:       parts[1],
		Symbol:     parts[2],
		Decimal:    uint8(decimal),
	}, nil
}

// GetAddressFtBalance 获取地址FT余额
func (i *ContractFtIndexer) GetAddressFtBalance(address string) ([]*FtBalance, error) {
	return i.GetFtBalance(address, "", "")
}

// GetAddressFtUTXOs 获取地址FT UTXO列表
func (i *ContractFtIndexer) GetAddressFtUTXOs(address string) ([]*FtUTXO, error) {
	return i.GetFtUTXOs(address, "", "")
}

// GetMempoolUTXOs 查询地址在内存池中的UTXO
func (i *ContractFtIndexer) GetMempoolFtUTXOs(address string, codeHash string, genesis string) (mempoolIncomeList []common.FtUtxo, mempoolSpendList []common.FtUtxo, err error) {
	// 检查是否设置了内存池管理器
	if i.mempoolMgr == nil {
		return nil, nil, fmt.Errorf("内存池管理器未设置")
	}

	// 直接使用接口方法
	mempoolIncomeList, mempoolSpendList, err = i.mempoolMgr.GetFtUTXOsByAddress(address, codeHash, genesis)
	if err != nil {
		return nil, nil, fmt.Errorf("获取内存池UTXO失败: %w", err)
	}
	return
}

// GetAllDbAddressFtIncome 获取所有地址的 FT 收入数据
func (i *ContractFtIndexer) GetAllDbAddressFtIncome() (map[string]string, error) {
	result := make(map[string]string)

	// 遍历所有分片
	for _, db := range i.addressFtIncomeStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("创建迭代器失败: %w", err)
		}
		defer iter.Close()

		// 遍历所有键值对
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = value
		}
	}

	return result, nil
}

// GetAllDbAddressFtSpend 获取所有地址的 FT 支出数据
func (i *ContractFtIndexer) GetAllDbAddressFtSpend() (map[string]string, error) {
	result := make(map[string]string)

	// 遍历所有分片
	for _, db := range i.addressFtSpendStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("创建迭代器失败: %w", err)
		}
		defer iter.Close()

		// 遍历所有键值对
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = value
		}
	}

	return result, nil
}
