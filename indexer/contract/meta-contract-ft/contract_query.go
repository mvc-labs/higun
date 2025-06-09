package indexer

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/storage"
)

type FtBalance struct {
	Confirmed                                   int64  `json:"confirmed"`
	ConfirmedString                             string `json:"confirmedString"`
	UnconfirmedIncome                           int64  `json:"unconfirmedIncome"`
	UnconfirmedIncomeString                     string `json:"unconfirmedIncomeString"`
	UnconfirmedSpend                            int64  `json:"unconfirmedSpend"`
	UnconfirmedSpendString                      string `json:"unconfirmedSpendString"`
	UnconfirmedSpendFromConfirmed               int64  `json:"unconfirmedSpendFromConfirmed"`
	UnconfirmedSpendFromConfirmedString         string `json:"unconfirmedSpendFromConfirmedString"`
	UnconfirmedSpendFromUnconfirmedIncome       int64  `json:"unconfirmedSpendFromUnconfirmedIncome"`
	UnconfirmedSpendFromUnconfirmedIncomeString string `json:"unconfirmedSpendFromUnconfirmedIncomeString"`
	Balance                                     int64  `json:"balance"`
	BalanceString                               string `json:"balanceString"`
	UTXOCount                                   int64  `json:"utxoCount"`
	CodeHash                                    string `json:"codeHash"`
	Genesis                                     string `json:"genesis"`
	SensibleId                                  string `json:"sensibleId"`
	Name                                        string `json:"name"`
	Symbol                                      string `json:"symbol"`
	Decimal                                     uint8  `json:"decimal"`
	FtAddress                                   string `json:"ftAddress"`
}

type FtUTXO struct {
	CodeHash      string `json:"codeHash"`
	Genesis       string `json:"genesis"`
	Name          string `json:"name"`
	Symbol        string `json:"symbol"`
	SensibleId    string `json:"sensibleId"`
	Decimal       uint8  `json:"decimal"`
	Txid          string `json:"txid"`
	TxIndex       int64  `json:"txIndex"`
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

type UniqueFtUtxo struct {
	Txid          string `json:"txid"`
	TxIndex       int64  `json:"txIndex"`
	CodeHash      string `json:"codeHash"`
	Genesis       string `json:"genesis"`
	SensibleId    string `json:"sensibleId"`
	Height        int64  `json:"height"`
	CustomData    string `json:"customData"`
	Satoshi       string `json:"satoshi"`
	SatoshiString string `json:"satoshiString"`
}

func (i *ContractFtIndexer) GetFtBalance(address, codeHash, genesis string) (balanceResults []*FtBalance, err error) {
	balanceResults = make([]*FtBalance, 0)
	addrKey := []byte(address)
	spendMap := make(map[string]struct{})
	mempoolSpendMap := make(map[string]struct{})
	blockIncomeMap := make(map[string]struct{})
	confirmedIncomeOutpointList := make([]string, 0)
	unconfirmedIncomeOutpointList := make([]string, 0)
	defer func() {
		if spendMap != nil {
			spendMap = nil
		}
		if mempoolSpendMap != nil {
			mempoolSpendMap = nil
		}
		if blockIncomeMap != nil {
			blockIncomeMap = nil
		}
	}()

	// 获取已花费的FT UTXO
	spendData, _, err := i.addressFtSpendStore.GetWithShard(addrKey)
	if err == nil {
		for _, spendValue := range strings.Split(string(spendData), ",") {
			if spendValue == "" {
				continue
			}
			//spendValue: txid@index@codeHash@genesis@sensibleId@amount@value@height@usedTxId,...
			spendValueStrs := strings.Split(spendValue, "@")
			if len(spendValueStrs) != 9 {
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
		mempoolSpendMap[key] = struct{}{}
	}

	// 获取FT收入数据
	// data, _, err := i.addressFtIncomeStore.GetWithShard(addrKey)
	data, _, err := i.addressFtIncomeValidStore.GetWithShard(addrKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	// 按codeHash和genesis分类统计
	balanceMap := make(map[string]*FtBalance)
	// 用于去重的map
	uniqueUtxoMap := make(map[string]struct{})
	// 用于排序的map
	genesisUtxoMap := make(map[string][]string)

	parts := strings.Split(string(data), ",")
	for _, part := range parts {
		if part == "" {
			continue
		}
		//CodeHash@Genesis@Amount@TxID@Index@Value@height
		incomes := strings.Split(part, "@")
		if len(incomes) < 7 {
			continue
		}

		// 解析数据
		currCodeHash := incomes[0]
		currGenesis := incomes[1]
		currAmount := incomes[2]
		currTxID := incomes[3]
		currIndex := incomes[4]

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

		// 检查是否重复
		if _, exists := uniqueUtxoMap[key]; exists {
			continue
		}
		uniqueUtxoMap[key] = struct{}{}
		confirmedIncomeOutpointList = append(confirmedIncomeOutpointList, key)

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

		// 添加到排序map
		genesisUtxoMap[balanceKey] = append(genesisUtxoMap[balanceKey], key)
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
		// if _, exists := mempoolSpendMap[key]; exists {
		// 	continue
		// }

		// 检查是否重复
		if _, exists := uniqueUtxoMap[key]; exists {
			continue
		}
		uniqueUtxoMap[key] = struct{}{}
		unconfirmedIncomeOutpointList = append(unconfirmedIncomeOutpointList, key)

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

		// 添加到排序map
		genesisUtxoMap[balanceKey] = append(genesisUtxoMap[balanceKey], key)
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

		spendOutpoint := utxo.TxID + ":" + utxo.Index
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

		// fmt.Println("spendOutpoint:", spendOutpoint)
		// fmt.Println("confirmedIncomeOutpointList:", confirmedIncomeOutpointList)
		// fmt.Println("unconfirmedIncomeOutpointList:", unconfirmedIncomeOutpointList)

		for _, outpoint := range confirmedIncomeOutpointList {
			if outpoint == spendOutpoint {
				// fmt.Println("spendOutpoint from confirmed:", spendOutpoint)
				balance.UnconfirmedSpendFromConfirmed += amount
				balance.UnconfirmedSpendFromConfirmedString = strconv.FormatInt(balance.UnconfirmedSpendFromConfirmed, 10)
				break
			}
		}

		for _, outpoint := range unconfirmedIncomeOutpointList {
			if outpoint == spendOutpoint {
				// fmt.Println("spendOutpoint from unconfirmed income:", spendOutpoint)
				balance.UnconfirmedSpendFromUnconfirmedIncome += amount
				balance.UnconfirmedSpendFromUnconfirmedIncomeString = strconv.FormatInt(balance.UnconfirmedSpendFromUnconfirmedIncome, 10)
				break
			}
		}
	}

	// 对每个 balanceKey 的 outpoint 数组进行排序，并只保留第一个元素
	for balanceKey, outpoints := range genesisUtxoMap {
		if len(outpoints) > 0 {
			sort.Strings(outpoints)
			genesisUtxoMap[balanceKey] = []string{outpoints[0]}
		}
	}

	// 获取所有 balanceKey 并排序
	balanceKeys := make([]string, 0, len(genesisUtxoMap))
	for balanceKey := range genesisUtxoMap {
		balanceKeys = append(balanceKeys, balanceKey)
	}

	// 根据每个 balanceKey 对应的第一个 outpoint 进行排序
	sort.Slice(balanceKeys, func(i, j int) bool {
		return genesisUtxoMap[balanceKeys[i]][0] < genesisUtxoMap[balanceKeys[j]][0]
	})

	// 计算最终余额并转换map为slice
	for _, balanceKey := range balanceKeys {
		balance := balanceMap[balanceKey]
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
			//spendValue: txid@index@codeHash@genesis@sensibleId@amount@value@height@usedTxId,...
			spendValueStrs := strings.Split(spendValue, "@")
			if len(spendValueStrs) != 9 {
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
	// data, _, err := i.addressFtIncomeStore.GetWithShard(addrKey)
	data, _, err := i.addressFtIncomeValidStore.GetWithShard(addrKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	// 用于去重的map
	uniqueUtxoMap := make(map[string]*FtUTXO)

	parts := strings.Split(string(data), ",")
	for _, part := range parts {
		if part == "" {
			continue
		}
		//CodeHash@Genesis@Amount@TxID@Index@Value@height
		incomes := strings.Split(part, "@")
		if len(incomes) < 7 {
			continue
		}

		// 解析数据
		currCodeHash := incomes[0]
		currGenesis := incomes[1]
		currAmount := incomes[2]
		currTxID := incomes[3]
		currIndex := incomes[4]
		currValue := incomes[5]
		currHeight := incomes[6]

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

		// 检查是否重复
		if _, exists := uniqueUtxoMap[key]; exists {
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

		height, err := strconv.ParseInt(currHeight, 10, 64)
		if err != nil {
			continue
		}

		txIndex, err := strconv.ParseInt(currIndex, 10, 64)
		if err != nil {
			continue
		}

		utxos = append(utxos, &FtUTXO{
			Txid:          currTxID,
			TxIndex:       txIndex,
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
			Height:        height, // 已确认的UTXO
			Flag:          fmt.Sprintf("%s_%s", currTxID, currIndex),
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

		// 检查是否重复
		if _, exists := uniqueUtxoMap[key]; exists {
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
		txIndex, err := strconv.ParseInt(utxo.Index, 10, 64)
		if err != nil {
			continue
		}

		utxos = append(utxos, &FtUTXO{
			Txid:          utxo.TxID,
			TxIndex:       txIndex,
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
			Height:        -1, // 内存池中的UTXO
			Flag:          fmt.Sprintf("%s_%s", utxo.TxID, utxo.Index),
		})
	}

	return utxos, nil
}

func (i *ContractFtIndexer) GetDbFtUtxoByTx(tx string) ([]byte, error) {
	return i.contractFtUtxoStore.Get([]byte(tx))
}

func (i *ContractFtIndexer) GetDbAddressFtIncome(address string, codeHash string, genesis string) ([]string, error) {
	data, err := i.addressFtIncomeStore.Get([]byte(address))
	if err != nil {
		return nil, err
	}

	if codeHash == "" && genesis == "" {
		return strings.Split(string(data), ","), nil // 返回所有数据
	}

	// 按逗号分割数据
	parts := strings.Split(string(data), ",")
	filteredParts := make([]string, 0)

	// 筛选数据
	for _, part := range parts {
		if part == "" {
			continue
		}
		//CodeHash@Genesis@Amount@TxID@Index@Value@height
		incomes := strings.Split(part, "@")
		if len(incomes) < 7 {
			continue
		}

		currCodeHash := incomes[0]
		currGenesis := incomes[1]

		if (codeHash == "" || currCodeHash == codeHash) && (genesis == "" || currGenesis == genesis) {
			filteredParts = append(filteredParts, part)
		}
	}

	// 重新组合数据
	return filteredParts, nil
}

func (i *ContractFtIndexer) GetDbAddressFtSpend(address string, codeHash string, genesis string) ([]string, error) {
	data, err := i.addressFtSpendStore.Get([]byte(address))
	if err != nil {
		return nil, err
	}

	if codeHash == "" && genesis == "" {
		return strings.Split(string(data), ","), nil // 返回所有数据
	}

	// 按逗号分割数据
	parts := strings.Split(string(data), ",")
	filteredParts := make([]string, 0)

	// 筛选数据
	for _, part := range parts {
		if part == "" {
			continue
		}
		//txid@index@codeHash@genesis@amount@value@height@usedTxId
		spends := strings.Split(part, "@")
		if len(spends) < 8 {
			continue
		}

		currCodeHash := spends[2]
		currGenesis := spends[3]

		if (codeHash == "" || currCodeHash == codeHash) && (genesis == "" || currGenesis == genesis) {
			filteredParts = append(filteredParts, part)
		}
	}

	// 重新组合数据
	return filteredParts, nil
}

// GetFtInfo 获取FT信息
func (i *ContractFtIndexer) GetFtInfo(key string) (*FtInfo, error) {
	// 从contractFtInfoStore获取FT信息
	data, err := i.contractFtInfoStore.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("获取FT信息失败: %w", err)
	}

	// fmt.Println("data", string(data))
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

// GetDbAddressFtIncomeValidByAddress 获取指定地址的有效 FT 收入数据
func (i *ContractFtIndexer) GetDbAddressFtIncomeValid(address string, codeHash string, genesis string) ([]string, error) {
	data, err := i.addressFtIncomeValidStore.Get([]byte(address))
	if err != nil {
		return nil, err
	}

	if codeHash == "" && genesis == "" {
		return strings.Split(string(data), ","), nil // 返回所有数据
	}

	// 按逗号分割数据
	parts := strings.Split(string(data), ",")
	filteredParts := make([]string, 0)

	// 筛选数据
	for _, part := range parts {
		if part == "" {
			continue
		}
		// FtAddress@CodeHash@Genesis@sensibleId@Amount@TxID@Index@Value@height
		// CodeHash@Genesis@Amount@TxID@Index@Value@height
		incomes := strings.Split(part, "@")
		if len(incomes) < 7 {
			continue
		}

		currCodeHash := incomes[0]
		currGenesis := incomes[1]

		if (codeHash == "" || currCodeHash == codeHash) && (genesis == "" || currGenesis == genesis) {
			filteredParts = append(filteredParts, part)
		}
	}

	// 返回筛选后的数据
	return filteredParts, nil
}

// GetAllDbUncheckFtOutpoint 获取未检查的 FT outpoint 数据
// 如果提供了 outpoint 参数，则只返回对应的 value
// 如果没有提供 outpoint 参数，则返回所有数据
func (i *ContractFtIndexer) GetAllDbUncheckFtOutpoint(outpoint string) (map[string]string, error) {
	result := make(map[string]string)

	// 如果提供了 outpoint，直接获取对应的值
	if outpoint != "" {
		value, err := i.uncheckFtOutpointStore.Get([]byte(outpoint))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("获取 outpoint 数据失败: %w", err)
		}
		result[outpoint] = string(value)
		return result, nil
	}

	// 遍历所有分片
	for _, db := range i.uncheckFtOutpointStore.GetShards() {
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

// GetAllDbFtGenesis 获取所有 FT Genesis 数据
func (i *ContractFtIndexer) GetAllDbFtGenesis(key string) (map[string]string, error) {
	result := make(map[string]string)

	// 如果提供了 key，直接获取对应的值
	if key != "" {
		value, err := i.contractFtGenesisStore.Get([]byte(key))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("获取 FT Genesis 数据失败: %w", err)
		}
		result[key] = string(value)
		return result, nil
	}

	// 遍历所有分片
	for _, db := range i.contractFtGenesisStore.GetShards() {
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

// GetAllDbFtGenesisOutput 获取所有 FT Genesis Output 数据
func (i *ContractFtIndexer) GetAllDbFtGenesisOutput(key string) (map[string][]string, error) {
	result := make(map[string][]string)

	// 如果提供了 key，直接获取对应的值
	if key != "" {
		value, err := i.contractFtGenesisOutputStore.Get([]byte(key))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("获取 FT Genesis Output 数据失败: %w", err)
		}
		result[key] = strings.Split(string(value), ",")
		return result, nil
	}

	// 遍历所有分片
	for _, db := range i.contractFtGenesisOutputStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("创建迭代器失败: %w", err)
		}
		defer iter.Close()

		// 遍历所有键值对
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = strings.Split(value, ",")
		}
	}

	return result, nil
}

// GetAllDbUsedFtIncome 获取所有已使用的 FT 收入数据
func (i *ContractFtIndexer) GetAllDbUsedFtIncome(key string) (map[string]string, error) {
	result := make(map[string]string)

	// 如果提供了 key，直接获取对应的值
	if key != "" {
		value, err := i.usedFtIncomeStore.Get([]byte(key))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("获取已使用的 FT 收入数据失败: %w", err)
		}
		result[key] = string(value)
		return result, nil
	}

	// 遍历所有分片
	for _, db := range i.usedFtIncomeStore.GetShards() {
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

// GetAllDbFtGenesisUtxo 获取所有 FT Genesis UTXO 数据
func (i *ContractFtIndexer) GetAllDbFtGenesisUtxo(key string) (map[string]string, error) {
	result := make(map[string]string)

	// 如果提供了 key，直接获取对应的值
	if key != "" {
		value, err := i.contractFtGenesisUtxoStore.Get([]byte(key))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("获取 FT Genesis UTXO 数据失败: %w", err)
		}
		result[key] = string(value)
		return result, nil
	}

	// 遍历所有分片
	for _, db := range i.contractFtGenesisUtxoStore.GetShards() {
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

// GetUncheckFtOutpointTotal 获取未检查的 FT outpoint 总数量
func (i *ContractFtIndexer) GetUncheckFtOutpointTotal() (int64, error) {
	var total int64 = 0

	// 遍历所有分片
	for _, db := range i.uncheckFtOutpointStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return 0, fmt.Errorf("创建迭代器失败: %w", err)
		}
		defer iter.Close()

		// 遍历所有键值对并计数
		for iter.First(); iter.Valid(); iter.Next() {
			total++
		}
	}

	return total, nil
}

// GetUniqueFtUTXOs 获取唯一的 FT UTXO 列表
func (i *ContractFtIndexer) GetUniqueFtUTXOs(codeHash, genesis string) (utxos []*UniqueFtUtxo, err error) {
	// 使用 map 来存储唯一的 UTXO
	uniqueUtxos := make(map[string]*UniqueFtUtxo)

	// 遍历所有分片
	for _, db := range i.addressFtIncomeStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("创建迭代器失败: %w", err)
		}
		defer iter.Close()

		// 遍历所有键值对
		for iter.First(); iter.Valid(); iter.Next() {
			value := string(iter.Value())
			parts := strings.Split(value, ",")

			for _, part := range parts {
				if part == "" {
					continue
				}
				//TxID@Index@Value@sensibleId@customData@height
				incomes := strings.Split(part, "@")
				if len(incomes) < 7 {
					continue
				}

				// 解析数据
				currCodeHash := incomes[0]
				currGenesis := incomes[1]
				currTxID := incomes[3]
				currIndex := incomes[4]
				currValue := incomes[5]
				currHeight := incomes[6]
				currCustomData := incomes[7]

				// 如果指定了 codeHash 和 genesis，则只处理匹配的
				if codeHash != "" && codeHash != currCodeHash {
					continue
				}
				if genesis != "" && genesis != currGenesis {
					continue
				}

				// 创建唯一键
				uniqueKey := currCodeHash + "@" + currGenesis + "@" + currTxID + "@" + currIndex

				// 如果已经存在，跳过
				if _, exists := uniqueUtxos[uniqueKey]; exists {
					continue
				}

				// 获取 FT 信息
				ftInfo, err := i.GetFtInfo(currCodeHash + "@" + currGenesis)
				if err != nil {
					continue
				}

				// 解析高度
				height, err := strconv.ParseInt(currHeight, 10, 64)
				if err != nil {
					continue
				}

				// 解析索引
				index, err := strconv.ParseInt(currIndex, 10, 64)
				if err != nil {
					continue
				}

				// 添加到唯一 UTXO 列表
				uniqueUtxos[uniqueKey] = &UniqueFtUtxo{
					Txid:          currTxID,
					TxIndex:       index,
					CodeHash:      currCodeHash,
					Genesis:       currGenesis,
					SensibleId:    ftInfo.SensibleId,
					Height:        height,
					CustomData:    currCustomData,
					Satoshi:       currValue,
					SatoshiString: currValue,
				}
			}
		}
	}

	// 将 map 转换为 slice
	for _, utxo := range uniqueUtxos {
		utxos = append(utxos, utxo)
	}

	return utxos, nil
}

// GetMempoolAddressFtSpendMap 获取内存池中地址的FT支出数据
func (i *ContractFtIndexer) GetMempoolAddressFtSpendMap(address string) (map[string]string, error) {
	if i.mempoolMgr == nil {
		return nil, fmt.Errorf("内存池管理器未设置")
	}
	return i.mempoolMgr.GetMempoolAddressFtSpendMap(address)
}

// GetMempoolUniqueFtSpendMap 获取内存池中唯一FT的支出数据
func (i *ContractFtIndexer) GetMempoolUniqueFtSpendMap(codeHashGenesis string) (map[string]string, error) {
	if i.mempoolMgr == nil {
		return nil, fmt.Errorf("内存池管理器未设置")
	}
	return i.mempoolMgr.GetMempoolUniqueFtSpendMap(codeHashGenesis)
}
