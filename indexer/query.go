package indexer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/metaid/utxo_indexer/common"
)

type Balance struct {
	ConfirmedBalanceSatoshi uint64  `json:"confirmed_balance_satoshi"`
	ConfirmedBalance        float64 `json:"confirmed_balance"`
	BalanceSatoshi          uint64  `json:"balance_satoshi"`
	Balance                 float64 `json:"balance"`
	UTXOCount               int64   `json:"confirmed_utxo_count"`
	MempoolIncome           int64   `json:"mempool_income_satoshi"`
	MempoolIncomeBTC        float64 `json:"mempool_income"`
	MempoolSpend            int64   `json:"mempool_spend_satoshi"`
	MempoolSpendBTC         float64 `json:"mempool_spend"`
	MempoolUTXOCount        int64   `json:"mempool_utxo_count"`
}

func (i *UTXOIndexer) GetBalance(address string) (balanceResult Balance, err error) {
	addrKey := []byte(address)
	spendMap := make(map[string]struct{})
	var income int64
	var spend int64
	var mempoolIncome int64
	var mempoolSpend int64
	var mempoolUtxoCount int64
	var utxoCount int64
	mempoolCheckTxMap := make(map[string]int64)
	defer func() {
		if spendMap != nil {
			spendMap = nil
		}
	}()

	spendData, _, err := i.spendStore.GetWithShard(addrKey)
	if err == nil {
		for _, spendTx := range strings.Split(string(spendData), ",") {
			if spendTx == "" {
				continue
			}
			spendMap[spendTx] = struct{}{}
		}
	}

	// Get with shard info for debugging
	data, _, err := i.addressStore.GetWithShard(addrKey)
	if err == nil {
		parts := strings.Split(string(data), ",")

		incomeMap := make(map[string]struct{})

		for _, part := range parts {
			incomes := strings.Split(part, "@")
			if len(incomes) < 3 {
				continue
			}
			key := incomes[0] + ":" + incomes[1]
			if _, exists := incomeMap[key]; exists {
				continue
			}
			incomeMap[key] = struct{}{}

			in, err := strconv.ParseInt(incomes[2], 10, 64)
			if err != nil {
				continue
			}
			if _, exists := spendMap[key]; exists {
				spend += in
			}
			income += in
			utxoCount += 1
			mempoolCheckTxMap[key] = in
		}
	}
	balance := income - spend
	// 转换为BTC单位 (1 BTC = 100,000,000 satoshis)
	btcBalance := float64(balance) / 1e8
	mempoolIncomeList, err := i.mempoolManager.GetUTXOsByAddress(address)
	if err == nil {
		for _, utxo := range mempoolIncomeList {
			in, err := strconv.ParseInt(utxo.Amount, 10, 64)
			if err != nil {
				continue
			}
			mempoolIncome += in
			mempoolCheckTxMap[utxo.TxID] = in
		}
		// for _, utxo := range mempoolSpendList {
		// 	in, err := strconv.ParseInt(utxo.Amount, 10, 64)
		// 	if err != nil {
		// 		continue
		// 	}
		// 	mempoolSpend += in
		// }
		//mempoolUtxoCount = int64(len(mempoolIncomeList) + len(mempoolSpendList))
	}
	//检查内存池是否花费
	if len(mempoolCheckTxMap) > 0 {
		var list []string
		for txPoint := range mempoolCheckTxMap {
			list = append(list, txPoint)
		}
		mempoolSpendMap, _ := i.mempoolManager.GetSpendUTXOs(list)
		for txPoint := range mempoolSpendMap {
			if _, exists := mempoolCheckTxMap[txPoint]; exists {
				mempoolSpend += mempoolCheckTxMap[txPoint]
				mempoolUtxoCount += 1
			}
		}

	}
	lastBalance := balance + mempoolIncome - mempoolSpend
	balanceResult = Balance{
		ConfirmedBalanceSatoshi: uint64(balance),
		ConfirmedBalance:        btcBalance,
		Balance:                 float64(lastBalance) / 1e8,
		BalanceSatoshi:          uint64(lastBalance),
		UTXOCount:               utxoCount,
		MempoolIncome:           mempoolIncome,
		MempoolIncomeBTC:        float64(mempoolIncome) / 1e8,
		MempoolSpend:            mempoolSpend,
		MempoolSpendBTC:         float64(mempoolSpend) / 1e8,
		MempoolUTXOCount:        mempoolUtxoCount,
	}
	// 清理内存
	spendMap = nil
	mempoolCheckTxMap = nil

	return balanceResult, nil
}
func (i *UTXOIndexer) GetUTXOs(address string) (result []UTXO, err error) {
	// 1. 获取已确认的UTXO
	addrKey := []byte(address)
	spendMap := make(map[string]struct{})
	incomeMap := make(map[string]struct{})
	mempoolCheckTxMap := make(map[string]int64)
	var utxos []UTXO
	// 2. 获取内存池UTXO
	if i.mempoolManager != nil {
		mempoolIncomeList, err := i.mempoolManager.GetUTXOsByAddress(address)
		if err == nil {
			for _, utxo := range mempoolIncomeList {
				txArray := strings.Split(utxo.TxID, ":")
				if len(txArray) < 2 {
					continue
				}
				amount, err := strconv.ParseInt(utxo.Amount, 10, 64)
				if err != nil {
					continue
				}
				utxos = append(utxos, UTXO{
					TxID:      txArray[0],
					Index:     txArray[1],
					Amount:    uint64(amount),
					IsMempool: true,
				})
				incomeMap[utxo.TxID] = struct{}{}
				mempoolCheckTxMap[utxo.TxID] = amount
			}

		}
	}
	defer func() {
		if spendMap != nil {
			spendMap = nil
		}
		if incomeMap != nil {
			incomeMap = nil
		}
	}()
	data, _, _ := i.addressStore.GetWithShard(addrKey)
	// 获取已花费的UTXO
	spendData, _, err := i.spendStore.GetWithShard(addrKey)
	if err == nil {
		for _, spendTx := range strings.Split(string(spendData), ",") {
			if spendTx == "" {
				continue
			}
			spendMap[spendTx] = struct{}{}
		}
	}

	// 处理已确认的UTXO
	if data != nil {
		parts := strings.Split(string(data), ",")
		for _, part := range parts {
			incomes := strings.Split(part, "@")
			if len(incomes) < 3 {
				continue
			}
			key := incomes[0] + ":" + incomes[1]
			if _, exists := incomeMap[key]; exists {
				continue
			}
			incomeMap[key] = struct{}{}

			in, err := strconv.ParseInt(incomes[2], 10, 64)
			if err != nil {
				continue
			}
			if _, exists := spendMap[key]; exists {
				continue
			}
			if in <= 1000 {
				continue
			}
			utxos = append(utxos, UTXO{
				TxID:      incomes[0],
				Index:     incomes[1],
				Amount:    uint64(in),
				IsMempool: false,
			})
			mempoolCheckTxMap[key] = in
		}
	}
	// 检查内存池是否花费
	if len(mempoolCheckTxMap) > 0 {
		var list []string
		for txPoint := range mempoolCheckTxMap {
			list = append(list, txPoint)
		}
		mempoolSpendMap, _ := i.mempoolManager.GetSpendUTXOs(list)

		for txPoint := range mempoolSpendMap {
			if _, exists := mempoolCheckTxMap[txPoint]; exists {
				spendMap[txPoint] = struct{}{}
			}
		}

	}
	//最后过滤
	for _, utxo := range utxos {
		if _, exists := spendMap[utxo.TxID+":"+utxo.Index]; exists {
			continue // 如果已花费，则跳过
		}
		result = append(result, utxo)
	}
	// 清理内存
	mempoolCheckTxMap = nil
	spendMap = nil
	incomeMap = nil
	return result, nil
}
func (i *UTXOIndexer) GetSpendUTXOs(address string) (utxos []string, err error) {
	// 1. 获取已确认的UTXO
	addrKey := []byte(address)
	// 获取已花费的UTXO
	spendData, _, err := i.spendStore.GetWithShard(addrKey)
	if err == nil {
		for _, spendTx := range strings.Split(string(spendData), ",") {
			if spendTx == "" {
				continue
			}
			utxos = append(utxos, spendTx)
		}
	}

	return utxos, nil
}

type UTXO struct {
	TxID      string `json:"tx_id"`
	Index     string `json:"index"`
	Amount    uint64 `json:"amount"`
	IsMempool bool   `json:"is_mempool"`
}

func (i *UTXOIndexer) GetDbUtxoByTx(tx string) ([]byte, error) {
	return i.utxoStore.Get([]byte(tx))
}

// GetMempoolUTXOs 查询地址在内存池中的UTXO
func (i *UTXOIndexer) GetMempoolUTXOs(address string) (mempoolIncomeList []common.Utxo, mempoolSpendList []common.Utxo, err error) {
	// 检查是否设置了内存池管理器
	if i.mempoolManager == nil {
		return nil, nil, fmt.Errorf("内存池管理器未设置")
	}

	// 直接使用接口方法
	mempoolIncomeList, err = i.mempoolManager.GetUTXOsByAddress(address)
	if err != nil {
		return nil, nil, fmt.Errorf("获取内存池UTXO失败: %w", err)
	}
	return
}

// GetAddressBalance 获取地址余额
func (i *UTXOIndexer) GetAddressBalance(address string) (*Balance, error) {
	// 直接使用GetBalance方法
	balance, err := i.GetBalance(address)
	if err != nil {
		return nil, err
	}
	return &balance, nil
}
