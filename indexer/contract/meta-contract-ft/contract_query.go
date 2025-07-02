package indexer

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
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

// FtInfo struct definition
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

	// Get spent FT UTXOs
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

	// Get UTXOs in mempool
	var mempoolIncomeList, mempoolSpendList []common.FtUtxo
	if i.mempoolMgr != nil {
		mempoolIncomeList, mempoolSpendList, err = i.mempoolMgr.GetFtUTXOsByAddress(address, codeHash, genesis)
		if err != nil {
			return nil, fmt.Errorf("Failed to get mempool UTXOs: %w", err)
		}
	}

	// Process spent UTXOs in mempool
	for _, utxo := range mempoolSpendList {
		key := utxo.TxID + ":" + utxo.Index
		mempoolSpendMap[key] = struct{}{}
	}

	// Get FT income data
	// data, _, err := i.addressFtIncomeStore.GetWithShard(addrKey)
	data, _, err := i.addressFtIncomeValidStore.GetWithShard(addrKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	// Classify and count by codeHash and genesis
	balanceMap := make(map[string]*FtBalance)
	// Map for deduplication
	uniqueUtxoMap := make(map[string]struct{})
	// Map for sorting
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

		// Parse data
		currCodeHash := incomes[0]
		currGenesis := incomes[1]
		currAmount := incomes[2]
		currTxID := incomes[3]
		currIndex := incomes[4]

		// If codeHash and genesis are specified, only process matching ones
		if codeHash != "" && codeHash != currCodeHash {
			continue
		}
		if genesis != "" && genesis != currGenesis {
			continue
		}

		// Check if already spent
		key := currTxID + ":" + currIndex
		if _, exists := spendMap[key]; exists {
			continue
		}

		// Check for duplicates
		if _, exists := uniqueUtxoMap[key]; exists {
			continue
		}
		uniqueUtxoMap[key] = struct{}{}
		confirmedIncomeOutpointList = append(confirmedIncomeOutpointList, key)

		// Get or create balance record
		balanceKey := currCodeHash + "@" + currGenesis
		balance, exists := balanceMap[balanceKey]
		if !exists {
			// Get FT info
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

		// Update balance
		amount, err := strconv.ParseInt(currAmount, 10, 64)
		if err != nil {
			continue
		}
		balance.Confirmed += amount
		balance.ConfirmedString = strconv.FormatInt(balance.Confirmed, 10)
		balance.UTXOCount++

		// Add to sorting map
		genesisUtxoMap[balanceKey] = append(genesisUtxoMap[balanceKey], key)
	}

	// Process income UTXOs in mempool
	for _, utxo := range mempoolIncomeList {
		// If codeHash and genesis are specified, only process matching ones
		if codeHash != "" && codeHash != utxo.CodeHash {
			continue
		}
		if genesis != "" && genesis != utxo.Genesis {
			continue
		}

		// Check if already spent
		key := utxo.TxID + ":" + utxo.Index
		// if _, exists := mempoolSpendMap[key]; exists {
		// 	continue
		// }

		// Check for duplicates
		if _, exists := uniqueUtxoMap[key]; exists {
			continue
		}
		uniqueUtxoMap[key] = struct{}{}
		unconfirmedIncomeOutpointList = append(unconfirmedIncomeOutpointList, key)

		// Get or create balance record
		balanceKey := utxo.CodeHash + "@" + utxo.Genesis
		balance, exists := balanceMap[balanceKey]
		if !exists {
			// Get FT info
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

		// Update unconfirmed income balance
		amount, err := strconv.ParseInt(utxo.Amount, 10, 64)
		if err != nil {
			continue
		}
		balance.UnconfirmedIncome += amount
		balance.UnconfirmedIncomeString = strconv.FormatInt(balance.UnconfirmedIncome, 10)
		balance.UTXOCount++

		// Add to sorting map
		genesisUtxoMap[balanceKey] = append(genesisUtxoMap[balanceKey], key)
	}

	// Process spent UTXOs in mempool
	for _, utxo := range mempoolSpendList {
		// If codeHash and genesis are specified, only process matching ones
		if codeHash != "" && codeHash != utxo.CodeHash {
			continue
		}
		if genesis != "" && genesis != utxo.Genesis {
			continue
		}

		spendOutpoint := utxo.TxID + ":" + utxo.Index
		// Get or create balance record
		balanceKey := utxo.CodeHash + "@" + utxo.Genesis
		balance, exists := balanceMap[balanceKey]
		if !exists {
			// Get FT info
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

		// Update unconfirmed spend balance
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

	// Sort the outpoint array for each balanceKey and keep only the first element
	for balanceKey, outpoints := range genesisUtxoMap {
		if len(outpoints) > 0 {
			sort.Strings(outpoints)
			genesisUtxoMap[balanceKey] = []string{outpoints[0]}
		}
	}

	// Get all balanceKeys and sort them
	balanceKeys := make([]string, 0, len(genesisUtxoMap))
	for balanceKey := range genesisUtxoMap {
		balanceKeys = append(balanceKeys, balanceKey)
	}

	// Sort based on the first outpoint corresponding to each balanceKey
	sort.Slice(balanceKeys, func(i, j int) bool {
		return genesisUtxoMap[balanceKeys[i]][0] < genesisUtxoMap[balanceKeys[j]][0]
	})

	// Calculate final balance and convert map to slice
	for _, balanceKey := range balanceKeys {
		balance := balanceMap[balanceKey]
		// Calculate total balance: confirmed + unconfirmed income - unconfirmed spend
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

	// Get spent FT UTXOs
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

	// Get UTXOs in mempool
	var mempoolIncomeList, mempoolSpendList []common.FtUtxo
	if i.mempoolMgr != nil {
		mempoolIncomeList, mempoolSpendList, err = i.mempoolMgr.GetFtUTXOsByAddress(address, codeHash, genesis)
		if err != nil {
			return nil, fmt.Errorf("Failed to get mempool UTXOs: %w", err)
		}
	}

	// Process spent UTXOs in mempool
	for _, utxo := range mempoolSpendList {
		key := utxo.TxID + ":" + utxo.Index
		spendMap[key] = struct{}{}
	}

	// Get FT income data
	// data, _, err := i.addressFtIncomeStore.GetWithShard(addrKey)
	data, _, err := i.addressFtIncomeValidStore.GetWithShard(addrKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	// Map for deduplication
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

		// Parse data
		currCodeHash := incomes[0]
		currGenesis := incomes[1]
		currAmount := incomes[2]
		currTxID := incomes[3]
		currIndex := incomes[4]
		currValue := incomes[5]
		currHeight := incomes[6]

		// If codeHash and genesis are specified, only process matching ones
		if codeHash != "" && codeHash != currCodeHash {
			continue
		}
		if genesis != "" && genesis != currGenesis {
			continue
		}

		// Check if already spent
		key := currTxID + ":" + currIndex
		if _, exists := spendMap[key]; exists {
			continue
		}

		// Check for duplicates
		if _, exists := uniqueUtxoMap[key]; exists {
			continue
		}

		// Get FT info
		ftInfo, err := i.GetFtInfo(currCodeHash + "@" + currGenesis)
		if err != nil {
			continue
		}

		// Parse amount
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
			Height:        height, // Confirmed UTXO
			Flag:          fmt.Sprintf("%s_%s", currTxID, currIndex),
		})
	}

	// Process income UTXOs in mempool
	for _, utxo := range mempoolIncomeList {
		// If codeHash and genesis are specified, only process matching ones
		if codeHash != "" && codeHash != utxo.CodeHash {
			continue
		}
		if genesis != "" && genesis != utxo.Genesis {
			continue
		}

		// Check if already spent
		key := utxo.TxID + ":" + utxo.Index
		if _, exists := spendMap[key]; exists {
			continue
		}

		// Check for duplicates
		if _, exists := uniqueUtxoMap[key]; exists {
			continue
		}

		// Get FT info
		ftInfo, err := i.GetFtInfo(utxo.CodeHash + "@" + utxo.Genesis)
		if err != nil {
			continue
		}

		// Parse amount
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
			Height:        -1, // UTXO in mempool
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
		return strings.Split(string(data), ","), nil // Return all data
	}

	// Split data by comma
	parts := strings.Split(string(data), ",")
	filteredParts := make([]string, 0)

	// Filter data
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

	// Recombine data
	return filteredParts, nil
}

func (i *ContractFtIndexer) GetDbAddressFtSpend(address string, codeHash string, genesis string) ([]string, error) {
	data, err := i.addressFtSpendStore.Get([]byte(address))
	if err != nil {
		return nil, err
	}

	if codeHash == "" && genesis == "" {
		return strings.Split(string(data), ","), nil // Return all data
	}

	// Split data by comma
	parts := strings.Split(string(data), ",")
	filteredParts := make([]string, 0)

	// Filter data
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

	// Recombine data
	return filteredParts, nil
}

// GetFtInfo gets FT information
func (i *ContractFtIndexer) GetFtInfo(key string) (*FtInfo, error) {
	// Get FT information from contractFtInfoStore
	data, err := i.contractFtInfoStore.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("Failed to get FT info: %w", err)
	}

	// fmt.Println("data", string(data))
	// Parse FT information
	parts := strings.Split(string(data), "@")
	if len(parts) < 4 {
		return nil, fmt.Errorf("FT info format error")
	}
	decimal, err := strconv.ParseUint(parts[3], 10, 8)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse decimal: %w", err)
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

// GetAddressFtBalance gets address FT balance
func (i *ContractFtIndexer) GetAddressFtBalance(address string) ([]*FtBalance, error) {
	return i.GetFtBalance(address, "", "")
}

// GetAddressFtUTXOs gets address FT UTXO list
func (i *ContractFtIndexer) GetAddressFtUTXOs(address string) ([]*FtUTXO, error) {
	return i.GetFtUTXOs(address, "", "")
}

// GetMempoolUTXOs queries UTXOs in mempool for an address
func (i *ContractFtIndexer) GetMempoolFtUTXOs(address string, codeHash string, genesis string) (mempoolIncomeList []common.FtUtxo, mempoolSpendList []common.FtUtxo, err error) {
	// Check if mempool manager is set
	if i.mempoolMgr == nil {
		return nil, nil, fmt.Errorf("Mempool manager not set")
	}

	// Use interface method directly
	mempoolIncomeList, mempoolSpendList, err = i.mempoolMgr.GetFtUTXOsByAddress(address, codeHash, genesis)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to get mempool UTXOs: %w", err)
	}
	return
}

// GetAllDbAddressFtIncome gets all address FT income data
func (i *ContractFtIndexer) GetAllDbAddressFtIncome() (map[string]string, error) {
	result := make(map[string]string)

	// Iterate through all shards
	for _, db := range i.addressFtIncomeStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Iterate through all key-value pairs
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = value
		}
	}

	return result, nil
}

// GetAllDbAddressFtSpend gets all address FT spend data
func (i *ContractFtIndexer) GetAllDbAddressFtSpend() (map[string]string, error) {
	result := make(map[string]string)

	// Iterate through all shards
	for _, db := range i.addressFtSpendStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Iterate through all key-value pairs
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = value
		}
	}

	return result, nil
}

// GetDbAddressFtIncomeValidByAddress gets valid FT income data for specified address
func (i *ContractFtIndexer) GetDbAddressFtIncomeValid(address string, codeHash string, genesis string) ([]string, error) {
	data, err := i.addressFtIncomeValidStore.Get([]byte(address))
	if err != nil {
		return nil, err
	}

	if codeHash == "" && genesis == "" {
		return strings.Split(string(data), ","), nil // Return all data
	}

	// Split data by comma
	parts := strings.Split(string(data), ",")
	filteredParts := make([]string, 0)

	// Filter data
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

	// Return filtered data
	return filteredParts, nil
}

// GetAllDbUncheckFtOutpoint gets unchecked FT outpoint data
// If the outpoint parameter is provided, only the corresponding value is returned
// If the outpoint parameter is not provided, all data is returned
func (i *ContractFtIndexer) GetAllDbUncheckFtOutpoint(outpoint string) (map[string]string, error) {
	result := make(map[string]string)

	// If outpoint is provided, get the corresponding value directly
	if outpoint != "" {
		value, err := i.uncheckFtOutpointStore.Get([]byte(outpoint))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("Failed to get outpoint data: %w", err)
		}
		result[outpoint] = string(value)
		return result, nil
	}

	// Iterate through all shards
	for _, db := range i.uncheckFtOutpointStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Iterate through all key-value pairs
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = value
		}
	}

	return result, nil
}

// GetAllDbFtGenesis gets all FT Genesis data
func (i *ContractFtIndexer) GetAllDbFtGenesis(key string) (map[string]string, error) {
	result := make(map[string]string)

	// If key is provided, get the corresponding value directly
	if key != "" {
		value, err := i.contractFtGenesisStore.Get([]byte(key))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("Failed to get FT Genesis data: %w", err)
		}
		result[key] = string(value)
		return result, nil
	}

	// Iterate through all shards
	for _, db := range i.contractFtGenesisStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Iterate through all key-value pairs
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = value
		}
	}

	return result, nil
}

// GetAllDbFtGenesisOutput gets all FT Genesis Output data
func (i *ContractFtIndexer) GetAllDbFtGenesisOutput(key string) (map[string][]string, error) {
	result := make(map[string][]string)

	// If key is provided, get the corresponding value directly
	if key != "" {
		value, err := i.contractFtGenesisOutputStore.Get([]byte(key))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("Failed to get FT Genesis Output data: %w", err)
		}
		result[key] = strings.Split(string(value), ",")
		return result, nil
	}

	// Iterate through all shards
	for _, db := range i.contractFtGenesisOutputStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Iterate through all key-value pairs
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = strings.Split(value, ",")
		}
	}

	return result, nil
}

// GetAllDbUsedFtIncome gets all used FT income data
func (i *ContractFtIndexer) GetAllDbUsedFtIncome(key string) (map[string]string, error) {
	result := make(map[string]string)

	// If key is provided, get the corresponding value directly
	if key != "" {
		value, err := i.usedFtIncomeStore.Get([]byte(key))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("Failed to get used FT income data: %w", err)
		}
		result[key] = string(value)
		return result, nil
	}

	// Iterate through all shards
	for _, db := range i.usedFtIncomeStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Iterate through all key-value pairs
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = value
		}
	}

	return result, nil
}

// GetAllDbFtGenesisUtxo gets all FT Genesis UTXO data
func (i *ContractFtIndexer) GetAllDbFtGenesisUtxo(key string) (map[string]string, error) {
	result := make(map[string]string)

	// If key is provided, get the corresponding value directly
	if key != "" {
		value, err := i.contractFtGenesisUtxoStore.Get([]byte(key))
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return result, nil
			}
			return nil, fmt.Errorf("Failed to get FT Genesis UTXO data: %w", err)
		}
		result[key] = string(value)
		return result, nil
	}

	// Iterate through all shards
	for _, db := range i.contractFtGenesisUtxoStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Iterate through all key-value pairs
		for iter.First(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())
			result[key] = value
		}
	}

	return result, nil
}

// GetUncheckFtOutpointTotal gets the total count of unchecked FT outpoints
func (i *ContractFtIndexer) GetUncheckFtOutpointTotal() (int64, error) {
	var total int64 = 0

	// Iterate through all shards
	for _, db := range i.uncheckFtOutpointStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return 0, fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Iterate through all key-value pairs and count
		for iter.First(); iter.Valid(); iter.Next() {
			total++
		}
	}

	return total, nil
}

// GetUniqueFtUTXOs gets unique FT UTXO list
func (i *ContractFtIndexer) GetUniqueFtUTXOs(codeHash, genesis string) (utxos []*UniqueFtUtxo, err error) {
	// Use map to store unique UTXOs
	uniqueUtxos := make(map[string]*UniqueFtUtxo)

	// Iterate through all shards
	for _, db := range i.addressFtIncomeStore.GetShards() {
		iter, err := db.NewIter(&pebble.IterOptions{})
		if err != nil {
			return nil, fmt.Errorf("Failed to create iterator: %w", err)
		}
		defer iter.Close()

		// Iterate through all key-value pairs
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

				// Parse data
				currCodeHash := incomes[0]
				currGenesis := incomes[1]
				currTxID := incomes[3]
				currIndex := incomes[4]
				currValue := incomes[5]
				currHeight := incomes[6]
				currCustomData := incomes[7]

				// If codeHash and genesis are specified, only process matching ones
				if codeHash != "" && codeHash != currCodeHash {
					continue
				}
				if genesis != "" && genesis != currGenesis {
					continue
				}

				// Create unique key
				uniqueKey := currCodeHash + "@" + currGenesis + "@" + currTxID + "@" + currIndex

				// If already exists, skip
				if _, exists := uniqueUtxos[uniqueKey]; exists {
					continue
				}

				// Get FT info
				ftInfo, err := i.GetFtInfo(currCodeHash + "@" + currGenesis)
				if err != nil {
					continue
				}

				// Parse height
				height, err := strconv.ParseInt(currHeight, 10, 64)
				if err != nil {
					continue
				}

				// Parse index
				index, err := strconv.ParseInt(currIndex, 10, 64)
				if err != nil {
					continue
				}

				// Add to unique UTXO list
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

	// Convert map to slice
	for _, utxo := range uniqueUtxos {
		utxos = append(utxos, utxo)
	}

	return utxos, nil
}

// GetMempoolAddressFtSpendMap gets FT spend data for address in mempool
func (i *ContractFtIndexer) GetMempoolAddressFtSpendMap(address string) (map[string]string, error) {
	if i.mempoolMgr == nil {
		return nil, fmt.Errorf("Mempool manager not set")
	}
	return i.mempoolMgr.GetMempoolAddressFtSpendMap(address)
}

// GetMempoolUniqueFtSpendMap gets unique FT spend data in mempool
func (i *ContractFtIndexer) GetMempoolUniqueFtSpendMap(codeHashGenesis string) (map[string]string, error) {
	if i.mempoolMgr == nil {
		return nil, fmt.Errorf("Mempool manager not set")
	}
	return i.mempoolMgr.GetMempoolUniqueFtSpendMap(codeHashGenesis)
}
