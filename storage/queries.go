package storage

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/metaid/utxo_indexer/common"
)

// SimpleDB is a simple single database connection, not using sharding
type SimpleDB struct {
	db   *pebble.DB
	path string
}

// NewSimpleDB creates a new single database connection
func NewSimpleDB(dbPath string) (*SimpleDB, error) {
	db, err := pebble.Open(dbPath, &pebble.Options{Logger: noopLogger})
	if err != nil {
		return nil, fmt.Errorf("Failed to open database: %w", err)
	}
	return &SimpleDB{db: db, path: dbPath}, nil
}

// Close closes the database connection
func (s *SimpleDB) Close() error {
	return s.db.Close()
}

func (s *SimpleDB) Get(key string) (result string, err error) {
	value, _, err := s.db.Get([]byte(key))
	if err != nil {
		return
	}
	result = string(value)
	return
}

// GetByUTXO queries addresses associated with UTXO ID
// Example: For key "tx1:0_addr1", query by "tx1:0"
func (s *SimpleDB) GetByUTXO(utxoID string) (address string, amount string, err error) {
	// Use prefix query to find all matching keys
	prefix := []byte(utxoID + "_")
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	// Iterate to find all matching keys
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if key starts with prefix
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // Already beyond prefix range
		}

		// Extract address part (part after _)
		parts := strings.Split(string(key), "_")
		if len(parts) == 2 {
			address = parts[1]
		}
		// Get value corresponding to key
		amount = string(iter.Value())
		break
	}
	return
}

// GetByAddress queries all related UTXOs by address
// Example: For key "addr1_tx1:0", query by "addr1"
func (s *SimpleDB) GetUtxoByKey(key string) (utxoList []common.Utxo, err error) {
	// Use prefix query to find all matching keys
	prefix := []byte(key + "_")
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	// Iterate to find all matching keys
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if key starts with prefix
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // Already beyond prefix range
		}
		utxo := common.Utxo{}
		// Extract address part (part after _)
		parts := strings.Split(string(key), "_")
		if len(parts) == 2 {
			utxo.Address = parts[0]
			utxo.TxID = parts[1]
		}
		// Get value corresponding to key
		utxo.Amount = string(iter.Value())
		utxoList = append(utxoList, utxo)
	}
	return
}

// GetByUTXO queries related addresses through UTXO ID
// Example: For key "tx1:0_addr1", query through "tx1:0"
// value:CodeHash@Genesis@sensibleId@Amount@Index@Value
func (s *SimpleDB) GetByFtUTXO(utxoID string) (address string, codeHash string, genesis string, sensibleId string, amount string, value string, index string, err error) {
	// Use prefix query to find all matching keys
	prefix := []byte(utxoID + "_")
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	// Iterate to find all matching keys
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if key starts with prefix
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // Already beyond prefix range
		}

		// Extract address part (part after _)
		keyParts := strings.Split(string(key), "_")
		if len(keyParts) == 2 {
			address = keyParts[1]
		}
		// Get value corresponding to key
		//CodeHash@Genesis@sensibleId@Amount@Index@Value
		valueData := string(iter.Value())
		valueParts := strings.Split(valueData, "@")
		if len(valueParts) == 6 {
			codeHash = valueParts[0]
			genesis = valueParts[1]
			sensibleId = valueParts[2]
			amount = valueParts[3]
			index = valueParts[4]
			value = valueParts[5]
		}
		break
	}
	return
}

// GetByUTXO queries related addresses through UTXO ID
// Example: For key "tx1:0_addr1", query through "tx1:0"
// value:CodeHash@Genesis@sensibleId@customData@Index@Value
func (s *SimpleDB) GetByUniqueFtUTXO(utxoID string) (codehashGenesis string, codeHash string, genesis string, sensibleId string, customData string, value string, index string, err error) {
	// Use prefix query to find all matching keys
	prefix := []byte(utxoID + "_")
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	// Iterate to find all matching keys
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if key starts with prefix
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // Already beyond prefix range
		}

		// Extract address part (part after _)
		keyParts := strings.Split(string(key), "_")
		if len(keyParts) == 2 {
			codehashGenesis = keyParts[1]
		}
		// Get value corresponding to key
		valueData := string(iter.Value())
		valueParts := strings.Split(valueData, "@")
		if len(valueParts) == 6 {
			codeHash = valueParts[0]
			genesis = valueParts[1]
			sensibleId = valueParts[2]
			customData = valueParts[3]
			index = valueParts[4]
			value = valueParts[5]
		}
		break
	}
	return
}

// GetByAddress queries all related UTXOs through address
// Example: For key "addr1_tx1:0", query through "addr1"
// value:CodeHash@Genesis@sensibleId@Amount@Index@Value
func (s *SimpleDB) GetFtUtxoByKey(key string) (ftUtxoList []common.FtUtxo, err error) {
	// Use prefix query to find all matching keys
	prefix := []byte(key + "_")
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	// Iterate to find all matching keys
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if key starts with prefix
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // Already beyond prefix range
		}
		utxo := common.FtUtxo{}
		// Extract address part (part after _)
		keyParts := strings.Split(string(key), "_")
		if len(keyParts) == 2 {
			utxo.Address = keyParts[0]
			utxo.UtxoId = keyParts[1]

			utxoIdStrs := strings.Split(utxo.UtxoId, ":")
			if len(utxoIdStrs) == 2 {
				utxo.TxID = utxoIdStrs[0]
				utxo.Index = utxoIdStrs[1]
			}

		}
		// Get value corresponding to key
		valueData := string(iter.Value())
		valueParts := strings.Split(valueData, "@")
		if len(valueParts) == 6 {
			utxo.CodeHash = valueParts[0]
			utxo.Genesis = valueParts[1]
			utxo.SensibleId = valueParts[2]
			utxo.Amount = valueParts[3]
			utxo.Index = valueParts[4]
			utxo.Value = valueParts[5]
		}
		ftUtxoList = append(ftUtxoList, utxo)
	}
	return
}

func (s *SimpleDB) GetFtUtxoByOutpoint(outpoint string) (ftUtxoList []common.FtUtxo, err error) {
	// Use prefix query to find all matching keys
	prefix := []byte(outpoint + "_")
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	// Iterate to find all matching keys
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if key starts with prefix
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // Already beyond prefix range
		}
		utxo := common.FtUtxo{}
		// Extract address part (part after _)
		keyParts := strings.Split(string(key), "_")
		if len(keyParts) == 2 {
			utxo.Address = keyParts[1]
			utxo.UtxoId = keyParts[0]

			utxoIdStrs := strings.Split(utxo.UtxoId, ":")
			if len(utxoIdStrs) == 2 {
				utxo.TxID = utxoIdStrs[0]
				utxo.Index = utxoIdStrs[1]
			}

		}
		// Get value corresponding to key
		valueData := string(iter.Value())
		valueParts := strings.Split(valueData, "@")
		if len(valueParts) == 6 {
			utxo.CodeHash = valueParts[0]
			utxo.Genesis = valueParts[1]
			utxo.SensibleId = valueParts[2]
			utxo.Amount = valueParts[3]
			utxo.Index = valueParts[4]
			utxo.Value = valueParts[5]
		}
		ftUtxoList = append(ftUtxoList, utxo)
	}
	return
}

// GetFtGenesisByKey queries all related genesis information through outpoint
// Example: For key "outpoint_", query through "outpoint"
// key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
func (s *SimpleDB) GetFtGenesisByKey(key string) ([]byte, error) {
	// Use prefix query to find all matching keys
	prefix := []byte(key + "_")
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// Iterate to find all matching keys
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if key starts with prefix
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // Already beyond prefix range
		}

		// Return first matching value
		return iter.Value(), nil
	}

	return nil, nil
}

// GetFtGenesisByKey queries all related genesis information through outpoint
// Example: For key "outpoint_", query through "outpoint"
// key: usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
func (s *SimpleDB) GetFtGenesisOutputsByKey(key string) ([]byte, error) {
	// Use prefix query to find all matching keys
	prefix := []byte(key + "_")
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// Iterate to find all matching keys
	for iter.First(); iter.Valid(); iter.Next() {
		// Check if key starts with prefix
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // Already beyond prefix range
		}

		// Return first matching value
		return iter.Value(), nil
	}

	return nil, nil
}

func (s *SimpleDB) AddMempolRecord(key string, value []byte) error {
	return s.db.Set([]byte(key), value, pebble.Sync)
}

func (s *SimpleDB) DeleteMempolRecord(key string) error {
	return s.db.Delete([]byte(key), pebble.Sync)
}

func (s *SimpleDB) BatchDeleteMempolRecord(keys []string) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, key := range keys {
		if err := batch.Delete([]byte(key), pebble.Sync); err != nil {
			continue
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("Failed to commit batch: %w", err)
	}
	return nil
}

func (s *SimpleDB) BatchGetMempolRecord(keys []string) (list []string, err error) {
	for _, key := range keys {
		_, closer, err := s.db.Get([]byte(key))
		if err == nil {
			list = append(list, key)
		}
		if closer != nil {
			closer.Close()
		}
	}
	return
}

// SetWithIndex
func (s *SimpleDB) AddRecord(utxoID string, address string, value []byte) error {
	// Create batch
	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. Primary keys
	mainKey1 := []byte(utxoID + "_" + address)
	mainKey2 := []byte(address + "_" + utxoID)
	//fmt.Println("mainKey1:", string(mainKey1), "mainKey2:", string(mainKey2))
	// Add set operations to batch
	if err := batch.Set(mainKey1, value, pebble.Sync); err != nil {
		return fmt.Errorf("Failed to add key1 in batch: %w", err)
	}
	if err := batch.Set(mainKey2, value, pebble.Sync); err != nil {
		return fmt.Errorf("Failed to add key2 in batch: %w", err)
	}

	// Commit batch
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("Failed to commit batch: %w", err)
	}

	return nil
}

// SetWithIndex
func (s *SimpleDB) AddSimpleRecord(key string, value []byte) error {
	// Create batch
	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. Primary key
	mainKey := []byte(key)
	// Add set operation to batch
	if err := batch.Set(mainKey, value, pebble.Sync); err != nil {
		return fmt.Errorf("Failed to set key1 in batch: %w", err)
	}

	// Commit batch
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("Failed to commit batch: %w", err)
	}

	return nil
}

func (s *SimpleDB) GetSimpleRecord(key string) ([]byte, error) {
	value, _, err := s.db.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *SimpleDB) DeleteSimpleRecord(key string) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.Delete([]byte(key), pebble.Sync); err != nil {
		return fmt.Errorf("Failed to delete key in batch: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("Failed to commit batch: %w", err)
	}
	return nil
}

// DeleteWithIndex deletes Spend records
func (s *SimpleDB) DeleteRecord(utxoID string, address string) error {
	// Create batch
	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. Delete primary keys
	mainKey1 := []byte(utxoID + "_" + address)
	mainKey2 := []byte(address + "_" + utxoID)

	// Add delete operations to batch
	if err := batch.Delete(mainKey1, pebble.Sync); err != nil {
		return fmt.Errorf("Failed to delete key1 in batch: %w", err)
	}
	if err := batch.Delete(mainKey2, pebble.Sync); err != nil {
		return fmt.Errorf("Failed to delete key2 in batch: %w", err)
	}

	// Commit batch
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("Failed to commit batch: %w", err)
	}

	return nil
}

func (s *SimpleDB) DeleteSpendRecord(utxoID string) error {
	utxoList, err := s.GetUtxoByKey(utxoID)
	if err != nil {
		return fmt.Errorf("Query failed: %w", err)
	}
	for _, utxo := range utxoList {
		s.DeleteRecord(utxo.TxID, utxo.Address)
	}
	return nil
}

func (s *SimpleDB) DeleteFtSpendRecord(utxoID string) error {
	utxoList, err := s.GetFtUtxoByOutpoint(utxoID)
	if err != nil {
		return fmt.Errorf("Query failed: %w", err)
	}
	for _, utxo := range utxoList {
		s.DeleteRecord(utxoID, utxo.Address)
	}
	return nil
}

func (s *SimpleDB) DeleteUniqueSpendRecord(utxoID string) error {
	utxoList, err := s.GetFtUtxoByOutpoint(utxoID)
	if err != nil {
		return fmt.Errorf("Query failed: %w", err)
	}
	for _, utxo := range utxoList {
		s.DeleteRecord(utxoID, utxo.Address)
	}
	return nil
}

// // UpdateRecord updates record, if record exists then update, otherwise set
// func (s *SimpleDB) UpdateRecord(utxoID string, address string, value []byte) error {
// 	// Create batch
// 	batch := s.db.NewBatch()
// 	defer batch.Close()

// 	// 1. Primary key
// 	// 1. Delete primary key
// 	mainKey1 := []byte(utxoID + "_" + address)
// 	mainKey2 := []byte(address + "_" + utxoID)

// 	// Check if record exists
// 	_, closer, err := s.db.Get(mainKey1)
// 	if err == nil {
// 		// Record exists, perform update
// 		closer.Close()
// 		if err := batch.Set(mainKey1, value, pebble.NoSync); err != nil {
// 			return fmt.Errorf("Failed to update key1 in batch: %w", err)
// 		}
// 		if err := batch.Set(mainKey2, value, pebble.NoSync); err != nil {
// 			return fmt.Errorf("Failed to update key2 in batch: %w", err)
// 		}
// 	} else if err == pebble.ErrNotFound {
// 		// Record doesn't exist, perform set
// 		if err := batch.Set(mainKey1, value, pebble.NoSync); err != nil {
// 			return fmt.Errorf("Failed to set key1 in batch: %w", err)
// 		}
// 		if err := batch.Set(mainKey2, value, pebble.NoSync); err != nil {
// 			return fmt.Errorf("Failed to set key2 in batch: %w", err)
// 		}
// 	} else {
// 		// Other errors
// 		return fmt.Errorf("Failed to check record existence: %w", err)
// 	}

// 	// Commit batch
// 	if err := batch.Commit(pebble.Sync); err != nil {
// 		return fmt.Errorf("Failed to commit batch: %w", err)
// 	}

// 	return nil
// }

// GetFtUtxo gets all FT UTXO records
func (s *SimpleDB) GetFtUtxo() (ftUtxoList []common.FtUtxo, err error) {
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return
	}
	defer iter.Close()

	// Iterate through all records
	for iter.First(); iter.Valid(); iter.Next() {
		utxo := common.FtUtxo{}
		// Extract address part (part after _)
		keyParts := strings.Split(string(iter.Key()), ":")
		if len(keyParts) == 2 {
			utxo.TxID = keyParts[0]
			utxo.Index = keyParts[1]
			utxo.UtxoId = string(iter.Key())
		}

		// Get value corresponding to key
		//ftAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value
		valueData := string(iter.Value())
		valueParts := strings.Split(valueData, "@")
		if len(valueParts) == 7 {
			utxo.Address = valueParts[0]
			utxo.CodeHash = valueParts[1]
			utxo.Genesis = valueParts[2]
			utxo.SensibleId = valueParts[3]
			utxo.Amount = valueParts[4]
			utxo.Index = valueParts[5]
			utxo.Value = valueParts[6]
		}
		ftUtxoList = append(ftUtxoList, utxo)
	}
	return
}

// GetAll gets all records
func (s *SimpleDB) GetAll() ([]string, error) {
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var values []string
	// Iterate through all records
	for iter.First(); iter.Valid(); iter.Next() {
		values = append(values, string(iter.Value()))
	}
	return values, nil
}

func (s *SimpleDB) GetAllKeyValues() (map[string]string, error) {
	// Create iterator
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	keyValues := make(map[string]string)
	// Iterate through all records
	for iter.First(); iter.Valid(); iter.Next() {
		keyValues[string(iter.Key())] = string(iter.Value())
	}
	return keyValues, nil
}
