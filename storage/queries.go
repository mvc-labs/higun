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
