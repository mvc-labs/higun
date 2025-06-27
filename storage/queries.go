package storage

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/metaid/utxo_indexer/common"
)

// SimpleDB 是一个简单的单一数据库连接，不使用分片
type SimpleDB struct {
	db   *pebble.DB
	path string
}

// NewSimpleDB 创建一个新的单一数据库连接
func NewSimpleDB(dbPath string) (*SimpleDB, error) {
	db, err := pebble.Open(dbPath, &pebble.Options{Logger: noopLogger})
	if err != nil {
		return nil, fmt.Errorf("无法打开数据库: %w", err)
	}
	return &SimpleDB{db: db, path: dbPath}, nil
}

// Close 关闭数据库连接
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

// GetByUTXO 通过UTXO ID查询相关联的地址
// 例如: 对于键 "tx1:0_addr1", 通过 "tx1:0" 查询
func (s *SimpleDB) GetByUTXO(utxoID string) (address string, amount string, err error) {
	// 使用前缀查询找到所有匹配的键
	prefix := []byte(utxoID + "_")
	// 创建迭代器
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	// 遍历找到所有匹配的键
	for iter.First(); iter.Valid(); iter.Next() {
		// 检查键是否以前缀开头
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // 已经超出前缀范围
		}

		// 提取地址部分(在_之后的部分)
		parts := strings.Split(string(key), "_")
		if len(parts) == 2 {
			address = parts[1]
		}
		// 获取键对应的值
		amount = string(iter.Value())
		break
	}
	return
}

// GetByAddress 通过地址查询所有相关的UTXO
// 例如: 对于键 "addr1_tx1:0", 通过 "addr1" 查询
func (s *SimpleDB) GetUtxoByKey(key string) (utxoList []common.Utxo, err error) {
	// 使用前缀查询找到所有匹配的键
	prefix := []byte(key + "_")
	// 创建迭代器
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	// 遍历找到所有匹配的键
	for iter.First(); iter.Valid(); iter.Next() {
		// 检查键是否以前缀开头
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // 已经超出前缀范围
		}
		utxo := common.Utxo{}
		// 提取地址部分(在_之后的部分)
		parts := strings.Split(string(key), "_")
		if len(parts) == 2 {
			utxo.Address = parts[0]
			utxo.TxID = parts[1]
		}
		// 获取键对应的值
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
		return fmt.Errorf("提交批处理失败: %w", err)
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
	// 创建批处理
	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. 主键
	mainKey1 := []byte(utxoID + "_" + address)
	mainKey2 := []byte(address + "_" + utxoID)
	//fmt.Println("mainKey1:", string(mainKey1), "mainKey2:", string(mainKey2))
	// 在批处理中添加增加操作
	if err := batch.Set(mainKey1, value, pebble.Sync); err != nil {
		return fmt.Errorf("批处理增加键1失败: %w", err)
	}
	if err := batch.Set(mainKey2, value, pebble.Sync); err != nil {
		return fmt.Errorf("批处理增加键2失败: %w", err)
	}

	// 提交批处理
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("提交批处理失败: %w", err)
	}

	return nil
}

// DeleteWithIndex 删除Spend记录
func (s *SimpleDB) DeleteRecord(utxoID string, address string) error {
	// 创建批处理
	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. 删除主键
	mainKey1 := []byte(utxoID + "_" + address)
	mainKey2 := []byte(address + "_" + utxoID)

	// 在批处理中添加删除操作
	if err := batch.Delete(mainKey1, pebble.Sync); err != nil {
		return fmt.Errorf("批处理删除键1失败: %w", err)
	}
	if err := batch.Delete(mainKey2, pebble.Sync); err != nil {
		return fmt.Errorf("批处理删除键2失败: %w", err)
	}

	// 提交批处理
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("提交批处理失败: %w", err)
	}

	return nil
}
func (s *SimpleDB) DeleteSpendRecord(utxoID string) error {
	utxoList, err := s.GetUtxoByKey(utxoID)
	if err != nil {
		return fmt.Errorf("查询失败: %w", err)
	}
	for _, utxo := range utxoList {
		s.DeleteRecord(utxo.TxID, utxo.Address)
	}
	return nil
}
