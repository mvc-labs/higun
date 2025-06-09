package storage

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/v2"
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

// GetByUTXO 通过UTXO ID查询相关联的地址
// 例如: 对于键 "tx1:0_addr1", 通过 "tx1:0" 查询
// value:CodeHash@Genesis@sensibleId@Amount@Index@Value
func (s *SimpleDB) GetByFtUTXO(utxoID string) (address string, codeHash string, genesis string, sensibleId string, amount string, value string, index string, err error) {
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
		keyParts := strings.Split(string(key), "_")
		if len(keyParts) == 2 {
			address = keyParts[1]
		}
		// 获取键对应的值
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

// GetByUTXO 通过UTXO ID查询相关联的地址
// 例如: 对于键 "tx1:0_addr1", 通过 "tx1:0" 查询
// value:CodeHash@Genesis@sensibleId@customData@Index@Value
func (s *SimpleDB) GetByUniqueFtUTXO(utxoID string) (codehashGenesis string, codeHash string, genesis string, sensibleId string, customData string, value string, index string, err error) {
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
		keyParts := strings.Split(string(key), "_")
		if len(keyParts) == 2 {
			codehashGenesis = keyParts[1]
		}
		// 获取键对应的值
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

// GetByAddress 通过地址查询所有相关的UTXO
// 例如: 对于键 "addr1_tx1:0", 通过 "addr1" 查询
// value:CodeHash@Genesis@sensibleId@Amount@Index@Value
func (s *SimpleDB) GetFtUtxoByKey(key string) (ftUtxoList []common.FtUtxo, err error) {
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
		utxo := common.FtUtxo{}
		// 提取地址部分(在_之后的部分)
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
		// 获取键对应的值
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
	// 使用前缀查询找到所有匹配的键
	prefix := []byte(outpoint + "_")
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
		utxo := common.FtUtxo{}
		// 提取地址部分(在_之后的部分)
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
		// 获取键对应的值
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

// GetFtGenesisByKey 通过outpoint查询所有相关的创世信息
// 例如: 对于键 "outpoint_", 通过 "outpoint" 查询
// key:outpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis
func (s *SimpleDB) GetFtGenesisByKey(key string) ([]byte, error) {
	// 使用前缀查询找到所有匹配的键
	prefix := []byte(key + "_")
	// 创建迭代器
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// 遍历找到所有匹配的键
	for iter.First(); iter.Valid(); iter.Next() {
		// 检查键是否以前缀开头
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // 已经超出前缀范围
		}

		// 返回第一个匹配的值
		return iter.Value(), nil
	}

	return nil, nil
}

// GetFtGenesisByKey 通过outpoint查询所有相关的创世信息
// 例如: 对于键 "outpoint_", 通过 "outpoint" 查询
// key: usedOutpoint, value: sensibleId@name@symbol@decimal@codeHash@genesis@amount@txId@index@value,...
func (s *SimpleDB) GetFtGenesisOutputsByKey(key string) ([]byte, error) {
	// 使用前缀查询找到所有匹配的键
	prefix := []byte(key + "_")
	// 创建迭代器
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// 遍历找到所有匹配的键
	for iter.First(); iter.Valid(); iter.Next() {
		// 检查键是否以前缀开头
		key := iter.Key()
		if !strings.HasPrefix(string(key), string(prefix)) {
			break // 已经超出前缀范围
		}

		// 返回第一个匹配的值
		return iter.Value(), nil
	}

	return nil, nil
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
	if err := batch.Set(mainKey1, value, pebble.NoSync); err != nil {
		return fmt.Errorf("批处理增加键1失败: %w", err)
	}
	if err := batch.Set(mainKey2, value, pebble.NoSync); err != nil {
		return fmt.Errorf("批处理增加键2失败: %w", err)
	}

	// 提交批处理
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("提交批处理失败: %w", err)
	}

	return nil
}

// SetWithIndex
func (s *SimpleDB) AddSimpleRecord(key string, value []byte) error {
	// 创建批处理
	batch := s.db.NewBatch()
	defer batch.Close()

	// 1. 主键
	mainKey := []byte(key)
	// 在批处理中添加增加操作
	if err := batch.Set(mainKey, value, pebble.NoSync); err != nil {
		return fmt.Errorf("批处理增加键1失败: %w", err)
	}

	// 提交批处理
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("提交批处理失败: %w", err)
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

	if err := batch.Delete([]byte(key), pebble.NoSync); err != nil {
		return fmt.Errorf("批处理删除键失败: %w", err)
	}
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
	if err := batch.Delete(mainKey1, pebble.NoSync); err != nil {
		return fmt.Errorf("批处理删除键1失败: %w", err)
	}
	if err := batch.Delete(mainKey2, pebble.NoSync); err != nil {
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

func (s *SimpleDB) DeleteFtSpendRecord(utxoID string) error {
	utxoList, err := s.GetFtUtxoByOutpoint(utxoID)
	if err != nil {
		return fmt.Errorf("查询失败: %w", err)
	}
	for _, utxo := range utxoList {
		s.DeleteRecord(utxoID, utxo.Address)
	}
	return nil
}

func (s *SimpleDB) DeleteUniqueSpendRecord(utxoID string) error {
	utxoList, err := s.GetFtUtxoByOutpoint(utxoID)
	if err != nil {
		return fmt.Errorf("查询失败: %w", err)
	}
	for _, utxo := range utxoList {
		s.DeleteRecord(utxoID, utxo.Address)
	}
	return nil
}

// // UpdateRecord 更新记录，如果记录存在则更新，不存在则设置
// func (s *SimpleDB) UpdateRecord(utxoID string, address string, value []byte) error {
// 	// 创建批处理
// 	batch := s.db.NewBatch()
// 	defer batch.Close()

// 	// 1. 主键
// 	// 1. 删除主键
// 	mainKey1 := []byte(utxoID + "_" + address)
// 	mainKey2 := []byte(address + "_" + utxoID)

// 	// 检查记录是否存在
// 	_, closer, err := s.db.Get(mainKey1)
// 	if err == nil {
// 		// 记录存在，执行更新
// 		closer.Close()
// 		if err := batch.Set(mainKey1, value, pebble.NoSync); err != nil {
// 			return fmt.Errorf("批处理更新键1失败: %w", err)
// 		}
// 		if err := batch.Set(mainKey2, value, pebble.NoSync); err != nil {
// 			return fmt.Errorf("批处理更新键2失败: %w", err)
// 		}
// 	} else if err == pebble.ErrNotFound {
// 		// 记录不存在，执行设置
// 		if err := batch.Set(mainKey1, value, pebble.NoSync); err != nil {
// 			return fmt.Errorf("批处理设置键1失败: %w", err)
// 		}
// 		if err := batch.Set(mainKey2, value, pebble.NoSync); err != nil {
// 			return fmt.Errorf("批处理设置键2失败: %w", err)
// 		}
// 	} else {
// 		// 其他错误
// 		return fmt.Errorf("检查记录存在性失败: %w", err)
// 	}

// 	// 提交批处理
// 	if err := batch.Commit(pebble.Sync); err != nil {
// 		return fmt.Errorf("提交批处理失败: %w", err)
// 	}

// 	return nil
// }

// GetFtUtxo 获取所有FT UTXO记录
func (s *SimpleDB) GetFtUtxo() (ftUtxoList []common.FtUtxo, err error) {
	// 创建迭代器
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return
	}
	defer iter.Close()

	// 遍历所有记录
	for iter.First(); iter.Valid(); iter.Next() {
		utxo := common.FtUtxo{}
		// 提取地址部分(在_之后的部分)
		keyParts := strings.Split(string(iter.Key()), ":")
		if len(keyParts) == 2 {
			utxo.TxID = keyParts[0]
			utxo.Index = keyParts[1]
			utxo.UtxoId = string(iter.Key())
		}

		// 获取键对应的值
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

// GetAll 获取所有记录
func (s *SimpleDB) GetAll() ([]string, error) {
	// 创建迭代器
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var values []string
	// 遍历所有记录
	for iter.First(); iter.Valid(); iter.Next() {
		values = append(values, string(iter.Value()))
	}
	return values, nil
}

func (s *SimpleDB) GetAllKeyValues() (map[string]string, error) {
	// 创建迭代器
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	keyValues := make(map[string]string)
	// 遍历所有记录
	for iter.First(); iter.Valid(); iter.Next() {
		keyValues[string(iter.Key())] = string(iter.Value())
	}
	return keyValues, nil
}
