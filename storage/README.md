# 存储索引查询功能

本文档介绍了如何使用存储包中的索引查询功能，特别是针对具有复合键的情况。

## 复合键查询

在本系统中，我们使用了复合键结构 `txid:index_address`，例如 `tx1:0_addr1`。为了能够从不同角度查询数据，我们实现了以下功能：

### 1. 通过 UTXO ID 查询

通过 `txid:index` 部分查询与该 UTXO 关联的所有地址和值：

```go
// 示例: 查询 tx1:0 关联的地址
addresses, err := store.GetByUTXO("tx1:0")
if err != nil {
    log.Fatalf("查询失败: %v", err)
}
fmt.Println("关联的地址:", addresses)
```

### 2. 通过地址查询

通过 `address` 部分查询与该地址关联的所有 UTXO：

```go
// 示例: 查询 addr1 关联的所有 UTXO
utxos, err := store.GetByAddress("addr1")
if err != nil {
    log.Fatalf("查询失败: %v", err)
}
fmt.Println("关联的UTXO列表:", utxos)
```

## 数据操作函数

### 1. 添加带索引的记录

```go
// 添加单条记录并同时更新索引
err := store.SetWithIndex("tx1:0", "addr1", []byte("100"))
```

### 2. 批量添加带索引的记录

```go
// 批量添加记录
batchRecords := map[string]map[string][]byte{
    "tx4:0": {"addr3": []byte("500")},
    "tx4:1": {"addr3": []byte("600")},
    "tx5:0": {"addr1": []byte("700")},
}
err := store.BatchSetWithIndex(batchRecords)
```

### 3. 删除记录并更新索引

```go
// 删除记录并同时更新索引
err := store.DeleteWithIndex("tx1:0", "addr1")
```

## 索引实现原理

系统通过维护两种数据结构实现双向查询：

1. **主键**: `txid:index_address -> value`
   - 例如: `tx1:0_addr1 -> 100`

2. **反向索引**: `addr_index:address -> txid:index1,txid:index2,...`
   - 例如: `addr_index:addr1 -> tx1:0,tx1:1,tx3:0`

当添加、修改或删除数据时，系统会自动维护这两种索引的一致性。

## 查询性能

- **GetByUTXO**: 使用前缀查询，时间复杂度 O(log N + K)，其中 K 是与该 UTXO 关联的地址数量
- **GetByAddress**: 使用直接查询，时间复杂度 O(log N)

## 示例代码

完整的示例代码可以参考 `examples.go` 文件，运行示例：

```go
// 运行示例
import "github.com/metaid/utxo_indexer/storage"

func main() {
    storage.ExampleIndexedQueries()
}
```

## 测试

运行单元测试来验证功能：

```bash
go test -v github.com/metaid/utxo_indexer/storage -run TestIndexedQueries
``` 