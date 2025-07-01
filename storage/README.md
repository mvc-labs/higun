# Storage Index Query Functionality

This document describes how to use the index query functionality in the storage package, especially for cases with composite keys.

## Composite Key Query

In this system, we use a composite key structure `txid:index_address`, for example `tx1:0_addr1`. To enable data queries from different perspectives, we implement the following features:

### 1. Query by UTXO ID

Query all addresses and values associated with the UTXO by the `txid:index` part:

```go
// Example: Query addresses associated with tx1:0
addresses, err := store.GetByUTXO("tx1:0")
if err != nil {
    log.Fatalf("Query failed: %v", err)
}
fmt.Println("Associated addresses:", addresses)
```

### 2. Query by Address

Query all UTXOs associated with the address by the `address` part:

```go
// Example: Query all UTXOs associated with addr1
utxos, err := store.GetByAddress("addr1")
if err != nil {
    log.Fatalf("Query failed: %v", err)
}
fmt.Println("Associated UTXO list:", utxos)
```

## Data Operation Functions

### 1. Add a Record with Index

```go
// Add a single record and update the index at the same time
err := store.SetWithIndex("tx1:0", "addr1", []byte("100"))
```

### 2. Batch Add Records with Index

```go
// Batch add records
batchRecords := map[string]map[string][]byte{
    "tx4:0": {"addr3": []byte("500")},
    "tx4:1": {"addr3": []byte("600")},
    "tx5:0": {"addr1": []byte("700")},
}
err := store.BatchSetWithIndex(batchRecords)
```

### 3. Delete a Record and Update Index

```go
// Delete a record and update the index at the same time
err := store.DeleteWithIndex("tx1:0", "addr1")
```

## Index Implementation Principle

The system maintains two data structures to achieve bidirectional queries:

1. **Primary Key**: `txid:index_address -> value`
   - Example: `tx1:0_addr1 -> 100`

2. **Reverse Index**: `addr_index:address -> txid:index1,txid:index2,...`
   - Example: `addr_index:addr1 -> tx1:0,tx1:1,tx3:0`

When adding, modifying, or deleting data, the system will automatically maintain the consistency of these two indexes.

## Query Performance

- **GetByUTXO**: Uses prefix query, time complexity O(log N + K), where K is the number of addresses associated with the UTXO
- **GetByAddress**: Uses direct query, time complexity O(log N)

## Example Code

Complete example code can be found in the `examples.go` file. To run the example:

```go
// Run example
import "github.com/metaid/utxo_indexer/storage"

func main() {
    storage.ExampleIndexedQueries()
}
```

## Testing

Run unit tests to verify functionality:

```bash
go test -v github.com/metaid/utxo_indexer/storage -run TestIndexedQueries
``` 