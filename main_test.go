package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/btcsuite/btcd/txscript"
	"github.com/cockroachdb/pebble/v2"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/storage"
)

func TestDefaultMerger(t *testing.T) {
	opts := &pebble.Options{}

	// 创建一个临时数据库目录
	dir := "data/test_merge"
	db, err := pebble.Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() {
		db.Close()
		_ = os.RemoveAll(dir)
	}()

	key := []byte("key")

	// 第一次 Merge
	if err := db.Merge(key, []byte("hello"), pebble.Sync); err != nil {
		t.Fatalf("merge 1 failed: %v", err)
	}

	// 第二次 Merge
	if err := db.Merge(key, []byte(",world"), pebble.Sync); err != nil {
		t.Fatalf("merge 2 failed: %v", err)
	}

	// 获取最终值
	value, closer, err := db.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer closer.Close()

	expected := "hello,world"
	if string(value) != expected {
		t.Errorf("expected %q, got %q", expected, value)
	} else {
		fmt.Printf("Merge result: %s\n", value)
	}
}
func TestMerger(t *testing.T) {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	// 创建自动配置
	params := config.AutoConfigure(config.SystemResources{
		CPUCores:   cfg.CPUCores, // 16核CPU
		MemoryGB:   cfg.MemoryGB, // 64GB内存
		HighPerf:   cfg.HighPerf, // 优先性能
		ShardCount: cfg.ShardCount,
	})
	addressStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize address store: %v", err)
	}
	defer addressStore.Close()
	data1 := map[string][]string{
		"key1": {"value1-1", "value1-2"},
		"key2": {"value2-1"},
	}
	addressStore.BulkMergeMapConcurrent(&data1, 1)
	data2 := map[string][]string{
		"key1": {"value1-3", "value1-4"},
		"key2": {"value2-1"},
	}
	addressStore.BulkMergeMapConcurrent(&data2, 1)
	v, err := addressStore.Get([]byte("key1"))
	fmt.Println(err, string(v))
	addressStore.Delete([]byte("key1"))
	addressStore.Delete([]byte("key2"))
}

func TestGetTx(t *testing.T) {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	client, err := blockchain.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create blockchain client: %v", err)
	}
	tx, err := client.GetRawTransaction("4e2a33ebdd11d2c32f9d7b05ac5ca745cd1dae68700a5f3fc5b7d3ef139d30cb")
	if err != nil {
		log.Fatalf("Failed to get transaction: %v", err)
	}
	net, _ := cfg.GetChainParams()

	fmt.Println("===============OUT======================")
	for _, txOut := range tx.MsgTx().TxOut {
		_, addrs, _, err := txscript.ExtractPkScriptAddrs(txOut.PkScript, net)
		var address string
		if err == nil && len(addrs) > 0 {
			address = addrs[0].EncodeAddress()
		} else {
			// Fallback for non-standard scripts
			address = "errAddress"
		}
		fmt.Println(address, txOut.Value)
	}
}
func TestGetBlock(t *testing.T) {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	client, err := blockchain.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create blockchain client: %v", err)
	}
	hash, _ := client.GetBlockHash(575)
	block1, _ := client.GetBlock(hash)
	for _, tx := range block1.Tx {
		fmt.Println("==>tx", tx.Txid)
		for _, in := range tx.Vin {
			fmt.Println("   ==in", in.Txid, in.Vout)
		}

	}

	// fmt.Println("=====================================")
	// block2, _ := client.GetBlock2(hash)
	// for _, tx := range block2.Transactions() {
	// 	for _, in := range tx.MsgTx().TxIn {
	// 		fmt.Println("==>processSpend2", in.PreviousOutPoint.Hash.String(), in.PreviousOutPoint.Index)
	// 	}
	// }
	// size1 := 0
	// var txhash []string
	// for i := 557; i < 558; i++ {
	// 	hash, _ := client.GetBlockHash(int64(i))
	// 	fmt.Printf("Checking block at height %d, hash: %s\n", i, hash)
	// 	block1, _ := client.GetBlock(hash)
	// 	fmt.Printf("GetBlock returned %d transactions\n", len(block1.Tx))
	// 	for _, tx := range block1.Tx {
	// 		hash := tx.Hash
	// 		fmt.Printf("Transaction hash: %s,%s\n", hash, tx.Txid)
	// 		txhash = append(txhash, hash)
	// 		if hash == "eb6130a2d2a528c06f7285beff07616d2349d37d4c510bd561e81d453281397f" {
	// 			fmt.Printf("find tx (converted hash) at height %d: %s\n", i, hash)
	// 			fmt.Printf("Full tx data: %+v\n", tx)
	// 		}
	// 	}
	// 	size1 += len(block1.Tx)
	// }
	// fmt.Println("size1:", size1)
	// fmt.Println("txhash:", len(txhash))
	// size2 := 0
	// for i := 557; i < 558; i++ {
	// 	hash, _ := client.GetBlockHash(int64(i))
	// 	block2, _ := client.GetBlock2(hash)
	// 	fmt.Printf("GetBlock2 returned %d transactions\n", len(block2.Transactions()))
	// 	for _, tx := range block2.Transactions() {
	// 		hashStr := tx.Hash().String()
	// 		if hashStr == "eb6130a2d2a528c06f7285beff07616d2349d37d4c510bd561e81d453281397f" {
	// 			fmt.Printf("find tx2 (direct compare) at height %d: %s\n", i, hashStr)
	// 			fmt.Printf("Full tx data: %+v\n", tx)
	// 		}
	// 	}
	// 	size2 += len(block2.Transactions())
	// }
	// fmt.Println("size2:", size2)
}

// reverseHexString 反转16进制字符串的字节顺序
func reverseHexString(s string) string {
	b, err := hex.DecodeString(s)
	if err != nil {
		return s
	}
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return hex.EncodeToString(b)
}

func TestGetUtxoDb(t *testing.T) {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	fmt.Println(cfg.RPC.Chain)
	// 创建自动配置
	params := config.AutoConfigure(config.SystemResources{
		CPUCores:   cfg.CPUCores, // 16核CPU
		MemoryGB:   cfg.MemoryGB, // 64GB内存
		HighPerf:   cfg.HighPerf, // 优先性能
		ShardCount: cfg.ShardCount,
	})
	utxoStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUTXO, cfg.ShardCount)
	if err != nil {
		log.Fatalf("Failed to initialize UTXO store: %v", err)
	}
	defer utxoStore.Close()
	key := "135db46fc8e8efe42b77c13f5b7ed91b1ce18068bcd84cfd935715bdfef45275"
	value, err := utxoStore.Get([]byte(key))
	fmt.Println(string(value), err)
}
