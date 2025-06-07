package blockindexer

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/config"
)

func TestGetBlockInfo(t *testing.T) {
	cfg, err := config.LoadConfig("../../config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	client, err := blockchain.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create blockchain client: %v", err)
	}
	hash, _ := client.GetBlockHash(124604)

	result, err := client.GetBlockHeaderWithTimeout(hash.String(), 5*time.Second)
	fmt.Println(err)
	fmt.Printf("GetBlockHeaderWithTimeout:%+v\n", result)
	stats, err := client.GetChainStatus()
	fmt.Println(err)
	fmt.Printf("%+v\n", stats)
	block1, _ := client.GetBlock(hash)
	fmt.Println("block1:", block1.Size)
	find := false
	for _, tx := range block1.Tx {
		if len(tx.Vin) == 0 {
			break
		}
		for _, in := range tx.Vin {
			if in.IsCoinBase() {
				tx2, err := client.GetRawTransaction(tx.Txid)
				fmt.Println(err)
				if err == nil {
					miner, re := GetMinerAndReward(tx2)
					fmt.Println(">>>", miner, re)
				}
				find = true
				break
			}
		}
		if find {
			break
		}
	}
}
