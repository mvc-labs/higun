package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	indexer "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"

	"github.com/metaid/utxo_indexer/api"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/storage"
)

func main() {
	// 加载配置
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	fmt.Println("cfg", cfg)
	config.GlobalConfig = cfg
	config.GlobalNetwork, _ = cfg.GetChainParams()

	// 创建自动配置
	params := config.AutoConfigure(config.SystemResources{
		CPUCores:   cfg.CPUCores,
		MemoryGB:   cfg.MemoryGB,
		HighPerf:   cfg.HighPerf,
		ShardCount: cfg.ShardCount,
	})
	params.MaxTxPerBatch = cfg.MaxTxPerBatch
	common.InitBytePool(params.BytePoolSizeKB)
	storage.DbInit(params)

	// 初始化存储
	contractFtUtxoStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTUTXO, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化FT存储失败: %v", err)
	}
	defer contractFtUtxoStore.Close()

	addressFtIncomeStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeAddressFTIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化FT地址存储失败: %v", err)
	}
	defer addressFtIncomeStore.Close()

	addressFtSpendStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeAddressFTSpend, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化FT花费存储失败: %v", err)
	}
	defer addressFtSpendStore.Close()

	contractFtInfoStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTInfo, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化FT信息存储失败: %v", err)
	}
	defer contractFtInfoStore.Close()

	contractFtGenesisStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTGenesis, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化FT创世存储失败: %v", err)
	}
	defer contractFtGenesisStore.Close()

	contractFtGenesisOutputStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTGenesisOutput, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化FT创世输出存储失败: %v", err)
	}
	defer contractFtGenesisOutputStore.Close()

	contractFtGenesisUtxoStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeContractFTGenesisUTXO, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化FT创世UTXO存储失败: %v", err)
	}
	defer contractFtGenesisUtxoStore.Close()

	addressFtIncomeValidStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeAddressFTIncomeValid, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化有效address FT UTXO存储失败: %v", err)
	}
	defer addressFtIncomeValidStore.Close()

	uncheckFtOutpointStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUnCheckFtIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化FT验证存储失败: %v", err)
	}
	defer uncheckFtOutpointStore.Close()

	usedFtIncomeStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUsedFTIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化已使用的FT合约Utxo存储失败: %v", err)
	}
	defer usedFtIncomeStore.Close()

	uniqueFtIncomeStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUniqueFTIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化unique合约UTXO存储失败: %v", err)
	}
	defer uniqueFtIncomeStore.Close()

	uniqueFtSpendStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUniqueFTSpend, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化unique合约UTXO存储失败: %v", err)
	}
	defer uniqueFtSpendStore.Close()

	// 创建区块链客户端
	bcClient, err := blockchain.NewFtClient(cfg)
	if err != nil {
		log.Fatalf("创建区块链客户端失败: %v", err)
	}
	defer bcClient.Shutdown()

	// 创建元数据存储
	metaStore, err := storage.NewMetaStore(cfg.DataDir)
	if err != nil {
		log.Fatalf("创建元数据存储失败: %v", err)
	}
	defer metaStore.Close()

	// 验证最后索引高度
	lastHeight, err := metaStore.Get([]byte("last_ft_indexed_height"))
	if err == nil {
		log.Printf("从高度 %s 恢复FT索引", lastHeight)
	} else if errors.Is(err, storage.ErrNotFound) {
		log.Println("从创世区块开始新FT索引")
	} else {
		log.Printf("读取上次FT高度出错: %v", err)
	}

	// 强制同步元数据存储以确保持久性
	if err := metaStore.Sync(); err != nil {
		log.Printf("同步元数据存储失败: %v", err)
	}

	// 创建停止信号通道
	stopCh := make(chan struct{})

	// 捕获中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 处理中断信号
	go func() {
		<-sigCh
		log.Println("收到停止信号，准备关闭...")
		close(stopCh)
	}()

	// 创建FT索引器
	idx := indexer.NewContractFtIndexer(params,
		contractFtUtxoStore,
		addressFtIncomeStore,
		addressFtSpendStore,
		contractFtInfoStore,
		contractFtGenesisStore,
		contractFtGenesisOutputStore,
		contractFtGenesisUtxoStore,
		addressFtIncomeValidStore,
		uncheckFtOutpointStore,
		usedFtIncomeStore,
		uniqueFtIncomeStore,
		uniqueFtSpendStore,
		metaStore)

	// 获取当前区块链高度
	bestHeight, err := bcClient.GetBlockCount()
	if err != nil {
		log.Fatalf("获取区块数量失败: %v", err)
	}

	// 启动API服务器
	server := api.NewFtServer(idx, metaStore, stopCh)
	log.Printf("启动FT-UTXO索引器API，端口:%s", cfg.APIPort)
	go server.Start(fmt.Sprintf(":%s", cfg.APIPort))

	lastHeightInt, err := strconv.Atoi(string(lastHeight))
	if err != nil {
		lastHeightInt = 0
		log.Printf("上次高度转换失败，从0开始: %v", err)
	}

	// 初始化进度条
	idx.InitProgressBar(bestHeight, lastHeightInt)

	// 检查新区块的间隔时间
	checkInterval := 10 * time.Second

	log.Println("开始FT区块同步...")

	// 使用goroutine启动区块同步
	go func() {
		if err := bcClient.SyncBlocks(idx, checkInterval, stopCh, nil); err != nil {
			log.Fatalf("同步FT区块失败: %v", err)
		}
	}()

	// 等待停止信号
	<-stopCh
	log.Println("程序正在关闭...")

	// 获取最终索引高度
	finalHeight, err := idx.GetLastIndexedHeight()
	if err != nil {
		log.Printf("获取最终FT索引高度出错: %v", err)
	} else {
		log.Printf("最终FT索引高度: %d", finalHeight)
	}
}
