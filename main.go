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

	"github.com/metaid/utxo_indexer/api"
	"github.com/metaid/utxo_indexer/blockchain"
	"github.com/metaid/utxo_indexer/common"
	"github.com/metaid/utxo_indexer/config"
	"github.com/metaid/utxo_indexer/explorer/blockindexer"
	"github.com/metaid/utxo_indexer/indexer"
	"github.com/metaid/utxo_indexer/mempool"
	"github.com/metaid/utxo_indexer/storage"
)

var ApiServer *api.Server

func main() {
	// Load config
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	config.GlobalConfig = cfg
	config.GlobalNetwork, _ = cfg.GetChainParams()
	// //执行区块信息索引
	fmt.Println("正在初始化区块信息索引...")
	blockindexer.IndexerInit("blockinfo_data", cfg)
	go blockindexer.DoBlockInfoIndex()
	go blockindexer.SaveBlockInfoData()
	log.Println("0.==============>")
	// 创建自动配置
	params := config.AutoConfigure(config.SystemResources{
		CPUCores:   cfg.CPUCores,
		MemoryGB:   cfg.MemoryGB,
		HighPerf:   cfg.HighPerf,
		ShardCount: cfg.ShardCount,
	})
	params.MaxTxPerBatch = config.GlobalConfig.MaxTxPerBatch
	common.InitBytePool(params.BytePoolSizeKB)
	log.Println("1.==============>")
	storage.DbInit(params)
	log.Println("2.==============>")
	// Initialize storage
	utxoStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeUTXO, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化UTXO存储失败: %v", err)
	}
	defer utxoStore.Close()
	log.Println("3.==============>")
	addressStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeIncome, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化地址存储失败: %v", err)
	}
	log.Println("4.==============>")
	defer addressStore.Close()

	spendStore, err := storage.NewPebbleStore(params, cfg.DataDir, storage.StoreTypeSpend, cfg.ShardCount)
	if err != nil {
		log.Fatalf("初始化花费存储失败: %v", err)
	}
	defer spendStore.Close()
	log.Println("5.==============>")

	// 创建区块链客户端（提前创建，供内存池清理使用）
	bcClient, err := blockchain.NewClient(cfg)
	if err != nil {
		log.Fatalf("创建区块链客户端失败: %v", err)
	}
	defer bcClient.Shutdown()
	log.Println("6.==============>")
	// 创建元数据存储（提前创建，供内存池清理使用）
	metaStore, err := storage.NewMetaStore(cfg.DataDir)
	if err != nil {
		log.Fatalf("创建元数据存储失败: %v", err)
	}
	defer metaStore.Close()
	log.Println("7.==============>")
	// 验证最后索引高度
	lastHeight, err := metaStore.Get([]byte("last_indexed_height"))
	if err == nil {
		log.Printf("从高度 %s 恢复索引", lastHeight)
	} else if errors.Is(err, storage.ErrNotFound) {
		log.Println("从创世区块开始新索引")
	} else {
		log.Printf("读取上次高度出错: %v", err)
	}

	// Force sync meta store to ensure durability
	if err := metaStore.Sync(); err != nil {
		log.Printf("同步元数据存储失败: %v", err)
	}

	// 确保last_mempool_clean_height有初始值
	_, err = metaStore.Get([]byte("last_mempool_clean_height"))
	if errors.Is(err, storage.ErrNotFound) {
		// 第一次运行，使用配置文件中的起始高度
		startHeight := strconv.Itoa(cfg.MemPoolCleanStartHeight)
		log.Printf("初始化内存池清理高度为: %s", startHeight)
		err = metaStore.Set([]byte("last_mempool_clean_height"), []byte(startHeight))
		if err != nil {
			log.Printf("初始化内存池清理高度失败: %v", err)
		}
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

	// 获取链参数
	chainCfg, err := cfg.GetChainParams()
	if err != nil {
		log.Fatalf("获取链参数失败: %v", err)
	}

	// 创建内存池管理器，但不启动
	log.Printf("初始化内存池管理器，ZMQ地址: %s, 网络: %s", cfg.ZMQAddress, cfg.Network)
	mempoolMgr := mempool.NewMempoolManager(cfg.DataDir, utxoStore, chainCfg, cfg.ZMQAddress)
	if mempoolMgr == nil {
		log.Printf("创建内存池管理器失败")
	}

	idx := indexer.NewUTXOIndexer(params, utxoStore, addressStore, metaStore, spendStore)

	// 设置内存池管理器，让indexer能够查询内存池UTXO
	if mempoolMgr != nil {
		idx.SetMempoolManager(mempoolMgr)
	}
	// Start API server with Gin
	ApiServer = api.NewServer(idx, metaStore, stopCh)
	// 将内存池管理器和区块链客户端传递给API服务器
	ApiServer.SetMempoolManager(mempoolMgr, bcClient)
	log.Printf("启动UTXO索引器API，端口:%s", cfg.APIPort)
	blockindexer.SetRouter(ApiServer)
	go ApiServer.Start(fmt.Sprintf(":%s", cfg.APIPort))
	// Get current blockchain height
	var bestHeight int
	//for {
	bestHeight, err = bcClient.GetBlockCount()
	if err != nil {
		log.Printf("获取区块数量失败: %v，3秒后重试...", err)
		//time.Sleep(3 * time.Second)
		//continue
	}
	//	break
	//}

	lastHeightInt, err := strconv.Atoi(string(lastHeight))
	if err != nil {
		lastHeightInt = 0
		log.Printf("上次高度转换失败，从0开始: %v", err)
	}
	lastHeightInt = 121050
	// Initialize progress bar
	idx.InitProgressBar(bestHeight, lastHeightInt)

	// 检查新区块的间隔时间
	checkInterval := 10 * time.Second

	log.Println("开始区块同步...")
	//log.Println("注意: 内存池未自动启动，请在区块同步完成后使用API '/mempool/start'启动内存池")

	// 使用goroutine启动区块同步，不再自动启动内存池
	go func() {
		if err := bcClient.SyncBlocks(idx, checkInterval, stopCh, firstSyncCompleted); err != nil {
			log.Printf("同步区块失败: %v，3秒后重试...", err)
			select {
			case <-stopCh:
				return
			case <-time.After(3 * time.Second):
				// 继续重试
			}
		} else {
			// 正常退出（一般不会到这里）
			return
		}
	}()

	// 等待停止信号
	<-stopCh
	log.Println("程序正在关闭...")

	// 关闭内存池（如果已启动）
	if mempoolMgr != nil {
		mempoolMgr.Stop()
	}

	// 程序不会执行到这里，除非收到停止信号
	finalHeight, err := idx.GetLastIndexedHeight()
	if err != nil {
		log.Printf("获取最终索引高度出错: %v", err)
	} else {
		log.Printf("最终索引高度: %d", finalHeight)
	}
}
func firstSyncCompleted() {
	//return
	log.Println("初始同步完成，启动内存池")
	err := ApiServer.RebuildMempool()
	if err != nil {
		log.Printf("重建内存池失败: %v", err)
		return
	}
	err = ApiServer.StartMempoolCore()
	if err != nil {
		log.Printf("启动内存池核心失败: %v", err)
		return
	}
	log.Println("内存池核心已启动")
}
