package config

import (
	"log"
	"runtime"
)

// SystemResources 表示系统可用资源
type SystemResources struct {
	CPUCores   int  // CPU核心数
	MemoryGB   int  // 可用内存(GB)
	HighPerf   bool // 是否优先性能而非内存节约
	ShardCount int  // 数据库分片数量
}

// IndexerParams 包含所有索引器和存储参数
type IndexerParams struct {
	// 并发控制
	WorkerCount int // 工作线程数

	// 内存使用
	BatchSize      int // 索引批处理大小
	MaxBatchSizeMB int // 数据库批处理大小(MB)
	JobBufferSize  int // 任务队列大小
	BytePoolSizeKB int // 字节池初始大小(KB)

	// 数据库配置 - 每个分片的配置
	DBCacheSizeMB  int // 每个分片的数据库缓存大小(MB)
	MemTableSizeMB int // 每个分片的写入内存表大小(MB)
	WALSizeMB      int // 每个分片的预写日志大小(MB)

	// 整体配置
	TotalDBCacheMB     int // 所有分片总数据库缓存(MB)
	TotalMemoryUsageMB int // 预估总内存使用(MB)
	MaxTxPerBatch      int // 每个分片最大交易数
}

// AutoConfigure 根据系统资源自动计算最佳配置
func AutoConfigure(res SystemResources) IndexerParams {
	// 确保最小值
	if res.CPUCores <= 0 {
		res.CPUCores = runtime.NumCPU()
	}
	if res.MemoryGB <= 0 {
		res.MemoryGB = 4 // 默认4GB
	}
	if res.ShardCount <= 0 {
		res.ShardCount = 16 // 默认分片数
	}

	// 可用内存(MB)
	memoryMB := res.MemoryGB * 1024

	// 基本配置 - 根据性能模式选择基线
	var params IndexerParams
	if res.HighPerf {
		// 高性能模式 - 使用更多内存
		params = IndexerParams{
			WorkerCount:    res.CPUCores * 2,
			BatchSize:      5000,
			MaxBatchSizeMB: 16,
			JobBufferSize:  50000,
			BytePoolSizeKB: 8,
		}
	} else {
		// 平衡模式 - 节约内存
		params = IndexerParams{
			WorkerCount:    res.CPUCores,
			BatchSize:      500,
			MaxBatchSizeMB: 4,
			JobBufferSize:  10000,
			BytePoolSizeKB: 2,
		}
	}

	// 内存分配比例
	dbCachePercent := 0.4  // 40%用于DB缓存
	memTablePercent := 0.1 // 10%用于写内存表
	walPercent := 0.05     // 5%用于预写日志

	// 保留一部分内存给系统和应用其他部分
	reservedPercent := 0.2 // 20%保留给系统和其他应用
	availableMemoryMB := int(float64(memoryMB) * (1.0 - reservedPercent))

	// 计算总资源
	totalDBCacheMB := int(float64(availableMemoryMB) * dbCachePercent)
	totalMemTableMB := int(float64(availableMemoryMB) * memTablePercent)
	totalWALMB := int(float64(availableMemoryMB) * walPercent)

	// 考虑分片 - 计算每个分片的资源
	params.DBCacheSizeMB = totalDBCacheMB / res.ShardCount
	params.MemTableSizeMB = totalMemTableMB / res.ShardCount
	params.WALSizeMB = totalWALMB / res.ShardCount

	// 确保每个分片有最小资源
	minDBCacheMB := 32
	minMemTableMB := 8
	minWALMB := 4

	params.DBCacheSizeMB = max(params.DBCacheSizeMB, minDBCacheMB)
	params.MemTableSizeMB = max(params.MemTableSizeMB, minMemTableMB)
	params.WALSizeMB = max(params.WALSizeMB, minWALMB)

	// 存储总资源信息
	params.TotalDBCacheMB = params.DBCacheSizeMB * res.ShardCount

	// 计算总内存使用估计 - 使用浮点数
	indexBufferMB := float64(params.BatchSize) * 0.001    // 粗略估计每1000个条目消耗1MB
	jobBufferMB := float64(params.JobBufferSize) * 0.0001 // 粗略估计每10000个任务消耗1MB

	params.TotalMemoryUsageMB = params.TotalDBCacheMB +
		(params.MemTableSizeMB * res.ShardCount) +
		(params.WALSizeMB * res.ShardCount) +
		int(indexBufferMB) +
		int(jobBufferMB)

	// 对大内存系统做特殊调整
	if res.MemoryGB >= 32 {
		params.BatchSize *= 2
		params.MaxBatchSizeMB *= 2
		params.JobBufferSize *= 2
	}

	// 确保工作线程和分片数量合理搭配
	params.WorkerCount = min(params.WorkerCount, res.ShardCount*4)
	log.Printf("使用配置: CPU=%d, 内存=%dGB, 分片=%d, 批处理大小=%d, 工作线程=%d",
		res.CPUCores, res.MemoryGB, res.ShardCount, params.BatchSize, params.WorkerCount)
	return params
}

// 辅助函数：取最大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 辅助函数：取最小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
