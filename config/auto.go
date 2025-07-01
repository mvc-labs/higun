package config

import (
	"log"
	"runtime"
)

// SystemResources represents available system resources
type SystemResources struct {
	CPUCores   int  // Number of CPU cores
	MemoryGB   int  // Available memory (GB)
	HighPerf   bool // Prefer performance over memory saving
	ShardCount int  // Number of database shards
}

// IndexerParams contains all indexer and storage parameters
type IndexerParams struct {
	// Concurrency control
	WorkerCount int // Number of worker threads

	// Memory usage
	BatchSize      int // Index batch processing size
	MaxBatchSizeMB int // Database batch processing size (MB)
	JobBufferSize  int // Task queue size
	BytePoolSizeKB int // Initial byte pool size (KB)

	// Database configuration - per-shard configuration
	DBCacheSizeMB  int // Database cache size per shard (MB)
	MemTableSizeMB int // Write memory table size per shard (MB)
	WALSizeMB      int // Write-ahead log size per shard (MB)

	// Overall configuration
	TotalDBCacheMB     int // Total database cache for all shards (MB)
	TotalMemoryUsageMB int // Estimated total memory usage (MB)
	MaxTxPerBatch      int // Maximum transactions per shard
}

// AutoConfigure automatically calculates optimal configuration based on system resources
func AutoConfigure(res SystemResources) IndexerParams {
	// Ensure minimum values
	if res.CPUCores <= 0 {
		res.CPUCores = runtime.NumCPU()
	}
	if res.MemoryGB <= 0 {
		res.MemoryGB = 4 // Default 4GB
	}
	if res.ShardCount <= 0 {
		res.ShardCount = 16 // Default shard count
	}

	// Available memory (MB)
	memoryMB := res.MemoryGB * 1024

	// Basic configuration - select baseline based on performance mode
	var params IndexerParams
	if res.HighPerf {
		// High performance mode - use more memory
		params = IndexerParams{
			WorkerCount:    res.CPUCores * 2,
			BatchSize:      5000,
			MaxBatchSizeMB: 16,
			JobBufferSize:  50000,
			BytePoolSizeKB: 8,
		}
	} else {
		// Balanced mode - save memory
		params = IndexerParams{
			WorkerCount:    res.CPUCores,
			BatchSize:      500,
			MaxBatchSizeMB: 4,
			JobBufferSize:  10000,
			BytePoolSizeKB: 2,
		}
	}

	// Memory allocation ratio
	dbCachePercent := 0.4  // 40% for DB cache
	memTablePercent := 0.1 // 10% for write memory table
	walPercent := 0.05     // 5% for write-ahead log

	// Reserve some memory for system and other applications
	reservedPercent := 0.2 // 20% reserved for system and other applications
	availableMemoryMB := int(float64(memoryMB) * (1.0 - reservedPercent))

	// Calculate total resources
	totalDBCacheMB := int(float64(availableMemoryMB) * dbCachePercent)
	totalMemTableMB := int(float64(availableMemoryMB) * memTablePercent)
	totalWALMB := int(float64(availableMemoryMB) * walPercent)

	// Consider sharding - calculate resources per shard
	params.DBCacheSizeMB = totalDBCacheMB / res.ShardCount
	params.MemTableSizeMB = totalMemTableMB / res.ShardCount
	params.WALSizeMB = totalWALMB / res.ShardCount

	// Ensure minimum resources per shard
	minDBCacheMB := 32
	minMemTableMB := 8
	minWALMB := 4

	params.DBCacheSizeMB = max(params.DBCacheSizeMB, minDBCacheMB)
	params.MemTableSizeMB = max(params.MemTableSizeMB, minMemTableMB)
	params.WALSizeMB = max(params.WALSizeMB, minWALMB)

	// Storage total resource info
	params.TotalDBCacheMB = params.DBCacheSizeMB * res.ShardCount

	// Estimate total memory usage - use floating point
	indexBufferMB := float64(params.BatchSize) * 0.001    // Roughly estimate 1MB per 1000 items
	jobBufferMB := float64(params.JobBufferSize) * 0.0001 // Roughly estimate 1MB per 10000 tasks

	params.TotalMemoryUsageMB = params.TotalDBCacheMB +
		(params.MemTableSizeMB * res.ShardCount) +
		(params.WALSizeMB * res.ShardCount) +
		int(indexBufferMB) +
		int(jobBufferMB)

	// Special adjustment for large memory systems
	if res.MemoryGB >= 32 {
		params.BatchSize *= 2
		params.MaxBatchSizeMB *= 2
		params.JobBufferSize *= 2
	}

	// Ensure reasonable match between worker threads and shard count
	params.WorkerCount = min(params.WorkerCount, res.ShardCount*4)
	log.Printf("Using configuration: CPU=%d, Memory=%dGB, Shards=%d, BatchSize=%d, Workers=%d",
		res.CPUCores, res.MemoryGB, res.ShardCount, params.BatchSize, params.WorkerCount)
	return params
}

// Helper function: get max value
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Helper function: get min value
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
