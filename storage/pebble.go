package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/metaid/utxo_indexer/config"
)

const (
	defaultShardCount = 1
)

var (
	// ErrNotFound is returned when a key is not found in the database
	ErrNotFound  = errors.New("not found")
	maxBatchSize = int(4) * 1024 * 1024
	// Create custom log disabler
	noopLogger = &customLogger{}
)

// Custom logger - outputs nothing
type customLogger struct{}

func (l *customLogger) Infof(format string, args ...interface{})  {}
func (l *customLogger) Fatalf(format string, args ...interface{}) {}
func (l *customLogger) Errorf(format string, args ...interface{}) {}

type PebbleStore struct {
	shards []*pebble.DB
	mu     sync.RWMutex
}

type MetaStore struct {
	db *pebble.DB
}

func DbInit(params config.IndexerParams) {
	maxBatchSize = params.MaxBatchSizeMB * 1024 * 1024
}
func (m *MetaStore) Get(key []byte) ([]byte, error) {
	value, closer, err := m.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), value...), nil
}

func (m *MetaStore) Set(key, value []byte) error {
	return m.db.Set(key, value, nil)
}

func (m *MetaStore) Close() error {
	// Sync before closing
	if err := m.db.LogData(nil, pebble.Sync); err != nil {
		return err
	}
	return m.db.Close()
}

func (m *MetaStore) Sync() error {
	return m.db.LogData(nil, pebble.Sync)
}

type StoreType int

const (
	StoreTypeUTXO StoreType = iota
	StoreTypeIncome
	StoreTypeSpend
	StoreTypeMeta
	StoreTypeContractFTUTXO
	StoreTypeAddressFTIncome
	StoreTypeAddressFTSpend
	StoreTypeContractFTInfo
	StoreTypeContractFTGenesis
	StoreTypeContractFTGenesisOutput
	StoreTypeContractFTGenesisUTXO
	StoreTypeAddressFTIncomeValid
	StoreTypeUnCheckFtIncome
	StoreTypeUsedFTIncome
	StoreTypeUniqueFTIncome
	StoreTypeUniqueFTSpend
	StoreTypeInvalidFtOutpoint
)

func NewMetaStore(dataDir string) (*MetaStore, error) {
	dbPath := filepath.Join(dataDir, "meta")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create meta directory: %w", err)
	}
	db, err := pebble.Open(dbPath, &pebble.Options{Logger: noopLogger})
	if err != nil {
		return nil, fmt.Errorf("failed to open meta store: %w", err)
	}
	return &MetaStore{db: db}, nil
}

// Configure database options

func NewPebbleStore(params config.IndexerParams, dataDir string, storeType StoreType, shardCount int) (*PebbleStore, error) {
	if shardCount <= 0 {
		shardCount = defaultShardCount
	}
	// dbOptions := &pebble.Options{
	// 	Cache:        pebble.NewCache(int64(params.DBCacheSizeMB) * 1024 * 1024),
	// 	MemTableSize: uint64(params.MemTableSizeMB) * 1024 * 1024,
	// 	WALMinSyncInterval: func() time.Duration {
	// 		return time.Duration(params.WALSizeMB) * time.Millisecond
	// 	},
	// }
	dbOptions := &pebble.Options{
		//Logger: noopLogger,
		Levels: []pebble.LevelOptions{
			{
				Compression: pebble.NoCompression,
			},
		},
		//MemTableSize:                32 << 20, // Reduce to 32MB (default 64MB)
		//MemTableStopWritesThreshold: 2,        // Default 4
		// Limit block cache size (e.g., 128MB, adjust based on machine memory)
		Cache: pebble.NewCache(2 << 30), // 128MB
		// Limit table cache count (e.g., 64)
		//MaxOpenFiles: 512,
	}
	store := &PebbleStore{
		shards: make([]*pebble.DB, shardCount),
	}

	for i := 0; i < shardCount; i++ {
		var dbPath string
		switch storeType {
		case StoreTypeUTXO:
			dbPath = filepath.Join(dataDir, "utxo", fmt.Sprintf("shard_%d", i))
		case StoreTypeIncome:
			dbPath = filepath.Join(dataDir, "income", fmt.Sprintf("shard_%d", i))
		case StoreTypeSpend:
			dbPath = filepath.Join(dataDir, "spend", fmt.Sprintf("shard_%d", i))
		case StoreTypeContractFTUTXO:
			dbPath = filepath.Join(dataDir, "contract_ft_utxo", fmt.Sprintf("shard_%d", i))
		case StoreTypeAddressFTIncome:
			dbPath = filepath.Join(dataDir, "address_ft_income", fmt.Sprintf("shard_%d", i))
		case StoreTypeAddressFTSpend:
			dbPath = filepath.Join(dataDir, "address_ft_spend", fmt.Sprintf("shard_%d", i))
		case StoreTypeContractFTInfo:
			dbPath = filepath.Join(dataDir, "contract_ft_info", fmt.Sprintf("shard_%d", i))
		case StoreTypeContractFTGenesis:
			dbPath = filepath.Join(dataDir, "contract_ft_genesis", fmt.Sprintf("shard_%d", i))
		case StoreTypeContractFTGenesisOutput:
			dbPath = filepath.Join(dataDir, "contract_ft_genesis_output", fmt.Sprintf("shard_%d", i))
		case StoreTypeContractFTGenesisUTXO:
			dbPath = filepath.Join(dataDir, "contract_ft_genesis_utxo", fmt.Sprintf("shard_%d", i))
		case StoreTypeAddressFTIncomeValid:
			dbPath = filepath.Join(dataDir, "address_ft_income_valid", fmt.Sprintf("shard_%d", i))
		case StoreTypeUnCheckFtIncome:
			dbPath = filepath.Join(dataDir, "uncheck_ft_income", fmt.Sprintf("shard_%d", i))
		case StoreTypeUsedFTIncome:
			dbPath = filepath.Join(dataDir, "used_ft_income", fmt.Sprintf("shard_%d", i))
		case StoreTypeUniqueFTIncome:
			dbPath = filepath.Join(dataDir, "unique_ft_income", fmt.Sprintf("shard_%d", i))
		case StoreTypeUniqueFTSpend:
			dbPath = filepath.Join(dataDir, "unique_ft_spend", fmt.Sprintf("shard_%d", i))
		case StoreTypeInvalidFtOutpoint:
			dbPath = filepath.Join(dataDir, "invalid_ft_outpoint", fmt.Sprintf("shard_%d", i))
		}
		// Create parent directories if needed
		if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}

		db, err := pebble.Open(dbPath, dbOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to open shard %d: %w", i, err)
		}
		store.shards[i] = db
	}

	return store, nil
}

func (s *PebbleStore) getShard(key string) *pebble.DB {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := xxhash.Sum64String(key)
	return s.shards[h%uint64(len(s.shards))]
}

func (s *PebbleStore) GetWithShard(key []byte) ([]byte, *pebble.DB, error) {
	db := s.getShard(string(key))
	value, closer, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, db, ErrNotFound
		}
		return nil, db, err
	}
	defer closer.Close()
	return append([]byte(nil), value...), db, nil
}

func (s *PebbleStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	for _, db := range s.shards {
		if closeErr := db.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

type Batch struct {
	batches []*pebble.Batch
	store   *PebbleStore
}

func (s *PebbleStore) NewBatch() *Batch {
	return &Batch{
		batches: make([]*pebble.Batch, len(s.shards)),
		store:   s,
	}
}

func (b *Batch) Set(key, value []byte) error {
	db := b.store.getShard(string(key))
	shardIdx := b.store.getShardIndex(string(key))

	if b.batches[shardIdx] == nil {
		b.batches[shardIdx] = db.NewBatch()
	}
	return b.batches[shardIdx].Set(key, value, nil)
}

func (b *Batch) Commit() error {
	for _, batch := range b.batches {
		if batch != nil {
			if err := batch.Commit(nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *PebbleStore) getShardIndex(key string) int {
	h := xxhash.Sum64String(key)
	return int(h % uint64(len(s.shards)))
}

// BulkWriteMapConcurrent concurrently writes a map with many keys to corresponding shards
func (s *PebbleStore) BulkWriteMapConcurrent(data *map[string][]string, concurrency int) error {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	// Allocate workers based on shard count
	type job struct {
		shardIdx int
		key      string
		value    []byte
	}

	jobsCh := make(chan job, len(*data))
	errCh := make(chan error, 1)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var currentBatch *pebble.Batch
			var currentShardIdx int

			for job := range jobsCh {
				db := s.shards[job.shardIdx]

				// Commit current batch when switching shards
				if currentBatch != nil && currentShardIdx != job.shardIdx {
					if err := currentBatch.Commit(pebble.Sync); err != nil {
						select {
						case errCh <- fmt.Errorf("commit failed on shard %d: %w", currentShardIdx, err):
						default:
						}
						return
					}
					currentBatch.Reset()
					currentBatch = nil
				}

				// Initialize batch
				if currentBatch == nil {
					currentBatch = db.NewBatch()
					currentShardIdx = job.shardIdx
				}

				// Write data
				if err := currentBatch.Set([]byte(job.key), job.value, nil); err != nil {
					select {
					case errCh <- fmt.Errorf("set failed on shard %d: %w", job.shardIdx, err):
					default:
					}
					return
				}

				// Control batch size (e.g., 4MB)
				//if currentBatch.Len() > 4<<20 { // 4MB
				if currentBatch.Len() > maxBatchSize {
					if err := currentBatch.Commit(pebble.Sync); err != nil {
						select {
						case errCh <- fmt.Errorf("commit failed on shard %d: %w", job.shardIdx, err):
						default:
						}
						return
					}
					currentBatch = db.NewBatch()
				}
			}

			// Commit final batch
			if currentBatch != nil {
				if err := currentBatch.Commit(pebble.Sync); err != nil {
					select {
					case errCh <- fmt.Errorf("final commit failed on shard %d: %w", currentShardIdx, err):
					default:
					}
				}
			}
		}()
	}

	// Send tasks
	for key, values := range *data {
		fmt.Println("key:", key, "values:", strings.Join(values, ","))
		shardIdx := s.getShardIndex(key)
		valueBytes := []byte(strings.Join(values, ",")) // Can be replaced with other serialization methods
		jobsCh <- job{
			shardIdx: shardIdx,
			key:      key,
			value:    valueBytes,
		}
	}
	close(jobsCh)

	// Wait for completion
	go func() {
		wg.Wait()
	}()

	// Check for errors
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
func (s *PebbleStore) Get(key []byte) ([]byte, error) {
	db := s.getShard(string(key))
	value, closer, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), value...), nil
}

func (s *PebbleStore) Delete(key []byte) error {
	db := s.getShard(string(key))
	return db.Delete(key, pebble.Sync)
}
func (s *PebbleStore) Set(key, value []byte) error {
	db := s.getShard(string(key))
	return db.Set(key, value, pebble.Sync)
}

func (s *PebbleStore) Put(key, value []byte) error {
	db := s.getShard(string(key))
	return db.Set(key, value, nil)
}

func (s *PebbleStore) GetLastHeight() (int, error) {
	key := []byte("last_height")
	data, err := s.Get(key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return strconv.Atoi(string(data))
}

func (s *PebbleStore) SaveLastHeight(height int) error {
	key := []byte("last_height")
	return s.Put(key, []byte(strconv.Itoa(height)))
}

// Restore to near original version, only fix a few key issues
// Modify BulkMergeMapConcurrent function
func (s *PebbleStore) BulkMergeMapConcurrentBak(data *map[string][]string, concurrency int) error {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	concurrency = 1
	type job struct {
		shardIdx int
		key      string
		value    []byte
	}

	jobsCh := make(chan job, len(*data))
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	// Replace global lock with shard lock array
	shardMutexes := make([]sync.Mutex, len(s.shards))
	shardBatches := make([]*pebble.Batch, len(s.shards))

	// Initialize batch for each shard
	for i := range shardBatches {
		shardBatches[i] = s.shards[i].NewBatch()
	}

	// Start worker
	// for w := 0; w < concurrency; w++ {
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()

	// 		for job := range jobsCh {
	// 			db := s.shards[job.shardIdx]

	// 			// Use shard-level lock instead of global lock
	// 			shardMutexes[job.shardIdx].Lock()
	// 			batch := shardBatches[job.shardIdx]
	// 			if batch == nil {
	// 				batch = db.NewBatch()
	// 				shardBatches[job.shardIdx] = batch
	// 			}

	// 			// Use Pebble Batch.Merge for merge writes
	// 			if err := batch.Merge([]byte(job.key), job.value, pebble.NoSync); err != nil {
	// 				shardMutexes[job.shardIdx].Unlock()
	// 				select {
	// 				case errCh <- fmt.Errorf("merge failed on shard %d: %w", job.shardIdx, err):
	// 				default:
	// 				}
	// 				return
	// 			}
	// 			shardMutexes[job.shardIdx].Unlock()
	// 		}
	// 	}()
	// }
	// Modification: Add batch size limit
	maxBatchItems := 1000 // Maximum items per batch
	batchItemCounters := make([]int, len(s.shards))
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobsCh {
				db := s.shards[job.shardIdx]

				shardMutexes[job.shardIdx].Lock()
				batch := shardBatches[job.shardIdx]
				if batch == nil {
					batch = db.NewBatch()
					shardBatches[job.shardIdx] = batch
				}

				// Merge write
				if err := batch.Merge([]byte(job.key), job.value, pebble.Sync); err != nil {
					shardMutexes[job.shardIdx].Unlock()
					select {
					case errCh <- fmt.Errorf("merge failed on shard %d: %w", job.shardIdx, err):
					default:
					}
					return
				}

				// Check batch size and commit when appropriate
				batchItemCounters[job.shardIdx]++
				if batchItemCounters[job.shardIdx] >= maxBatchItems || batch.Len() >= maxBatchSize {
					if err := batch.Commit(pebble.Sync); err != nil {
						shardMutexes[job.shardIdx].Unlock()
						select {
						case errCh <- fmt.Errorf("commit failed on shard %d: %w", job.shardIdx, err):
						default:
						}
						return
					}
					// Reset batch
					batch.Reset()
					batchItemCounters[job.shardIdx] = 0
				}

				shardMutexes[job.shardIdx].Unlock()
			}
		}()
	}
	// Send tasks
	for key, values := range *data {
		shardIdx := s.getShardIndex(key)
		valueBytes := []byte("," + strings.Join(values, ","))
		jobsCh <- job{
			shardIdx: shardIdx,
			key:      key,
			value:    valueBytes,
		}
	}
	close(jobsCh)

	// Wait for all to complete
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// Check for errors
	if err := <-errCh; err != nil {
		return err
	}

	// Commit all batches - need to use shard locks here too
	for i, batch := range shardBatches {
		shardMutexes[i].Lock() // Lock to protect batch commit
		if batch != nil && batch.Len() > 0 {
			// Only the last batch uses Sync, others use NoSync
			commitOption := pebble.Sync
			if i == len(shardBatches)-1 {
				commitOption = pebble.Sync
			}
			if err := batch.Commit(commitOption); err != nil {
				_ = batch.Close()
				shardMutexes[i].Unlock() // Remember to unlock on error
				return fmt.Errorf("failed to commit shard %d: %w", i, err)
			}
			_ = batch.Close()
		}
		shardMutexes[i].Unlock() // Unlock after completion
	}

	return nil
}
func (s *PebbleStore) BulkMergeMapConcurrent(data *map[string][]string, concurrency int) error {
	shardCount := len(s.shards)
	type job struct {
		key   string
		value []byte
	}
	// One channel per shard
	shardChans := make([]chan job, shardCount)
	for i := range shardChans {
		shardChans[i] = make(chan job, 1024)
	}
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	// Start one goroutine per shard
	for shardIdx := range s.shards {
		wg.Add(1)
		go func(shardIdx int) {
			defer wg.Done()
			db := s.shards[shardIdx]
			batch := db.NewBatch()
			defer batch.Close()
			count := 0
			for job := range shardChans[shardIdx] {
				if err := batch.Merge([]byte(job.key), job.value, pebble.Sync); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				count++
				if count >= 1000 || batch.Len() >= maxBatchSize {
					if err := batch.Commit(pebble.Sync); err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
					batch.Reset()
					count = 0
				}
			}
			if batch.Len() > 0 {
				_ = batch.Commit(pebble.Sync)
			}
		}(shardIdx)
	}

	// Distribute tasks to respective channels
	for key, values := range *data {
		shardIdx := s.getShardIndex(key)
		valueBytes := []byte("," + strings.Join(values, ","))
		shardChans[shardIdx] <- job{
			key:   key,
			value: valueBytes,
		}
	}
	// Close all channels
	for _, ch := range shardChans {
		close(ch)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// QueryUTXOAddresses optimized version - only necessary modifications
func (s *PebbleStore) QueryUTXOAddresses(outpoints *[]string, concurrency int) (map[string][]string, error) {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	type job struct {
		key string // txid:output_index
	}

	type result struct {
		key     string
		address string
		err     error
	}

	jobsCh := make(chan job, len(*outpoints))
	resultsCh := make(chan result, len(*outpoints))

	var wg sync.WaitGroup
	// testMap := make(map[string]int)
	// testMap["66c93ec8bbb2548baba1502d6a7744271ca88e999d2e20e619168dd38898cd02"] = 1 // Special address test
	// testMap["1b158e20503c3f10fac31285308fbb44ec8b7a684a95384e6f643c3e654718f8"] = 2
	// testMap["ac7521d18f7ee7ad887832312088f64e0f4ffefbe6334b237aeb2b38c0bad2be"] = 3
	// testMap["c6b2309a4cc4b52a995cc10ed7ccc50c9842bb19c26f2824517f73185ee6ca04"] = 4
	// testMap2 := make(map[string]int)
	// Start concurrent workers
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobsCh {
				txArr := strings.Split(j.key, ":")
				if len(txArr) != 2 {
					resultsCh <- result{key: j.key, err: fmt.Errorf("invalid key format: %s", j.key)}
					continue
				}
				db := s.getShard(txArr[0])
				// if _, ok := testMap[txArr[0]]; ok {
				// 	fmt.Println("Found test transaction:", j.key)
				// 	testMap2[j.key] = 1
				// }
				value, closer, err := db.Get([]byte(txArr[0]))
				if err != nil {
					if err == pebble.ErrNotFound {
						resultsCh <- result{key: j.key, address: "", err: nil}
					} else {
						resultsCh <- result{key: j.key, err: err}
					}
					continue
				}

				// Fix 1: Immediately copy data and close resources to avoid defer accumulation in loops
				valueStr := string(append([]byte(nil), value...))

				closer.Close() // Close immediately instead of deferring

				resultsCh <- result{
					key:     j.key,
					address: valueStr,
				}
			}
		}()
	}

	// Send tasks
	go func() {
		for _, outkey := range *outpoints {
			jobsCh <- job{
				key: outkey,
			}
		}
		close(jobsCh)
	}()

	// Collect results
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make(map[string]string)
	var finalErr error

	for r := range resultsCh {
		if r.err != nil {
			finalErr = r.err
			break
		}
		if r.address != "" {
			results[r.key], _ = getAddressByStr(r.key, r.address)
			// if _, ok := testMap2[r.key]; ok {
			// 	fmt.Println("Get test transaction result:", r.key, r.address)
			// }
		}

	}
	finalResults := make(map[string][]string)
	for k, v := range results {
		// if v == "16Cxq6PZNKa5Gnw5GFrco5jkyrzbNQfHsR" {
		// 	fmt.Println("Found special address:", v, k)
		// }
		if v == "errAddress" {
			continue
		}
		finalResults[v] = append(finalResults[v], k)
	}

	// Fix 2: Optimize memory usage
	results = nil // Allow early garbage collection

	return finalResults, finalErr
}

func getAddressByStr(key, results string) (string, error) {
	info := strings.Split(key, ":")
	if len(info) != 2 {
		return "", fmt.Errorf("invalid key format: %s", key)
	}
	txIndex, err := strconv.Atoi(info[1])
	if err != nil {
		return "", fmt.Errorf("invalid index: %s", info[1])
	}
	addressInfo := strings.Split(results, ",")
	if len(addressInfo) <= txIndex+1 {
		return "", fmt.Errorf("invalid address index: %d", txIndex)
	}
	arr := strings.Split(addressInfo[txIndex+1], "@")
	if len(arr) != 2 {
		return "", fmt.Errorf("invalid address: %s", arr)
	}
	return arr[0], nil
}

// QueryUTXOAddresses optimized version - only necessary modifications
func (s *PebbleStore) QueryFtUTXOAddresses(outpoints *[]string, concurrency int, txPointUsedMap map[string]string) (map[string][]string, map[string][]string, error) {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	type job struct {
		key string // txid:output_index
	}

	type result struct {
		key       string
		valueData string
		err       error
	}

	jobsCh := make(chan job, len(*outpoints))
	resultsCh := make(chan result, len(*outpoints))

	var wg sync.WaitGroup

	// Start concurrent workers
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobsCh {
				txArr := strings.Split(j.key, ":")
				if len(txArr) != 2 {
					resultsCh <- result{key: j.key, err: fmt.Errorf("invalid key format: %s", j.key)}
					continue
				}
				db := s.getShard(txArr[0])
				value, closer, err := db.Get([]byte(txArr[0]))
				if err != nil {
					if err == pebble.ErrNotFound {
						resultsCh <- result{key: j.key, valueData: "", err: nil}
					} else {
						resultsCh <- result{key: j.key, err: err}
					}
					continue
				}

				// Fix 1: Immediately copy data and close resources to avoid defer accumulation in loops
				valueStr := string(append([]byte(nil), value...))
				closer.Close() // Close immediately instead of deferring

				resultsCh <- result{
					key:       j.key,
					valueData: valueStr,
				}
			}
		}()
	}

	// Send tasks
	go func() {
		for _, outkey := range *outpoints {
			jobsCh <- job{
				key: outkey,
			}
		}
		close(jobsCh)
	}()

	// Collect results
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make(map[string]string)
	var finalErr error

	for r := range resultsCh {
		if r.err != nil {
			finalErr = r.err
			break
		}
		if r.valueData != "" {
			//key: txid:output_index
			//value: FtAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType
			resultInfo, err := getFtAddressByStr(r.key, r.valueData)
			if err != nil {
				finalErr = err
				break
			}
			if resultInfo != "" {
				results[r.key] = resultInfo
			}
		}
	}

	finalFtResults := make(map[string][]string)
	finalUniqueResults := make(map[string][]string)
	for k, v := range results {
		kStrs := strings.Split(k, ":")
		if len(kStrs) != 2 {
			return nil, nil, fmt.Errorf("invalid kStrs: %s", kStrs)
		}
		vStrs := strings.Split(v, "@")
		if len(vStrs) != 9 {
			return nil, nil, fmt.Errorf("invalid vStrs: %s", vStrs)
		}
		usedTxId := ""
		if usedValue, ok := txPointUsedMap[kStrs[0]+":"+kStrs[1]]; ok {
			usedTxId = usedValue
		}

		if vStrs[8] == "ft" {
			// key: FtAddress
			// value: txid@index@codeHash@genesis@sensibleId@amount@value@height@usedTxId
			finalResultKey := vStrs[0] //key: FtAddress
			finalResultValue := kStrs[0] + "@" + kStrs[1] + "@" + vStrs[1] + "@" + vStrs[2] + "@" + vStrs[3] + "@" + vStrs[5] + "@" + vStrs[6] + "@" + vStrs[7] + "@" + usedTxId
			finalFtResults[finalResultKey] = append(finalFtResults[finalResultKey], finalResultValue)
		} else if vStrs[8] == "unique" {
			// key: codeHash@genesis
			// value: txid@index@usedTxId
			finalResultKey := vStrs[1] + "@" + vStrs[2] //key: codeHash@genesis
			finalResultValue := kStrs[0] + "@" + kStrs[1] + "@" + usedTxId
			finalUniqueResults[finalResultKey] = append(finalUniqueResults[finalResultKey], finalResultValue)
		}
	}

	// Fix 2: Optimize memory usage
	results = nil // Allow early garbage collection

	return finalFtResults, finalUniqueResults, finalErr
}

// FtAddress@CodeHash@Genesis@sensibleId@Amount@Index@Value@height@contractType
func getFtAddressByStr(key, results string) (string, error) {
	info := strings.Split(key, ":")
	if len(info) != 2 {
		return "", fmt.Errorf("invalid key format: %s", key)
	}
	// index := info[1]
	targetValueInfo := ""
	valueInfoList := strings.Split(results, ",")
	// fmt.Printf("[getFtAddressByStr]key: %s, results: %s\n", key, results)
	for _, valueInfo := range valueInfoList {
		arr := strings.Split(valueInfo, "@")
		if len(arr) != 9 {
			continue
		}
		if arr[5] == info[1] {
			targetValueInfo = valueInfo
			break
		}
	}
	if targetValueInfo == "" {
		// return "", fmt.Errorf("invalid targetValueInfo: %s", info[1])
		return "", nil
	}

	targetArr := strings.Split(targetValueInfo, "@")
	if len(targetArr) != 9 {
		return "", fmt.Errorf("invalid targetArr: %s", targetArr)
	}
	return targetValueInfo, nil
}

func (s *PebbleStore) QueryUTXOAddress(outpoint string) (string, error) {
	txArr := strings.Split(outpoint, ":")
	if len(txArr) != 2 {
		return "", fmt.Errorf("invalid key format: %s", outpoint)
	}

	// Get the corresponding shard DB
	db := s.getShard(txArr[0])

	// Query transaction information
	value, closer, err := db.Get([]byte(txArr[0]))
	if err != nil {
		if err == pebble.ErrNotFound {
			return "", ErrNotFound
		}
		return "", err
	}
	defer closer.Close()

	// Copy data
	valueStr := string(append([]byte(nil), value...))

	// Parse address information
	address, err := getAddressByStr(outpoint, valueStr)
	if err != nil {
		return "", err
	}

	return address, nil
}

// BulkMergeConcurrent for processing map[string]string type data
func (s *PebbleStore) BulkMergeConcurrent(data *map[string]string, concurrency int) error {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	type job struct {
		shardIdx int
		key      string
		value    []byte
	}

	jobsCh := make(chan job, len(*data))
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	shardMutexes := make([]sync.Mutex, len(s.shards))
	shardBatches := make([]*pebble.Batch, len(s.shards))

	// Initialize batch for each shard
	for i := range shardBatches {
		shardBatches[i] = s.shards[i].NewBatch()
	}

	maxBatchItems := 1000
	batchItemCounters := make([]int, len(s.shards))

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobsCh {
				db := s.shards[job.shardIdx]

				shardMutexes[job.shardIdx].Lock()
				batch := shardBatches[job.shardIdx]
				if batch == nil {
					batch = db.NewBatch()
					shardBatches[job.shardIdx] = batch
				}

				if err := batch.Merge([]byte(job.key), job.value, pebble.Sync); err != nil {
					shardMutexes[job.shardIdx].Unlock()
					select {
					case errCh <- fmt.Errorf("merge failed on shard %d: %w", job.shardIdx, err):
					default:
					}
					return
				}

				batchItemCounters[job.shardIdx]++
				if batchItemCounters[job.shardIdx] >= maxBatchItems || batch.Len() >= maxBatchSize {
					if err := batch.Commit(pebble.Sync); err != nil {
						shardMutexes[job.shardIdx].Unlock()
						select {
						case errCh <- fmt.Errorf("commit failed on shard %d: %w", job.shardIdx, err):
						default:
						}
						return
					}
					batch.Reset()
					batchItemCounters[job.shardIdx] = 0
				}

				shardMutexes[job.shardIdx].Unlock()
			}
		}()
	}

	// Send tasks
	for key, value := range *data {
		shardIdx := s.getShardIndex(key)
		valueBytes := []byte(value)
		jobsCh <- job{
			shardIdx: shardIdx,
			key:      key,
			value:    valueBytes,
		}
	}
	close(jobsCh)

	go func() {
		wg.Wait()
		close(errCh)
	}()

	if err := <-errCh; err != nil {
		return err
	}

	for i, batch := range shardBatches {
		shardMutexes[i].Lock()
		if batch != nil && batch.Len() > 0 {
			commitOption := pebble.Sync
			if i == len(shardBatches)-1 {
				commitOption = pebble.Sync
			}
			if err := batch.Commit(commitOption); err != nil {
				_ = batch.Close()
				shardMutexes[i].Unlock()
				return fmt.Errorf("failed to commit shard %d: %w", i, err)
			}
			_ = batch.Close()
		}
		shardMutexes[i].Unlock()
	}

	return nil
}

// GetShards returns all shards
func (s *PebbleStore) GetShards() []*pebble.DB {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shards
}

// BulkWriteConcurrent concurrently writes a map with many keys to corresponding shards
func (s *PebbleStore) BulkWriteConcurrent(data *map[string]string, concurrency int) error {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	// Allocate workers according to shard count
	type job struct {
		shardIdx int
		key      string
		value    []byte
	}

	jobsCh := make(chan job, len(*data))
	errCh := make(chan error, 1)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var currentBatch *pebble.Batch
			var currentShardIdx int

			for job := range jobsCh {
				db := s.shards[job.shardIdx]

				// Commit current batch when switching shard
				if currentBatch != nil && currentShardIdx != job.shardIdx {
					if err := currentBatch.Commit(pebble.Sync); err != nil {
						select {
						case errCh <- fmt.Errorf("commit failed on shard %d: %w", currentShardIdx, err):
						default:
						}
						return
					}
					currentBatch.Reset()
					currentBatch = nil
				}

				// Initialize batch
				if currentBatch == nil {
					currentBatch = db.NewBatch()
					currentShardIdx = job.shardIdx
				}

				// Write data
				if err := currentBatch.Set([]byte(job.key), job.value, nil); err != nil {
					select {
					case errCh <- fmt.Errorf("set failed on shard %d: %w", job.shardIdx, err):
					default:
					}
					return
				}

				// Control batch size
				if currentBatch.Len() > maxBatchSize {
					if err := currentBatch.Commit(pebble.Sync); err != nil {
						select {
						case errCh <- fmt.Errorf("commit failed on shard %d: %w", job.shardIdx, err):
						default:
						}
						return
					}
					currentBatch = db.NewBatch()
				}
			}

			// Commit final batch
			if currentBatch != nil {
				if err := currentBatch.Commit(pebble.Sync); err != nil {
					select {
					case errCh <- fmt.Errorf("final commit failed on shard %d: %w", currentShardIdx, err):
					default:
					}
				}
			}
		}()
	}

	// Send tasks
	for key, value := range *data {
		shardIdx := s.getShardIndex(key)
		jobsCh <- job{
			shardIdx: shardIdx,
			key:      key,
			value:    []byte(value),
		}
	}
	close(jobsCh)

	// Wait for completion
	go func() {
		wg.Wait()
	}()

	// Check for errors
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
