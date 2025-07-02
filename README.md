# HIGUN - Hyper Indexer of General UTXO Network

HIGUN is the abbreviation for **Hyper Indexer of General UTXO Network**. As a Bitcoin sidechain, MVC has been committed to expanding Bitcoin's performance. However, there has been a lack of a high-performance UTXO indexer that integrates Bitcoin and other sidechains in the market. 

This high-performance universal UTXO network indexer, jointly developed by the MVC technical team and Metalet team, will for the first time index both Bitcoin and MVC UTXOs simultaneously in a single indexer, treating these two as an integrated network. It adopts Pebble files instead of traditional databases, and a medium-configuration server can actually process tens of millions of transactions daily in performance tests.

## Current Features (v1.0)

- **Bitcoin UTXO Indexing**
- **MVC UTXO Indexing** 
- **MVC FT Indexing**
- **Block Information Indexing**

## Future Plans

Future versions will add support for more asset protocols and more Bitcoin sidechains.

## Deployment Instructions

### 1. Bitcoin UTXO Indexing

For Bitcoin UTXO indexing, use the standard Docker deployment:

### 2. MVC UTXO Indexing + Block Information Indexing

MVC UTXO indexing and block information indexing run together in the same service. Use the standard Docker deployment:

```bash
# Build the image
docker build -t utxo_indexer .

# Run the container
docker run -d \
  --name mvc-utxo-indexer \
  --network=host \
  --restart=always \
  -m 10g \
  -v /your/path/utxo_indexer:/app/utxo_indexer \
  -v /your/path/config.yaml:/app/config.yaml \
  -v /your/path/data:/app/data \
  -v /your/path/blockinfo_data:/app/blockinfo_data \
  utxo_indexer
```

### 3. MVC FT Indexing

For MVC FT (Fungible Token) indexing, use the dedicated deployment script:

```bash
# Navigate to the deploy directory
cd deploy/

# Run the FT deployment script
./deploy.ft.sh
```

The FT deployment script will:
- Stop and remove existing containers
- Build the FT-specific Docker image
- Start the FT indexer service
- Display service status

**FT Indexer Details:**
- Container name: `ft-utxo-indexer`
- Port: `7789`
- Configuration: Uses `config_mvc_ft.yaml` by default
- Resource limits: 4 CPU cores, 8GB memory

## Configuration Files

### Standard Configuration (`config.yaml`)

Used for Bitcoin UTXO indexing, MVC UTXO indexing, and block information indexing (MVC UTXO and block information run together):

```yaml
# UTXO Indexer Configuration
network: "mainnet" # mainnet/testnet/regtest
data_dir: "/Volumes/MyData/mvc/test"
shard_count: 2
tx_concurrency: 64 # How many concurrent requests to fetch raw transactions from nodes
workers: 4
batch_size: 20000
cpu_cores: 1 # 4-core CPU
memory_gb: 4 # 64GB memory
high_perf: true # Prefer performance
api_port: "8080"
zmq_address: "tcp://localhost:28336"  # ZeroMQ connection address
mempool_clean_start_height: 567 # Mempool cleaning start height, avoid cleaning from the genesis block
max_tx_per_batch: 30000

# Bitcoin RPC Configuration
rpc:
  chain: "mvc"
  host: "localhost"
  port: "9882"
  user: ""
  password: ""
```

### FT Configuration (`config_mvc_ft.yaml`)

Used for MVC FT indexing:

```yaml
# MVC MetaContract FT UTXO Indexer Configuration
network: "mainnet"  # mainnet/testnet/regtest
data_dir: "data/mainnet"
shard_count: 2
cpu_cores: 4      # CPU cores to use
memory_gb: 8      # Memory allocation in GB
high_perf: true    # Performance optimization flag
api_port: "7789"
zmq_address: "tcp://localhost:28333"  # ZeroMQ connection address
mempool_clean_start_height: 567   # Mempool cleaning start height, avoid cleaning from the genesis block
max_tx_per_batch: 1000
raw_tx_in_block: true

# MVC RPC Configuration
rpc:
  chain: "mvc"
  host: "localhost"
  port: "9882"
  user: ""
  password: ""
```

## Configuration Parameters

### General Parameters

- **network**: Network type (`mainnet`/`testnet`/`regtest`)
- **data_dir**: Data storage directory
- **shard_count**: Number of database shards for performance optimization
- **cpu_cores**: Number of CPU cores to use
- **memory_gb**: Memory allocation in GB
- **high_perf**: Performance optimization flag
- **api_port**: API service port
- **zmq_address**: ZeroMQ connection address for real-time transaction monitoring
- **mempool_clean_start_height**: Starting block height for mempool cleaning
- **max_tx_per_batch**: Maximum transactions per batch for processing
- **raw_tx_in_block**: Enable raw transaction processing in blocks (FT specific)

### RPC Configuration

- **chain**: Blockchain type (`mvc` for MVC, `btc` for Bitcoin)
- **host**: RPC server host address
- **port**: RPC server port
- **user**: RPC authentication username
- **password**: RPC authentication password

## Service Management

### View Logs

```bash
# Standard indexer logs
docker logs -f mvc-utxo-indexer

# FT indexer logs
docker logs -f ft-utxo-indexer
# Or use docker-compose
docker-compose -f deploy/docker-compose.ft.yml logs -f
```

### Stop Services

```bash
# Stop standard indexer
docker stop mvc-utxo-indexer

# Stop FT indexer
docker stop ft-utxo-indexer
# Or use docker-compose
docker-compose -f deploy/docker-compose.ft.yml down
```

### Restart Services

```bash
# Restart standard indexer
docker restart mvc-utxo-indexer

# Restart FT indexer
docker restart ft-utxo-indexer
# Or use docker-compose
docker-compose -f deploy/docker-compose.ft.yml restart
```
