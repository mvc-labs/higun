package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/btcsuite/btcd/chaincfg"
	"gopkg.in/yaml.v3"
)

type RPCConfig struct {
	Chain    string `yaml:"chain"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

var GlobalConfig *Config
var GlobalNetwork *chaincfg.Params

type Config struct {
	Network                 string    `yaml:"network"`
	DataDir                 string    `yaml:"data_dir"`
	ShardCount              int       `yaml:"shard_count"`
	CPUCores                int       `yaml:"cpu_cores"`
	MemoryGB                int       `yaml:"memory_gb"`
	HighPerf                bool      `yaml:"high_perf"`
	APIPort                 string    `yaml:"api_port"`
	ZMQAddress              string    `yaml:"zmq_address"`
	MemPoolCleanStartHeight int       `yaml:"mempool_clean_start_height"`
	MaxTxPerBatch           int       `yaml:"max_tx_per_batch"`
	RPC                     RPCConfig `yaml:"rpc"`
}

func (c *Config) GetChainParams() (*chaincfg.Params, error) {
	switch c.Network {
	case "mainnet":
		return &chaincfg.MainNetParams, nil
	case "testnet":
		return &chaincfg.TestNet3Params, nil
	case "regtest":
		return &chaincfg.RegressionNetParams, nil
	default:
		return nil, fmt.Errorf("unknown network: %s", c.Network)
	}
}

func LoadConfig() (*Config, error) {
	configFlag := flag.String("config", "", "path to config file")
	flag.Parse()
	// Default config
	cfg := &Config{
		Network:                 "testnet",
		DataDir:                 "data",
		ShardCount:              16,
		APIPort:                 "8080",
		ZMQAddress:              "tcp://localhost:28332",
		MemPoolCleanStartHeight: 0,    // 默认从0开始清理
		MaxTxPerBatch:           3000, // 默认每批次最多处理3000个交易
		RPC: RPCConfig{
			Host: "localhost",
			Port: "8332",
		},
	}

	// Try to load from config file
	configPath := *configFlag
	if configPath == "" {
		configPath = "config.yaml"
	}

	if _, err := os.Stat(configPath); err == nil {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	// Override with environment variables if set
	if network := os.Getenv("NETWORK"); network != "" {
		cfg.Network = network
	}
	if dir := os.Getenv("DATA_DIR"); dir != "" {
		cfg.DataDir = dir
	}
	if user := os.Getenv("RPC_USER"); user != "" {
		cfg.RPC.User = user
	}
	if pass := os.Getenv("RPC_PASS"); pass != "" {
		cfg.RPC.Password = pass
	}
	if host := os.Getenv("RPC_HOST"); host != "" {
		cfg.RPC.Host = host
	}
	if port := os.Getenv("RPC_PORT"); port != "" {
		cfg.RPC.Port = port
	}
	if zmq := os.Getenv("ZMQ_ADDRESS"); zmq != "" {
		cfg.ZMQAddress = zmq
	}
	if startHeight := os.Getenv("MEMPOOL_CLEAN_START_HEIGHT"); startHeight != "" {
		height, err := strconv.Atoi(startHeight)
		if err == nil && height >= 0 {
			cfg.MemPoolCleanStartHeight = height
		}
	}
	if maxTxPerBatch := os.Getenv("MAX_TX_PER_BATCH"); maxTxPerBatch != "" {
		val, err := strconv.Atoi(maxTxPerBatch)
		if err == nil && val > 0 {
			cfg.MaxTxPerBatch = val
		}
	}

	// Ensure data dir exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	return cfg, nil
}
