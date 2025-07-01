package blockchain

import (
	"encoding/json"
	"fmt"
)

type MempoolInfo struct {
	Size  int64 `json:"size"`
	Bytes int64 `json:"bytes"`
	// You can add more fields as needed
}
type ChainStatus struct {
	Chain                string `json:"chain"`
	Blocks               int64  `json:"blocks"`
	Headers              int64  `json:"headers"`
	BestBlockHash        string `json:"bestBlockHash"`
	Difficulty           string `json:"difficulty"`
	MedianTime           int64  `json:"medianTime"`
	Chainwork            string `json:"chainwork"`
	NetworkHashPerSecond string `json:"networkHashPerSecond"`
	MempoolTxCount       int64  `json:"mempoolTxCount"`
	MempoolUsage         int64  `json:"mempoolUsage"`
	EstimatedBlockSize   int64  `json:"estimatedBlockSize"`
}
type SimpleBlockChainInfo struct {
	Chain         string  `json:"chain"`
	Blocks        int64   `json:"blocks"`
	Headers       int64   `json:"headers"`
	BestBlockHash string  `json:"bestblockhash"`
	Difficulty    float64 `json:"difficulty"`
	MedianTime    int64   `json:"mediantime"`
	Chainwork     string  `json:"chainwork"`
	// Don't include softforks field
}

func (c *Client) GetChainStatus() (*ChainStatus, error) {
	// 1. getblockchaininfo
	var blockchainInfo SimpleBlockChainInfo
	resp, err := c.rpcClient.RawRequest("getblockchaininfo", []json.RawMessage{})
	if err != nil {
		return nil, fmt.Errorf("getblockchaininfo: %w", err)
	}
	if err := json.Unmarshal(resp, &blockchainInfo); err != nil {
		return nil, fmt.Errorf("unmarshal getblockchaininfo: %w", err)
	}

	// 2. getnetworkhashps
	networkHashPS, err := c.rpcClient.GetNetworkHashPS()
	if err != nil {
		return nil, fmt.Errorf("getnetworkhashps: %w", err)
	}

	// 3. getmempoolinfo
	mempoolInfo, err := c.GetMempoolInfo()
	if err != nil {
		return nil, fmt.Errorf("getmempoolinfo: %w", err)
	}

	// 4. Estimate block size (using the size of the latest block)
	bestBlockHash, err := c.rpcClient.GetBestBlockHash()
	if err != nil {
		return nil, fmt.Errorf("getbestblockhash: %w", err)
	}
	blockVerbose, err := c.rpcClient.GetBlockVerbose(bestBlockHash)
	estimatedBlockSize := int64(0)
	if err == nil {
		estimatedBlockSize = int64(blockVerbose.Size)
	}

	return &ChainStatus{
		Chain:                blockchainInfo.Chain,
		Blocks:               int64(blockchainInfo.Blocks),
		Headers:              int64(blockchainInfo.Headers),
		BestBlockHash:        blockchainInfo.BestBlockHash,
		Difficulty:           fmt.Sprintf("%.4f", blockchainInfo.Difficulty),
		MedianTime:           blockchainInfo.MedianTime,
		Chainwork:            blockchainInfo.Chainwork,
		NetworkHashPerSecond: fmt.Sprintf("%.0f", networkHashPS),
		MempoolTxCount:       mempoolInfo.Size,
		MempoolUsage:         mempoolInfo.Bytes,
		EstimatedBlockSize:   estimatedBlockSize,
	}, nil
}
func (c *Client) GetMempoolInfo() (*MempoolInfo, error) {
	resp, err := c.rpcClient.RawRequest("getmempoolinfo", []json.RawMessage{})
	if err != nil {
		return nil, err
	}
	var info MempoolInfo
	if err := json.Unmarshal(resp, &info); err != nil {
		return nil, err
	}
	return &info, nil
}
