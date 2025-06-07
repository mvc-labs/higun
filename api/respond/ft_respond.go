package respond

import (
	ft "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"
)

// FtBalanceResponse FT余额响应
type FtBalanceResponse struct {
	Balances []*ft.FtBalance `json:"balances"`
}

// FtUTXOsResponse FT UTXO列表响应
type FtUTXOsResponse struct {
	Address string       `json:"address"`
	UTXOs   []*ft.FtUTXO `json:"utxos"`
	Count   int          `json:"count"`
}

// FtUtxoByTxResponse FT UTXO by tx响应
type FtUtxoByTxResponse struct {
	UTXOs string `json:"utxos"`
}

// FtIncomeResponse FT收入响应
type FtIncomeResponse struct {
	Income []string `json:"income"`
}

// FtSpendResponse FT支出响应
type FtSpendResponse struct {
	Spend []string `json:"spend"`
}

// FtMempoolUTXOsResponse FT内存池UTXO响应
type FtMempoolUTXOsResponse struct {
	Address string       `json:"address"`
	Income  []*ft.FtUTXO `json:"income"`
	Spend   []*ft.FtUTXO `json:"spend"`
	Count   int          `json:"count"`
}

// FtIncomeValidResponse FT有效收入响应
type FtIncomeValidResponse struct {
	Address    string   `json:"address"`
	IncomeData []string `json:"income_data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination"`
}

// FtAllIncomeResponse 所有FT收入响应
type FtAllIncomeResponse struct {
	IncomeData map[string]string `json:"income_data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination"`
}

// FtAllSpendResponse 所有FT支出响应
type FtAllSpendResponse struct {
	SpendData  map[string]string `json:"spend_data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination"`
}

// FtInfoResponse FT信息响应
type FtInfoResponse struct {
	CodeHash   string `json:"codeHash"`
	Genesis    string `json:"genesis"`
	SensibleId string `json:"sensibleId"`
	Name       string `json:"name"`
	Symbol     string `json:"symbol"`
	Decimal    uint8  `json:"decimal"`
}

// FtUncheckOutpointResponse 用于返回未检查的FT outpoint数据
type FtUncheckOutpointResponse struct {
	Data       map[string]string `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtUncheckOutpointTotalResponse 用于返回未检查的FT outpoint总数量
type FtUncheckOutpointTotalResponse struct {
	Total int64 `json:"total"`
}

// FtGenesisResponse 用于返回 FT Genesis 数据
type FtGenesisResponse struct {
	Data       map[string]string `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtGenesisOutputResponse 用于返回 FT Genesis Output 数据
type FtGenesisOutputResponse struct {
	Data       map[string][]string `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtUsedIncomeResponse 用于返回已使用的 FT 收入数据
type FtUsedIncomeResponse struct {
	Data       map[string]string `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtGenesisUtxo 用于存储 FT Genesis UTXO 的详细信息
type FtGenesisUtxo struct {
	SensibleId string `json:"sensible_id"`
	Name       string `json:"name"`
	Symbol     string `json:"symbol"`
	Decimal    uint8  `json:"decimal"`
	CodeHash   string `json:"code_hash"`
	Genesis    string `json:"genesis"`
	Amount     string `json:"amount"`
	Index      string `json:"index"`
	Value      string `json:"value"`
	IsSpent    bool   `json:"is_spent"`
}

// FtGenesisUtxoResponse 用于返回 FT Genesis UTXO 数据
type FtGenesisUtxoResponse struct {
	Data       map[string]*FtGenesisUtxo `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtUniqueUTXOsResponse 用于返回唯一的 FT UTXO 列表
type FtUniqueUTXOsResponse struct {
	UTXOs []*ft.UniqueFtUtxo `json:"utxos"`
	Count int                `json:"count"`
}
