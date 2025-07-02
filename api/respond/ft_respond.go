package respond

import (
	ft "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"
)

// FtBalanceResponse FT balance response
type FtBalanceResponse struct {
	Balances []*ft.FtBalance `json:"balances"`
}

// FtUTXOsResponse FT UTXO list response
type FtUTXOsResponse struct {
	Address string       `json:"address"`
	UTXOs   []*ft.FtUTXO `json:"utxos"`
	Count   int          `json:"count"`
}

// FtUtxoByTxResponse FT UTXO by tx response
type FtUtxoByTxResponse struct {
	UTXOs string `json:"utxos"`
}

// FtIncomeResponse FT income response
type FtIncomeResponse struct {
	Income []string `json:"income"`
}

// FtSpendResponse FT spend response
type FtSpendResponse struct {
	Spend []string `json:"spend"`
}

// FtMempoolUTXOsResponse FT mempool UTXO response
type FtMempoolUTXOsResponse struct {
	Address string       `json:"address"`
	Income  []*ft.FtUTXO `json:"income"`
	Spend   []*ft.FtUTXO `json:"spend"`
	Count   int          `json:"count"`
}

// FtIncomeValidResponse FT valid income response
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

// FtAllIncomeResponse All FT income response
type FtAllIncomeResponse struct {
	IncomeData map[string]string `json:"income_data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination"`
}

// FtAllSpendResponse All FT spend response
type FtAllSpendResponse struct {
	SpendData  map[string]string `json:"spend_data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination"`
}

// FtInfoResponse FT info response
type FtInfoResponse struct {
	CodeHash   string `json:"codeHash"`
	Genesis    string `json:"genesis"`
	SensibleId string `json:"sensibleId"`
	Name       string `json:"name"`
	Symbol     string `json:"symbol"`
	Decimal    uint8  `json:"decimal"`
}

// FtUncheckOutpointResponse Used to return unchecked FT outpoint data
type FtUncheckOutpointResponse struct {
	Data       map[string]string `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtUncheckOutpointTotalResponse Used to return total count of unchecked FT outpoints
type FtUncheckOutpointTotalResponse struct {
	Total int64 `json:"total"`
}

// FtGenesisResponse Used to return FT Genesis data
type FtGenesisResponse struct {
	Data       map[string]string `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtGenesisOutputResponse Used to return FT Genesis Output data
type FtGenesisOutputResponse struct {
	Data       map[string][]string `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtUsedIncomeResponse Used to return used FT income data
type FtUsedIncomeResponse struct {
	Data       map[string]string `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtGenesisUtxo Used to store detailed information of FT Genesis UTXO
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

// FtGenesisUtxoResponse Used to return FT Genesis UTXO data
type FtGenesisUtxoResponse struct {
	Data       map[string]*FtGenesisUtxo `json:"data"`
	Pagination struct {
		CurrentPage int `json:"current_page"`
		PageSize    int `json:"page_size"`
		Total       int `json:"total"`
		TotalPages  int `json:"total_pages"`
	} `json:"pagination,omitempty"`
}

// FtUniqueUTXOsResponse Used to return unique FT UTXO list
type FtUniqueUTXOsResponse struct {
	UTXOs []*ft.UniqueFtUtxo `json:"utxos"`
	Count int                `json:"count"`
}

// FtAddressFtIncomeMapResponse FT address income data response
type FtAddressFtIncomeMapResponse struct {
	Address   string            `json:"address"`
	IncomeMap map[string]string `json:"incomeMap"`
}

// FtAddressFtIncomeValidMapResponse FT address valid income data response
type FtAddressFtIncomeValidMapResponse struct {
	Address        string            `json:"address"`
	IncomeValidMap map[string]string `json:"incomeValidMap"`
}

// FtSpendMapResponse FT spend data response
type FtSpendMapResponse struct {
	Address  string            `json:"address"`
	SpendMap map[string]string `json:"spendMap"`
}

// FtUniqueSpendMapResponse Unique FT spend data response
type FtUniqueSpendMapResponse struct {
	CodeHashGenesis string            `json:"codeHashGenesis"`
	SpendMap        map[string]string `json:"spendMap"`
}
