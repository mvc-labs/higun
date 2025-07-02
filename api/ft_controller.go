package api

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/metaid/utxo_indexer/api/respond"
	ft "github.com/metaid/utxo_indexer/indexer/contract/meta-contract-ft"
	"github.com/metaid/utxo_indexer/storage"
)

func (s *FtServer) getFtBalance(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	balances, err := s.indexer.GetFtBalance(address, codeHash, genesis)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtBalanceResponse{
		Balances: balances,
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getFtUTXOs(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	utxos, err := s.indexer.GetFtUTXOs(address, codeHash, genesis)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUTXOsResponse{
		Address: address,
		UTXOs:   utxos,
		Count:   len(utxos),
	}, time.Now().UnixMilli()-startTime))
}

// DB
func (s *FtServer) getDbFtUtxoByTx(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	tx := c.Query("tx")
	if tx == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("tx parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	utxos, err := s.indexer.GetDbFtUtxoByTx(tx)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUtxoByTxResponse{
		UTXOs: string(utxos),
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getDbFtIncomeByAddress(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	income, err := s.indexer.GetDbAddressFtIncome(address, codeHash, genesis)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtIncomeResponse{
		Income: income,
	}, time.Now().UnixMilli()-startTime))
}

// db
func (s *FtServer) getDbFtSpendByAddress(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	spend, err := s.indexer.GetDbAddressFtSpend(address, codeHash, genesis)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtSpendResponse{
		Spend: spend,
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getFtMempoolUTXOs(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	income, spend, err := s.indexer.GetMempoolFtUTXOs(address, codeHash, genesis)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// Convert common.FtUtxo to ft.FtUTXO
	incomeUTXOs := make([]*ft.FtUTXO, 0, len(income))
	for _, utxo := range income {
		txIndex, err := strconv.ParseInt(utxo.Index, 10, 64)
		if err != nil {
			continue
		}
		incomeUTXOs = append(incomeUTXOs, &ft.FtUTXO{
			CodeHash:      utxo.CodeHash,
			Genesis:       utxo.Genesis,
			Name:          utxo.Name,
			Symbol:        utxo.Symbol,
			SensibleId:    utxo.SensibleId,
			Decimal:       uint8(0), // Need to get from elsewhere
			Txid:          utxo.TxID,
			TxIndex:       txIndex,
			ValueString:   utxo.Amount,
			SatoshiString: utxo.Value,
			Value:         0, // Need to convert
			Satoshi:       0, // Need to convert
			Height:        0,
			Address:       utxo.Address,
			Flag:          "unconfirmed",
		})
	}

	spendUTXOs := make([]*ft.FtUTXO, 0, len(spend))
	for _, utxo := range spend {
		txIndex, err := strconv.ParseInt(utxo.Index, 10, 64)
		if err != nil {
			continue
		}
		spendUTXOs = append(spendUTXOs, &ft.FtUTXO{
			CodeHash:      utxo.CodeHash,
			Genesis:       utxo.Genesis,
			Name:          utxo.Name,
			Symbol:        utxo.Symbol,
			SensibleId:    utxo.SensibleId,
			Decimal:       uint8(0), // Need to get from elsewhere
			Txid:          utxo.TxID,
			TxIndex:       txIndex,
			ValueString:   utxo.Amount,
			SatoshiString: utxo.Value,
			Value:         0, // Need to convert
			Satoshi:       0, // Need to convert
			Height:        0,
			Address:       utxo.Address,
			Flag:          "unconfirmed",
		})
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtMempoolUTXOsResponse{
		Address: address,
		Income:  incomeUTXOs,
		Spend:   spendUTXOs,
		Count:   len(incomeUTXOs) + len(spendUTXOs),
	}, time.Now().UnixMilli()-startTime))
}

// getAllFtIncome gets FT income data for all addresses
func (s *FtServer) getDbAllFtIncome(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	incomeData, err := s.indexer.GetAllDbAddressFtIncome()
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	if incomeData == nil {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtAllIncomeResponse{
			IncomeData: make(map[string]string),
			Pagination: struct {
				CurrentPage int `json:"current_page"`
				PageSize    int `json:"page_size"`
				Total       int `json:"total"`
				TotalPages  int `json:"total_pages"`
			}{
				CurrentPage: page,
				PageSize:    pageSize,
				Total:       0,
				TotalPages:  0,
			},
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// Calculate pagination
	total := len(incomeData)
	totalPages := (total + pageSize - 1) / pageSize

	// Get current page data
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// Extract current page data
	currentPageData := make(map[string]string)
	keys := make([]string, 0, len(incomeData))
	for k := range incomeData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := start; i < end && i < len(keys); i++ {
		currentPageData[keys[i]] = incomeData[keys[i]]
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtAllIncomeResponse{
		IncomeData: currentPageData,
		Pagination: struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

// getAllFtSpend gets FT spend data for all addresses
func (s *FtServer) getDbAllFtSpend(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	spendData, err := s.indexer.GetAllDbAddressFtSpend()
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	if spendData == nil {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtAllSpendResponse{
			SpendData: make(map[string]string),
			Pagination: struct {
				CurrentPage int `json:"current_page"`
				PageSize    int `json:"page_size"`
				Total       int `json:"total"`
				TotalPages  int `json:"total_pages"`
			}{
				CurrentPage: page,
				PageSize:    pageSize,
				Total:       0,
				TotalPages:  0,
			},
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// Calculate pagination
	total := len(spendData)
	totalPages := (total + pageSize - 1) / pageSize

	// Get current page data
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// Extract current page data
	currentPageData := make(map[string]string)
	keys := make([]string, 0, len(spendData))
	for k := range spendData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := start; i < end && i < len(keys); i++ {
		currentPageData[keys[i]] = spendData[keys[i]]
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtAllSpendResponse{
		SpendData: currentPageData,
		Pagination: struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

// getAddressFtIncome gets FT income data for specified address
func (s *FtServer) getDbAddressFtIncome(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	incomeData, err := s.indexer.GetDbAddressFtIncome(address, codeHash, genesis)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtIncomeResponse{
				Income: []string{},
			}, time.Now().UnixMilli()-startTime))
			return
		}
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtIncomeResponse{
		Income: incomeData,
	}, time.Now().UnixMilli()-startTime))
}

// getAddressFtSpend gets FT spend data for specified address
func (s *FtServer) getDbAddressFtSpend(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	spendData, err := s.indexer.GetDbAddressFtSpend(address, codeHash, genesis)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtSpendResponse{
				Spend: []string{},
			}, time.Now().UnixMilli()-startTime))
			return
		}
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtSpendResponse{
		Spend: spendData,
	}, time.Now().UnixMilli()-startTime))
}

// getFtInfo gets FT information
func (s *FtServer) getDbFtInfo(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	if codeHash == "" || genesis == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("codeHash and genesis parameters are required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	// Build query key
	key := codeHash + "@" + genesis

	// Get FT information
	ftInfo, err := s.indexer.GetFtInfo(key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtInfoResponse{
				CodeHash: codeHash,
				Genesis:  genesis,
			}, time.Now().UnixMilli()-startTime))
			return
		}
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	response := respond.FtInfoResponse{
		CodeHash:   ftInfo.CodeHash,
		Genesis:    ftInfo.Genesis,
		SensibleId: ftInfo.SensibleId,
		Name:       ftInfo.Name,
		Symbol:     ftInfo.Symbol,
		Decimal:    ftInfo.Decimal,
	}
	c.JSONP(http.StatusOK, respond.RespSuccess(response, time.Now().UnixMilli()-startTime))
}

// getDbAddressFtIncomeValid gets valid FT income data for specified address
func (s *FtServer) getDbAddressFtIncomeValid(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address parameter is required"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	incomeData, err := s.indexer.GetDbAddressFtIncomeValid(address, codeHash, genesis)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtIncomeValidResponse{
				Address:    address,
				IncomeData: []string{},
				Pagination: struct {
					CurrentPage int `json:"current_page"`
					PageSize    int `json:"page_size"`
					Total       int `json:"total"`
					TotalPages  int `json:"total_pages"`
				}{
					CurrentPage: page,
					PageSize:    pageSize,
					Total:       0,
					TotalPages:  0,
				},
			}, time.Now().UnixMilli()-startTime))
			return
		}
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// Calculate pagination
	total := len(incomeData)
	totalPages := (total + pageSize - 1) / pageSize

	// Get current page data
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// Extract current page data
	var currentPageData []string
	if start < total {
		currentPageData = incomeData[start:end]
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtIncomeValidResponse{
		Address:    address,
		IncomeData: currentPageData,
		Pagination: struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getAllDbUncheckFtOutpoint(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	// Get outpoint parameter
	outpoint := c.Query("outpoint")

	// Get pagination parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// Get data
	data, err := s.indexer.GetAllDbUncheckFtOutpoint(outpoint)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// If outpoint is provided, return result directly
	if outpoint != "" {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUncheckOutpointResponse{
			Data: data,
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// Calculate pagination
	total := len(data)
	totalPages := (total + pageSize - 1) / pageSize

	// Get current page data
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// Extract current page data
	currentPageData := make(map[string]string)
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := start; i < end && i < len(keys); i++ {
		currentPageData[keys[i]] = data[keys[i]]
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUncheckOutpointResponse{
		Data: currentPageData,
		Pagination: struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getAllDbFtGenesis(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	// Get key parameter
	key := c.Query("key")

	// Get pagination parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// Get data
	data, err := s.indexer.GetAllDbFtGenesis(key)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// If key is provided, return result directly
	if key != "" {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtGenesisResponse{
			Data: data,
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// Calculate pagination
	total := len(data)
	totalPages := (total + pageSize - 1) / pageSize

	// Get current page data
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// Extract current page data
	currentPageData := make(map[string]string)
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := start; i < end && i < len(keys); i++ {
		currentPageData[keys[i]] = data[keys[i]]
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtGenesisResponse{
		Data: currentPageData,
		Pagination: struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getAllDbFtGenesisOutput(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	// Get key parameter
	key := c.Query("key")

	// Get pagination parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// Get data
	data, err := s.indexer.GetAllDbFtGenesisOutput(key)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// If key is provided, return result directly
	if key != "" {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtGenesisOutputResponse{
			Data: data,
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// Calculate pagination
	total := len(data)
	totalPages := (total + pageSize - 1) / pageSize

	// Get current page data
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// Extract current page data
	currentPageData := make(map[string][]string)
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := start; i < end && i < len(keys); i++ {
		currentPageData[keys[i]] = data[keys[i]]
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtGenesisOutputResponse{
		Data: currentPageData,
		Pagination: struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getAllDbUsedFtIncome(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	// Get key parameter
	txId := c.Query("txId")

	// Get pagination parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// Get data
	data, err := s.indexer.GetAllDbUsedFtIncome(txId)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// If key is provided, return result directly
	if txId != "" {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUsedIncomeResponse{
			Data: data,
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// Calculate pagination
	total := len(data)
	totalPages := (total + pageSize - 1) / pageSize

	// Get current page data
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// Extract current page data
	currentPageData := make(map[string]string)
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := start; i < end && i < len(keys); i++ {
		currentPageData[keys[i]] = data[keys[i]]
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUsedIncomeResponse{
		Data: currentPageData,
		Pagination: struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getAllDbFtGenesisUtxo(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	// Get key parameter
	key := c.Query("key")

	// Get pagination parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// Get data
	data, err := s.indexer.GetAllDbFtGenesisUtxo(key)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// Parse data
	parsedData := make(map[string]*respond.FtGenesisUtxo)
	for outpoint, value := range data {
		parts := strings.Split(value, "@")
		if len(parts) < 9 {
			continue
		}

		// Parse decimal
		decimal, _ := strconv.ParseUint(parts[3], 10, 8)

		// Check if there's an IsSpent flag
		isSpent := false
		if len(parts) > 9 {
			isSpent = parts[9] == "1"
		}

		parsedData[outpoint] = &respond.FtGenesisUtxo{
			SensibleId: parts[0],
			Name:       parts[1],
			Symbol:     parts[2],
			Decimal:    uint8(decimal),
			CodeHash:   parts[4],
			Genesis:    parts[5],
			Amount:     parts[6],
			Index:      parts[7],
			Value:      parts[8],
			IsSpent:    isSpent,
		}
	}

	// If key is provided, return result directly
	if key != "" {
		if utxo, exists := parsedData[key]; exists {
			c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtGenesisUtxoResponse{
				Data: map[string]*respond.FtGenesisUtxo{key: utxo},
			}, time.Now().UnixMilli()-startTime))
		} else {
			c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtGenesisUtxoResponse{
				Data: make(map[string]*respond.FtGenesisUtxo),
			}, time.Now().UnixMilli()-startTime))
		}
		return
	}

	// Calculate pagination
	total := len(parsedData)
	totalPages := (total + pageSize - 1) / pageSize

	// Get current page data
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// Extract current page data
	currentPageData := make(map[string]*respond.FtGenesisUtxo)
	keys := make([]string, 0, len(parsedData))
	for k := range parsedData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := start; i < end && i < len(keys); i++ {
		currentPageData[keys[i]] = parsedData[keys[i]]
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtGenesisUtxoResponse{
		Data: currentPageData,
		Pagination: struct {
			CurrentPage int `json:"current_page"`
			PageSize    int `json:"page_size"`
			Total       int `json:"total"`
			TotalPages  int `json:"total_pages"`
		}{
			CurrentPage: page,
			PageSize:    pageSize,
			Total:       total,
			TotalPages:  totalPages,
		},
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getUncheckFtOutpointTotal(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	// Get total count
	total, err := s.indexer.GetUncheckFtOutpointTotal()
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUncheckOutpointTotalResponse{
		Total: total,
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getUniqueFtUTXOs(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	utxos, err := s.indexer.GetUniqueFtUTXOs(codeHash, genesis)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUniqueUTXOsResponse{
		UTXOs: utxos,
		Count: len(utxos),
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getMempoolAddressFtSpendMap(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")

	spendMap, err := s.indexer.GetMempoolAddressFtSpendMap(address)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtSpendMapResponse{
		Address:  address,
		SpendMap: spendMap,
	}, time.Now().UnixMilli()-startTime))
}

func (s *FtServer) getMempoolUniqueFtSpendMap(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	codeHashGenesis := c.Query("codeHashGenesis")

	spendMap, err := s.indexer.GetMempoolUniqueFtSpendMap(codeHashGenesis)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUniqueSpendMapResponse{
		CodeHashGenesis: codeHashGenesis,
		SpendMap:        spendMap,
	}, time.Now().UnixMilli()-startTime))
}

// getMempoolAddressFtIncomeMap gets FT income data for all addresses in mempool
func (s *FtServer) getMempoolAddressFtIncomeMap(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	incomeMap := s.mempoolMgr.GetMempoolAddressFtIncomeMap()

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtAddressFtIncomeMapResponse{
		Address:   "",
		IncomeMap: incomeMap,
	}, time.Now().UnixMilli()-startTime))
}

// getMempoolAddressFtIncomeValidMap gets valid FT income data for all addresses in mempool
func (s *FtServer) getMempoolAddressFtIncomeValidMap(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	// Get data
	incomeValidMap := s.mempoolMgr.GetMempoolAddressFtIncomeValidMap()

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtAddressFtIncomeValidMapResponse{
		Address:        "",
		IncomeValidMap: incomeValidMap,
	}, time.Now().UnixMilli()-startTime))
}
