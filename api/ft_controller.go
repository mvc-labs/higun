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
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address 参数是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
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
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address 参数是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
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
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("tx 参数是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
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
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address 参数是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
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
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address 参数是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
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
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address 参数是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	income, spend, err := s.indexer.GetMempoolFtUTXOs(address, codeHash, genesis)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 转换 common.FtUtxo 到 ft.FtUTXO
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
			Decimal:       uint8(0), // 需要从其他地方获取
			Txid:          utxo.TxID,
			TxIndex:       txIndex,
			ValueString:   utxo.Amount,
			SatoshiString: utxo.Value,
			Value:         0, // 需要转换
			Satoshi:       0, // 需要转换
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
			Decimal:       uint8(0), // 需要从其他地方获取
			Txid:          utxo.TxID,
			TxIndex:       txIndex,
			ValueString:   utxo.Amount,
			SatoshiString: utxo.Value,
			Value:         0, // 需要转换
			Satoshi:       0, // 需要转换
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

// getAllFtIncome 获取所有地址的 FT 收入数据
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

	// 计算分页
	total := len(incomeData)
	totalPages := (total + pageSize - 1) / pageSize

	// 获取当前页的数据
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// 提取当前页的数据
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

// getAllFtSpend 获取所有地址的 FT 支出数据
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

	// 计算分页
	total := len(spendData)
	totalPages := (total + pageSize - 1) / pageSize

	// 获取当前页的数据
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// 提取当前页的数据
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

// getAddressFtIncome 获取指定地址的 FT 收入数据
func (s *FtServer) getDbAddressFtIncome(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address 参数是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
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

// getAddressFtSpend 获取指定地址的 FT 支出数据
func (s *FtServer) getDbAddressFtSpend(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address 参数是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
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

// getFtInfo 获取 FT 信息
func (s *FtServer) getDbFtInfo(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	codeHash := c.Query("codeHash")
	genesis := c.Query("genesis")

	if codeHash == "" || genesis == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("codeHash 和 genesis 参数都是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
		return
	}

	// 构建查询键
	key := codeHash + "@" + genesis

	// 获取 FT 信息
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

// getDbAddressFtIncomeValid 获取指定地址的有效 FT 收入数据
func (s *FtServer) getDbAddressFtIncomeValid(c *gin.Context) {
	startTime := time.Now().UnixMilli()
	address := c.Query("address")
	if address == "" {
		c.JSONP(http.StatusBadRequest, respond.RespErr(errors.New("address 参数是必须的"), time.Now().UnixMilli()-startTime, http.StatusBadRequest))
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

	// 计算分页
	total := len(incomeData)
	totalPages := (total + pageSize - 1) / pageSize

	// 获取当前页的数据
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// 提取当前页的数据
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

	// 获取 outpoint 参数
	outpoint := c.Query("outpoint")

	// 获取分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 获取数据
	data, err := s.indexer.GetAllDbUncheckFtOutpoint(outpoint)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 如果提供了 outpoint，直接返回结果
	if outpoint != "" {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUncheckOutpointResponse{
			Data: data,
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// 计算分页
	total := len(data)
	totalPages := (total + pageSize - 1) / pageSize

	// 获取当前页的数据
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// 提取当前页的数据
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

	// 获取 key 参数
	key := c.Query("key")

	// 获取分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 获取数据
	data, err := s.indexer.GetAllDbFtGenesis(key)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 如果提供了 key，直接返回结果
	if key != "" {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtGenesisResponse{
			Data: data,
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// 计算分页
	total := len(data)
	totalPages := (total + pageSize - 1) / pageSize

	// 获取当前页的数据
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// 提取当前页的数据
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

	// 获取 key 参数
	key := c.Query("key")

	// 获取分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 获取数据
	data, err := s.indexer.GetAllDbFtGenesisOutput(key)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 如果提供了 key，直接返回结果
	if key != "" {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtGenesisOutputResponse{
			Data: data,
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// 计算分页
	total := len(data)
	totalPages := (total + pageSize - 1) / pageSize

	// 获取当前页的数据
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// 提取当前页的数据
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

	// 获取 key 参数
	txId := c.Query("txId")

	// 获取分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 获取数据
	data, err := s.indexer.GetAllDbUsedFtIncome(txId)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 如果提供了 key，直接返回结果
	if txId != "" {
		c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtUsedIncomeResponse{
			Data: data,
		}, time.Now().UnixMilli()-startTime))
		return
	}

	// 计算分页
	total := len(data)
	totalPages := (total + pageSize - 1) / pageSize

	// 获取当前页的数据
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// 提取当前页的数据
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

	// 获取 key 参数
	key := c.Query("key")

	// 获取分页参数
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "10"))

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	// 获取数据
	data, err := s.indexer.GetAllDbFtGenesisUtxo(key)
	if err != nil {
		c.JSONP(http.StatusInternalServerError, respond.RespErr(err, time.Now().UnixMilli()-startTime, http.StatusInternalServerError))
		return
	}

	// 解析数据
	parsedData := make(map[string]*respond.FtGenesisUtxo)
	for outpoint, value := range data {
		parts := strings.Split(value, "@")
		if len(parts) < 9 {
			continue
		}

		// 解析 decimal
		decimal, _ := strconv.ParseUint(parts[3], 10, 8)

		// 检查是否有 IsSpent 标记
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

	// 如果提供了 key，直接返回结果
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

	// 计算分页
	total := len(parsedData)
	totalPages := (total + pageSize - 1) / pageSize

	// 获取当前页的数据
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > total {
		end = total
	}

	// 提取当前页的数据
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

	// 获取总数量
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

// getMempoolAddressFtIncomeMap 获取内存池中所有地址的FT收入数据
func (s *FtServer) getMempoolAddressFtIncomeMap(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	incomeMap := s.mempoolMgr.GetMempoolAddressFtIncomeMap()

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtAddressFtIncomeMapResponse{
		Address:   "",
		IncomeMap: incomeMap,
	}, time.Now().UnixMilli()-startTime))
}

// getMempoolAddressFtIncomeValidMap 获取内存池中所有地址的有效FT收入数据
func (s *FtServer) getMempoolAddressFtIncomeValidMap(c *gin.Context) {
	startTime := time.Now().UnixMilli()

	// 获取数据
	incomeValidMap := s.mempoolMgr.GetMempoolAddressFtIncomeValidMap()

	c.JSONP(http.StatusOK, respond.RespSuccess(respond.FtAddressFtIncomeValidMapResponse{
		Address:        "",
		IncomeValidMap: incomeValidMap,
	}, time.Now().UnixMilli()-startTime))
}
