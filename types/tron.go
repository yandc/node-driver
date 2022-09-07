package types

type TronBalance struct {
	Balance int64 `json:"balance"`
}

type BalanceReq struct {
	Address string `json:"address"`
	Visible bool   `json:"visible"`
}

type TransactionReq struct {
	RawData string `json:"raw_data"`
}

//正常情况返回Txid+Result
//异常情况返回Txid+ErrorCode+Message
//错误情况返回Error
type SendTransactionRes struct {
	TxId    string `json:"txid"`
	Result  bool   `json:"result"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Error   string `json:"error"`
}

type NowBlockRes struct {
	BlockHeader struct {
		RawData struct {
			Number         uint64 `json:"number"`
			TxTrieRoot     string `json:"txTrieRoot"`
			WitnessAddress string `json:"witness_address"`
			ParentHash     string `json:"parentHash"`
			Version        int    `json:"version"`
			Timestamp      int64  `json:"timestamp"`
		} `json:"raw_data"`
	} `json:"block_header"`
}

type FeeLimit struct {
	EnergyUsed int `json:"energy_used"`
}

type TriggerConstantContractReq struct {
	OwnerAddress     string `json:"owner_address"`
	ContractAddress  string `json:"contract_address"`
	FunctionSelector string `json:"function_selector"`
	Parameter        string `json:"parameter"`
	Visible          bool   `json:"visible"`
}

type TronTokenInfo struct {
	Data []struct {
		TokenInfo struct {
			TokenID      string `json:"tokenId"`
			TokenAbbr    string `json:"tokenAbbr"`
			TokenName    string `json:"tokenName"`
			TokenDecimal int    `json:"tokenDecimal"`
			TokenCanShow int    `json:"tokenCanShow"`
			TokenType    string `json:"tokenType"`
		} `json:"tokenInfo"`
	} `json:"data"`
}

type TronTxResponse struct {
	Ret []struct {
		ContractRet string `json:"contractRet"`
	} `json:"ret"`
	Signature []string `json:"signature"`
	TxID      string   `json:"txID"`
	RawData   struct {
		Contract []struct {
			Parameter struct {
				Value struct {
					Data            string `json:"data"`
					OwnerAddress    string `json:"owner_address"`
					ContractAddress string `json:"contract_address"`
					Amount          int    `json:"amount"`
					ToAddress       string `json:"to_address"`
				} `json:"value"`
				TypeURL string `json:"type_url"`
			} `json:"parameter"`
			Type string `json:"type"`
		} `json:"contract"`
		RefBlockBytes string `json:"ref_block_bytes"`
		RefBlockHash  string `json:"ref_block_hash"`
		Expiration    int64  `json:"expiration"`
		FeeLimit      int    `json:"fee_limit"`
		Timestamp     int64  `json:"timestamp"`
	} `json:"raw_data"`
}

type TronTxReq struct {
	Value   string `json:"value"`
	Visible bool   `json:"visible"`
}

type TronTxInfoResponse struct {
	ID              string   `json:"id"`
	Fee             int      `json:"fee"`
	BlockNumber     int      `json:"blockNumber"`
	BlockTimeStamp  int64    `json:"blockTimeStamp"`
	ContractResult  []string `json:"contractResult"`
	ContractAddress string   `json:"contract_address"`
	Receipt         struct {
		EnergyUsage      int    `json:"energy_usage"`
		EnergyUsageTotal int    `json:"energy_usage_total"`
		NetUsage         int    `json:"net_usage"`
		NetFee           int    `json:"net_fee"`
		Result           string `json:"result"`
	} `json:"receipt"`
	Log []struct {
		Address string   `json:"address"`
		Topics  []string `json:"topics"`
		Data    string   `json:"data"`
	} `json:"log"`
}

type TronTokenBalanceRes struct {
	ConstantResult []string `json:"constant_result"`
}

type TronTokenBalanceReq struct {
	OwnerAddress     string `json:"owner_address"`
	ContractAddress  string `json:"contract_address"`
	FunctionSelector string `json:"function_selector"`
	Parameter        string `json:"parameter"`
	Visible          bool   `json:"visible"`
}
