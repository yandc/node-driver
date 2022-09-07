package types

const TYPEMAIN = "main"
const TYPETEST = "test"
const CODEERROR = "CODEERROR"

const SLEEP_DURATION = 500
const ATTEMPTS = 10

type TransactionResponse struct {
	BlockHash               string `json:"block_hash"`
	BlockNumber             string `json:"block_number"`
	TransactionHash         string `json:"transaction_hash"`
	Status                  string `json:"status"`
	ExpirationTimestampSecs string `json:"expiration_timestamp_secs"`
	From                    string `json:"from"`
	To                      string `json:"to"`
	Amount                  string `json:"amount"`
	Nonce                   string `json:"nonce"`
	GasUsed                 string `json:"gas_used"`
	GasPrice                string `json:"gas_price"`
	GasLimit                string `json:"gas_limit"`
	SequenceNumber          string `json:"sequence_number"`
	BaseFeePerGas           string `json:"base_fee_per_gas"`
	//Token                   TokenInfo `json:"token"`
}

type TokenInfo struct {
	Address  string `json:"address"`
	Amount   string `json:"amount"`
	Decimals int64  `json:"decimals"`
	Symbol   string `json:"symbol"`
}

type MultiBalance struct {
	Handler     string                    `json:"handler"`
	ChainType   string                    `json:"chain_type"`
	NodeUrl     string                    `json:"node_url"`
	AddressList map[string]map[string]int `json:"address_list"`
}

type KVPair struct {
	Key string
	Val int64
}

type ChainConfig struct {
	ChainType      string   `json:"chain_type"`
	Chain          string   `json:"chain"`
	ProxyKey       string   `json:"proxy_key"`
	RpcURLs        []string `json:"rpc_urls"`
	ProxyCacheTime int      `json:"proxy_cache_time"`
}

type AbiDecodeResult struct {
	MethodName string                 `json:"method_name"`
	InputArgs  map[string]interface{} `json:"input_args"`
	IsGeneral  bool                   `json:"is_general"`
}

type ContractAbiResult struct {
	Ok   bool   `json:"ok"`
	Data string `json:"data"`
}
