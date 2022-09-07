package types

import (
	"encoding/json"
	"math/big"
)

const (
	ERC20   = "ERC20"
	ERC721  = "ERC721"
	ERC1155 = "ERC1155"
)

var Status = map[uint64]string{
	uint64(0): "Fail",
	uint64(1): "Success",
}

type ProxyETHGasResult struct {
	Ok      bool   `json:"ok"`
	Message string `json:"message"`
	Data    Result `json:"data"`
}

type Result struct {
	SafeGasPrice    string `json:"safeGasPrice"`
	ProposeGasPrice string `json:"proposeGasPrice"`
	FastGasPrice    string `json:"fastGasPrice"`
	SuggestBaseFee  string `json:"suggestBaseFee"`
}

type FeeScope struct {
	Max float64
	Min float64
}

//var EVMFeeMap = map[string]struct{}{
//	coins.OKEX: {},
//}

type ProxyOkexChainFee struct {
	Ok   bool         `json:"ok"`
	Data OkexChainFee `json:"data"`
}

type OkexChainFee struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Average float64 `json:"average"`
		Slow    float64 `json:"slow"`
		Fast    float64 `json:"fast"`
	} `json:"data"`
}

type EVMResponse struct {
	JsonRpc string          `json:"jsonrpc"`
	Id      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonError      `json:"error,omitempty"`
}

type jsonError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type ProxyRequest struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Body    interface{}       `json:"body"`
}

type EventLog struct {
	From   string   `json:"from"`
	To     string   `json:"to"`
	Amount *big.Int `json:"amount"`
}
