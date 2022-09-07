package utils

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"

	uuid "github.com/satori/go.uuid"
	"gitlab.bixin.com/mili/node-driver/common"
	"gitlab.bixin.com/mili/node-driver/types"
)

func BigIntString(balance *big.Int, decimals int) string {
	var result string
	amount := balance.String()
	if len(amount) > decimals {
		result = fmt.Sprintf("%s.%s", amount[0:len(amount)-decimals], amount[len(amount)-decimals:])
	} else {
		sub := decimals - len(amount)
		var zero string
		for i := 0; i < sub; i++ {
			zero += "0"
		}
		result = "0." + zero + amount
	}
	return Clean(strings.TrimRight(result, "0"))
}

func UpdateDecimals(amount string, decimals int) string {
	var result string
	//amount := balance.String()
	if len(amount) > decimals {
		result = fmt.Sprintf("%s.%s", amount[0:len(amount)-decimals], amount[len(amount)-decimals:])
	} else {
		sub := decimals - len(amount)
		var zero string
		for i := 0; i < sub; i++ {
			zero += "0"
		}
		result = "0." + zero + amount
	}
	return Clean(strings.TrimRight(result, "0"))
}

func bigIntFloat(balance *big.Int, decimals int64) *big.Float {
	if balance.Sign() == 0 {
		return big.NewFloat(0)
	}
	bal := big.NewFloat(0)
	bal.SetInt(balance)
	pow := bigPow(10, decimals)
	p := big.NewFloat(0)
	p.SetInt(pow)
	bal.Quo(bal, p)
	return bal
}

func bigPow(a, b int64) *big.Int {
	r := big.NewInt(a)
	return r.Exp(r, big.NewInt(b), nil)
}

func Clean(newNum string) string {
	stringBytes := bytes.TrimRight([]byte(newNum), "0")
	newNum = string(stringBytes)
	if stringBytes[len(stringBytes)-1] == 46 {
		newNum = newNum[:len(stringBytes)-1]
	}
	if stringBytes[0] == 46 {
		newNum = "0" + newNum
	}
	return newNum
}

func NewTxResponse(blockHash, blockNumber, txHash, status, exTime, from, to, amount, nonce, gasUsed, gasPrice, gasLimit,
	sequenceNumber, baseFeePerGas string, token types.TokenInfo) map[string]interface{} {
	tokenInfo := map[string]interface{}{
		"address":  token.Address,
		"amount":   token.Amount,
		"decimals": token.Decimals,
		"symbol":   token.Symbol,
	}
	return map[string]interface{}{
		"block_hash":                blockHash,
		"block_number":              blockNumber,
		"transaction_hash":          txHash,
		"status":                    status,
		"expiration_timestamp_secs": exTime,
		"from":                      from,
		"to":                        to,
		"amount":                    amount,
		"nonce":                     nonce,
		"gas_used":                  gasUsed,
		"gas_price":                 gasPrice,
		"gas_limit":                 gasLimit,
		"sequence_number":           sequenceNumber,
		"base_fee_per_gas":          baseFeePerGas,
		"token":                     tokenInfo,
	}
}

func do(work func(retryIndex int, params ...interface{}), retryIndex int, params ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if strValue, ok := r.(string); ok {
				err = errors.New(strValue)
			} else if errValue, ok := r.(error); ok {
				err = errValue
			}
			return
		}
	}()
	work(retryIndex, params...)
	return
}

func DoWork(work func(retryIndex int, params ...interface{}), maxRetryCount int, wg *sync.WaitGroup, err *error, params ...interface{}) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		if maxRetryCount > 3 {
			maxRetryCount = 3
		}
		errInfo := do(work, -1, params...)
		if errInfo != nil {
			*err = common.NewCustomError(common.NETWORK_ERROR, errInfo)
		}
		for i := 0; errInfo != nil && i < maxRetryCount && strings.Contains(errInfo.Error(), "retry"); i++ {
			errInfo = do(work, i, params...)
			if errInfo != nil {
				*err = common.NewCustomError(common.NETWORK_ERROR, errInfo)
			}
		}
	}()
}

func GetUUID() string {
	id := uuid.NewV4().String()
	uuid := strings.Replace(id, "-", "", 4)
	return uuid
}
