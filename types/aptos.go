package types

type AptosBalanceResp struct {
	Data struct {
		Coin struct {
			Value string `json:"value"`
		} `json:"coin"`
	} `json:"data"`
	AptosBadResp
}

type AptosSendTxResp struct {
	Hash string `json:"hash"`
	AptosBadResp
}

type AptosAccountResp struct {
	SequenceNumber    string `json:"sequence_number"`
	AuthenticationKey string `json:"authentication_key"`
	AptosBadResp
}

type AptosLedgerInfoResp struct {
	ChainId             int    `json:"chain_id"`
	Epoch               string `json:"epoch"`
	LedgerVersion       string `json:"ledger_version"`
	OldestLedgerVersion string `json:"oldest_ledger_version"`
	BlockHeight         string `json:"block_height"`
	LedgerTimestamp     string `json:"ledger_timestamp"`
	NodeRole            string `json:"node_role"`
	AptosBadResp
}

type AptosResourceInfo []struct {
	Type string `json:"type"`
}

type AptosResourceResp []struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

type AptosTokenInfo struct {
	Decimals int    `json:"decimals"`
	Name     string `json:"name"`
	Symbol   string `json:"symbol"`
}

type AptosBadResp struct {
	ErrorCode          string `json:"error_code"`
	Message            string `json:"message"`
	AptosLedgerVersion string `json:"aptos_ledger_version"`
}
