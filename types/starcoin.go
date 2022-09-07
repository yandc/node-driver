package types

import (
	"encoding/json"
	"fmt"
)

//Request is a jsonrpc request
type Request struct {
	ID      int           `json:"id"`
	Jsonrpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

// ErrorObject is a jsonrpc error
type ErrorObject struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Response is a jsonrpc response
type Response struct {
	ID     uint64          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *ErrorObject    `json:"error,omitempty"`
}

type Balance struct {
	Raw  string `json:"raw"`
	JSON struct {
		Token struct {
			Value int64 `json:"value"`
		} `json:"token"`
	} `json:"json"`
}

type NodeInfo struct {
	PeerInfo   PeerInfo `json:"peer_info"`
	NowSeconds int      `json:"now_seconds"`
}

type PeerInfo struct {
	ChainInfo ChainInfo `json:"chain_info"`
}

type ChainInfo struct {
	ChainID int `json:"chain_id"`
}

// Error implements error interface
func (e *ErrorObject) Error() string {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf("jsonrpc.internal marshal error: %v", err)
	}
	return string(data)
}

type Transaction struct {
	BlockHash        string          `json:"block_hash"`
	BlockNumber      string          `json:"block_number"`
	TransactionHash  string          `json:"transaction_hash"`
	TransactionIndex int             `json:"transaction_index"`
	BlockMetadata    BlockMetadata   `json:"block_metadata"`
	UserTransaction  UserTransaction `json:"user_transaction"`
}

type BlockMetadata struct {
	Author        string `json:"author"`
	ChainID       string `json:"chain_id"`
	Number        string `json:"number"`
	ParentGasUsed int    `json:"parent_gas_used"`
	ParentHash    string `json:"parent_hash"`
	Timestamp     int64  `json:"timestamp"`
	Uncles        string `json:"uncles"`
}

type UserTransaction struct {
	TransactionHash string         `json:"transaction_hash"`
	RawTransaction  RawTransaction `json:"raw_txn"`
	Authenticator   Authenticator  `json:"authenticator"`
}

type RawTransaction struct {
	Sender                  string         `json:"sender"`
	SequenceNumber          string         `json:"sequence_number"`
	Payload                 string         `json:"payload"`
	DecodedPayload          DecodedPayload `json:"decoded_payload"`
	MaxGasAmount            string         `json:"max_gas_amount"`
	GasUnitPrice            string         `json:"gas_unit_price"`
	GasTokenCode            string         `json:"gas_token_code"`
	ExpirationTimestampSecs string         `json:"expiration_timestamp_secs"`
	ChainID                 int            `json:"chain_id"`
}

type DecodedPayload struct {
	ScriptFunction ScriptFunction `json:"ScriptFunction"`
}

type ScriptFunction struct {
	Module   string        `json:"module"`
	Function string        `json:"function"`
	TyArgs   []string      `json:"ty_args"`
	Args     []interface{} `json:"args"`
}

type Authenticator struct {
	Ed25519 Ed25519 `json:"Ed25519"`
}

type Ed25519 struct {
	PublicKey string `json:"public_key"`
	Signature string `json:"signature"`
}

type TransactionInfo struct {
	BlockHash              string          `json:"block_hash"`
	BlockNumber            string          `json:"block_number"`
	TransactionHash        string          `json:"transaction_hash"`
	TransactionIndex       int             `json:"transaction_index"`
	TransactionGlobalIndex string          `json:"transaction_global_index"`
	StateRootHash          string          `json:"state_root_hash"`
	EventRootHash          string          `json:"event_root_hash"`
	GasUsed                string          `json:"gas_used"`
	Status                 json.RawMessage `json:"status"`
}

type DryRunParam struct {
	ChainId         int               `json:"chain_id"`
	GasUnitPrice    int               `json:"gas_unit_price"`
	Sender          string            `json:"sender"`
	SenderPublicKey string            `json:"sender_public_key"`
	SequenceNumber  uint64            `json:"sequence_number"`
	MaxGasAmount    uint64            `json:"max_gas_amount"`
	Script          DryRunParamScript `json:"script"`
}

type DryRunParamScript struct {
	Code     string   `json:"code"`
	TypeArgs []string `json:"type_args"`
	Args     []string `json:"args"`
}

type DryRunResult struct {
	GasUsed string `json:"gas_used"`
}

type STCBlock struct {
	BlockHeader BlockHeader   `json:"header"`
	BlockBody   BlockBody     `json:"body"`
	Uncles      []BlockHeader `json:"uncles"`
}

type BlockHeader struct {
	Timestamp            string  `json:"timestamp"`
	Author               string  `json:"author"`
	AuthorAuthKey        *string `json:"author_auth_key"`
	BlockAccumulatorRoot string  `json:"block_accumulator_root"`
	BlockHash            string  `json:"block_hash"`
	BodyHash             string  `json:"body_hash"`
	ChainId              int     `json:"chain_id"`
	DifficultyHexStr     string  `json:"difficulty"`
	Difficulty           uint64  `json:"difficulty_number"`
	Extra                string  `json:"extra"`
	GasUsed              string  `json:"gas_used"`
	Nonce                uint64  `json:"Nonce"`
	Height               string  `json:"number"`
	ParentHash           string  `json:"parent_hash"`
	StateRoot            string  `json:"state_root"`
	TxnAccumulatorRoot   string  `json:"txn_accumulator_root"`
}

type BlockBody struct {
	UserTransactions []UserTransaction `json:"Full"`
}

type ContractResource struct {
	Value     [][]interface{} `json:"value"`
}
