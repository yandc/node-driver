package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
)

type ApiResponse struct {
	Status bool        `json:"status"`
	Msg    string      `json:"error,omitempty"`
	Data   interface{} `json:"result,omitempty"`
}

func (e *ApiResponse) Error() string {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	err := encoder.Encode(e)
	if err != nil {
		return fmt.Sprintf("encode apiResponse error, %s", err)
	}
	json := string(buffer.Bytes())
	return json
}

func NewApiResponse(status bool, msg string, data interface{}) *ApiResponse {
	return &ApiResponse{
		Status: status,
		Msg:    msg,
		Data:   data,
	}
}

func NewErrorApiResponse(msg string) *ApiResponse {
	return &ApiResponse{
		Status: false,
		Msg:    msg,
		Data:   nil,
	}
}

func (ar *ApiResponse) SetError(err error) {
	ar.Status = false
	ar.Msg = fmt.Sprintf("%s", err)
}

func (ar *ApiResponse) SetMsg(msg string) {
	ar.Status = false
	ar.Msg = msg
}

func (ar *ApiResponse) AddError(err error) {
	ar.Status = false
	msg := fmt.Sprintf("%s", err)
	if customErr, ok := err.(CustomError); ok {
		msg = customErr.Custom
	}
	if ar.Msg == "" {
		ar.Msg = msg
	} else {
		ar.Msg = ar.Msg + ", " + msg
	}
}

func (ar *ApiResponse) AddMsg(msg string) {
	ar.Status = false
	if ar.Msg == "" {
		ar.Msg = msg
	} else {
		ar.Msg = ar.Msg + ", " + msg
	}
}

func (ar *ApiResponse) SetData(result interface{}) {
	ar.Status = true
	ar.Msg = ""
	ar.Data = result
}

type CustomError struct {
	Message string
	Custom  string

	needRetry bool
}

func NewCustomError(custom string, message error) CustomError {
	_, file, line, _ := runtime.Caller(1)
	fileInfo := fmt.Sprintf("[%s]:%d", file, line)
	var messageInfo string
	if message != nil {
		messageInfo = message.Error()
	}
	return CustomError{
		Message:   fileInfo + " " + messageInfo,
		Custom:    custom,
		needRetry: false, // we don't need retry by default.
	}
}

// Error implements error interface
func (e CustomError) Error() string {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf("jsonrpc.internal marshal error: %v", err)
	}
	return string(data)
}

// NoNeedRetry mark current error don't need retry.
func (e CustomError) NeedRetry() {
	e.needRetry = true
}
