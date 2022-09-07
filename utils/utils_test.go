package utils

import (
	"fmt"
	"math/big"
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.bixin.com/mili/node-driver/common"
)

func TestBigIntString(t *testing.T) {
	amount := BigIntString(big.NewInt(0), 3)
	assert.Equal(t, "0", amount)

	amount = BigIntString(big.NewInt(100000), 3)
	assert.Equal(t, "100", amount)

}

func TestClean(t *testing.T) {
	err := test1()
	if value, ok := err.(common.CustomError); ok {
		fmt.Println("Custom:", value.Custom, ",message:", value.Message)
	}
	getFileAndLine()
	fmt.Println(err.Error())
}

func test1() error {
	pc, file, line, _ := runtime.Caller(0)
	funcName := runtime.FuncForPC(pc).Name()
	name := path.Base(file)
	fmt.Println("name=", name, funcName)
	fmt.Println(file, line)
	return common.CustomError{
		Custom:  "hello",
		Message: "world",
	}
}

func getFileAndLine() {
	_, file, line, _ := runtime.Caller(1)
	fmt.Println("file:", file, ",lien=", line)
	str := fmt.Sprintf("[%s]:%d", file, line)
	fmt.Println(str)

}

func TestGetUUID(t *testing.T) {
	GetUUID()

}
