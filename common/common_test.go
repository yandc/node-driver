package common

import (
	"errors"
	"testing"
)

func TestNeedRetry(t *testing.T) {
	if NeedRetry(errors.New("some random error")) {
		t.Fatal("random error shall not retry")
	}

	if !NeedRetry(errors.New(NETWORK_ERROR)) {
		t.Fatal("network error shall retry")
	}

	if !NeedRetry(Retry(errors.New("inner"))) {
		t.Fatal("error wrapped by Retry shall retry")
	}
}
