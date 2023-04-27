package common

import "strings"

// RetryErr a wrapper type to tell detector.WithRetry to retry on next node.
type RetryErr struct {
	inner error
}

// Retry to wrap an error to retry.
func Retry(err error) error {
	if err != nil {
		return &RetryErr{
			inner: err,
		}
	}
	return nil
}

// Error returns inner error.
func (e *RetryErr) Error() string {
	return e.inner.Error()
}

func UnwrapRetryErr(err error) error {
	if err == nil {
		return err
	}

	if rErr, ok := err.(*RetryErr); ok {
		return rErr.inner
	}

	return err
}

// NeedRetry returns true if the err need retry on next node.
func NeedRetry(err error) bool {
	if err == nil {
		return false
	}

	if _, ok := err.(*RetryErr); ok {
		return true
	}

	s := err.Error()

	if strings.Contains(s, NETWORK_ERROR) {
		return true
	}

	if strings.Contains(s, TRANSACTION_FAILED) {
		return true
	}

	if strings.Contains(s, REQUEST_BLOCKCHAIN_FAILED) {
		return true
	}

	if cErr, ok := err.(CustomError); ok {
		return cErr.needRetry
	}

	return false
}
