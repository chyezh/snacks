package stat

import (
	"context"
	"errors"
)

const (
	all state = iota
	success
	timeout
	failed
)

type state int

func newStateError(err error) state {
	if err == nil {
		return success
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return timeout
	}
	return failed
}

func (s state) String() string {
	switch s {
	case success:
		return "success"
	case timeout:
		return "timeout"
	case failed:
		return "failed"
	case all:
		return "all"
	default:
		return "unknown"
	}
}
