package testagent

import (
	"context"
	"fmt"
	"time"

	"pinecone_test/pinecone"

	"github.com/pkg/errors"
	"github.com/remeh/sizedwaitgroup"
	"golang.org/x/time/rate"
)

type Agent struct {
	Client *pinecone.Client
}

func (a *Agent) QueryTask(taskName string, concurrency int, limit rate.Limit) *QueryTask {
	return &QueryTask{
		Agent:   a,
		wg:      sizedwaitgroup.New(concurrency),
		rate:    *rate.NewLimiter(limit, 100),
		timeout: 10 * time.Second,
		name:    fmt.Sprintf("q-%s", taskName),
	}
}

func (a *Agent) UpsertTask(taskName string, concurrency int, limit rate.Limit) *UpsertTask {
	return &UpsertTask{
		Agent:   a,
		wg:      sizedwaitgroup.New(concurrency),
		rate:    *rate.NewLimiter(limit, 1000),
		timeout: 10 * time.Second,
		name:    fmt.Sprintf("u-%s", taskName),
	}
}

func (a *Agent) DeleteTask(taskName string, concurrency int, limit rate.Limit) *DeleteTask {
	return &DeleteTask{
		Agent:   a,
		wg:      sizedwaitgroup.New(concurrency),
		rate:    *rate.NewLimiter(limit, 100),
		timeout: 10 * time.Second,
		name:    fmt.Sprintf("d-%s", taskName),
	}
}

func getError(err error) string {
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "timeout"
		}
		return "error"
	}
	return "success"
}
