package testagent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"pinecone_test/pinecone"

	"github.com/pkg/errors"
	"github.com/remeh/sizedwaitgroup"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type Agent struct {
	Client   *pinecone.Client
	Mu       sync.Mutex
	OpLogger *bufio.Writer
}

type opInfo struct {
	Now    int64       `json:"n"`
	OpType string      `json:"o"`
	Status string      `json:"s"`
	Cost   int64       `json:"c"`
	Meta   interface{} `json:"m"`
}

func (a *Agent) recordOp(info *opInfo) {
	info.Now = time.Now().UnixNano()
	b, err := json.Marshal(info)
	if err != nil {
		zap.L().Warn("fail to record op", zap.Error(err))
		return
	}
	a.Mu.Lock()
	defer a.Mu.Unlock()
	a.OpLogger.Write(b)
	a.OpLogger.WriteByte('\n')
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
