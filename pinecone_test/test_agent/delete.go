package testagent

import (
	"context"
	"time"

	"pinecone_test/metrics"
	"pinecone_test/pinecone"

	"github.com/remeh/sizedwaitgroup"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var batchDeleteLimit int = 5

type DeleteTask struct {
	*Agent
	wg      *sizedwaitgroup.SizedWaitGroup
	rate    rate.Limiter
	timeout time.Duration
	name    string
}

func (w *DeleteTask) Do(deleteSource <-chan string) {
	defer w.wg.Wait()

	pending := make([]string, 0, batchDeleteLimit)
	for v := range deleteSource {
		pending = append(pending, v)
		if len(pending) >= batchDeleteLimit {
			ready := pending
			pending = make([]string, 0, batchDeleteLimit)
			w.startNewTask(ready)
		}
	}
	if len(pending) > 0 {
		w.startNewTask(pending)
	}
}

func (w *DeleteTask) startNewTask(ids []string) {
	_ = w.rate.WaitN(context.Background(), len(ids))
	w.wg.Add()
	go func() {
		defer w.wg.Done()
		req := pinecone.DeleteRequest{
			Namespace: w.Namespace,
			IDs:       ids,
		}
		ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
		defer cancel()
		start := time.Now()
		err := w.client.Delete(ctx, req)
		cost := time.Since(start)
		if err != nil {
			zap.L().Warn("delete failure", zap.Error(err))
		}

		status := getError(err)
		metrics.RequestDuration.WithLabelValues(
			w.name,
			"delete",
			status,
		).Observe(cost.Seconds())
		metrics.VectorTotal.WithLabelValues(
			w.name,
			"delete",
			status,
		).Add(float64(len(ids)))
	}()
}
