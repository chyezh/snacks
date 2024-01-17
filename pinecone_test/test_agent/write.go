package testagent

import (
	"context"
	"time"

	"pinecone_test/metrics"
	"pinecone_test/pinecone"

	"github.com/pkg/errors"
	"github.com/remeh/sizedwaitgroup"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var batchWriteLimit int = 1000

type WriteTask struct {
	*Agent
	wg      *sizedwaitgroup.SizedWaitGroup
	rate    rate.Limiter
	timeout time.Duration
	limit   int
	name    string
}

func (w *WriteTask) Do() {
	defer w.wg.Wait()

	pending := make([]pinecone.UpsertVector, 0, batchWriteLimit)
	cnt := 0

	for {
		v, ok := <-w.dataSource
		if ok {
			break
		}
		pending = append(pending, v)
		cnt++
		if cnt >= w.limit {
			break
		}

		if len(pending) >= batchWriteLimit {
			ready := pending
			pending = make([]pinecone.UpsertVector, 0, batchWriteLimit)
			w.startNewWriting(ready)
		}
	}
	if len(pending) > 0 {
		w.startNewWriting(pending)
	}
}

func (w *WriteTask) startNewWriting(vectors []pinecone.UpsertVector) {
	w.rate.WaitN(context.Background(), len(vectors))
	w.wg.Add()
	go func() {
		defer w.wg.Done()
		req := pinecone.UpsertRequest{
			Namespace: "test",
			Vectors:   vectors,
		}
		ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
		defer cancel()
		start := time.Now()
		response, err := w.client.Upsert(ctx, req)
		cost := time.Since(start)
		if err != nil {
			zap.L().Warn("upsert failure", zap.Error(err))
		}
		if response.UpsertedCount != len(vectors) {
			zap.L().Warn("upsert result unexpected", zap.Int("upsertedCount", response.UpsertedCount), zap.Int("expected", len(vectors)))
		}

		status := getError(err)
		metrics.RequestDuration.WithLabelValues(
			w.name,
			"upsert",
			status,
		).Observe(cost.Seconds())
		metrics.VectorTotal.WithLabelValues(
			w.name,
			"upsert",
			status,
		).Add(float64(len(vectors)))
	}()
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
