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

var batchUpsertLimit int = 1000

type UpsertTask struct {
	*Agent
	wg      *sizedwaitgroup.SizedWaitGroup
	rate    rate.Limiter
	timeout time.Duration
	name    string
}

func (w *UpsertTask) Do(dataSource <-chan pinecone.UpsertVector) {
	defer w.wg.Wait()

	pending := make([]pinecone.UpsertVector, 0, batchUpsertLimit)
	for v := range dataSource {
		pending = append(pending, v)
		if len(pending) >= batchUpsertLimit {
			ready := pending
			pending = make([]pinecone.UpsertVector, 0, batchUpsertLimit)
			w.startNewTask(ready)
		}
	}
	if len(pending) > 0 {
		w.startNewTask(pending)
	}
}

func (w *UpsertTask) startNewTask(vectors []pinecone.UpsertVector) {
	_ = w.rate.WaitN(context.Background(), len(vectors))
	w.wg.Add()
	go func() {
		defer w.wg.Done()
		req := pinecone.UpsertRequest{
			Namespace: w.Namespace,
			Vectors:   vectors,
		}
		ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
		defer cancel()
		start := time.Now()
		response, err := w.client.Upsert(ctx, req)
		cost := time.Since(start)
		if err != nil {
			zap.L().Warn("upsert failure", zap.Error(err))
		} else if response.UpsertedCount != len(vectors) {
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
