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

type UpsertTask struct {
	*Agent
	wg      sizedwaitgroup.SizedWaitGroup
	rate    rate.Limiter
	timeout time.Duration
	name    string
}

func (w *UpsertTask) Do(dataSource <-chan pinecone.UpsertRequest) {
	defer w.wg.Wait()
	for req := range dataSource {
		w.startNewTask(req)
	}
}

func (w *UpsertTask) startNewTask(req pinecone.UpsertRequest) {
	_ = w.rate.WaitN(context.Background(), len(req.Vectors))
	w.wg.Add()
	go func() {
		defer w.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
		defer cancel()
		start := time.Now()
		response, err := w.Client.Upsert(ctx, req)
		cost := time.Since(start)
		if err != nil {
			zap.L().Warn("upsert failure", zap.Error(err))
		} else if response.UpsertedCount != len(req.Vectors) {
			zap.L().Warn("upsert result unexpected", zap.Int("upsertedCount", response.UpsertedCount), zap.Int("expected", len(req.Vectors)))
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
		).Add(float64(len(req.Vectors)))
	}()
}
