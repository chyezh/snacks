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

type QueryTask struct {
	*Agent
	wg      sizedwaitgroup.SizedWaitGroup
	rate    rate.Limiter
	timeout time.Duration
	name    string
}

func (w *QueryTask) Do(reqSource <-chan pinecone.QueryRequest) {
	defer w.wg.Wait()

	for req := range reqSource {
		w.startNewTask(req)
	}
}

func (w *QueryTask) startNewTask(req pinecone.QueryRequest) {
	_ = w.rate.WaitN(context.Background(), 1)
	w.wg.Add()
	go func() {
		defer w.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
		defer cancel()
		start := time.Now()
		response, err := w.Client.Query(ctx, req)
		cost := time.Since(start)
		if err != nil {
			zap.L().Warn("query failure", zap.Error(err))
		}

		status := getError(err)
		metrics.RequestDuration.WithLabelValues(
			w.name,
			"query",
			status,
		).Observe(cost.Seconds())
		if response != nil {
			metrics.ReadUnitsTotal.WithLabelValues(
				w.name,
			).Add(float64(response.Usage.ReadUnits))
		}
	}()
}
