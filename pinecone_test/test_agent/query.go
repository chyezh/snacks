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
	wg      *sizedwaitgroup.SizedWaitGroup
	rate    rate.Limiter
	timeout time.Duration
	topK    int
	name    string
}

func (w *QueryTask) Do(vectorSource <-chan []float32) {
	defer w.wg.Wait()

	for v := range vectorSource {
		w.startNewTask(v)
	}
}

func (w *QueryTask) startNewTask(vector []float32) {
	_ = w.rate.WaitN(context.Background(), 1)
	w.wg.Add()
	go func() {
		defer w.wg.Done()
		req := pinecone.QueryRequest{
			Namespace:       w.Namespace,
			IncludeValues:   true,
			IncludeMetadata: true,
			TopK:            w.topK,
			Vector:          vector,
		}
		ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
		defer cancel()
		start := time.Now()
		response, err := w.client.Query(ctx, req)
		cost := time.Since(start)
		if err != nil {
			zap.L().Warn("query failure", zap.Error(err))
		}

		status := getError(err)
		metrics.RequestDuration.WithLabelValues(
			w.name,
			"upsert",
			status,
		).Observe(cost.Seconds())
		if response != nil {
			metrics.ReadUnitsTotal.WithLabelValues(
				w.name,
			).Add(float64(response.Usage.ReadUnits))
		}
	}()
}
