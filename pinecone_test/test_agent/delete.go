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

type DeleteTask struct {
	*Agent
	wg      sizedwaitgroup.SizedWaitGroup
	rate    rate.Limiter
	timeout time.Duration
	name    string
}

type opInfoDeleteMeta struct {
	IDs []string `json:"ids"`
}

func (w *DeleteTask) Do(deleteSource <-chan pinecone.DeleteRequest) {
	defer w.wg.Wait()
	for req := range deleteSource {
		w.startNewTask(req)
	}
}

func (w *DeleteTask) startNewTask(req pinecone.DeleteRequest) {
	_ = w.rate.WaitN(context.Background(), len(req.IDs))
	w.wg.Add()
	go func() {
		defer w.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
		defer cancel()
		start := time.Now()
		err := w.Client.Delete(ctx, req)
		cost := time.Since(start)
		if err != nil {
			zap.L().Warn("delete failure", zap.Error(err))
		}

		status := getError(err)
		info := &opInfo{
			OpType: "d",
			Status: status,
			Cost:   cost.Milliseconds(),
			Meta: &opInfoDeleteMeta{
				IDs: req.IDs,
			},
		}
		w.recordOp(info)

		metrics.RequestDuration.WithLabelValues(
			w.name,
			"delete",
			status,
		).Observe(cost.Seconds())
		metrics.VectorTotal.WithLabelValues(
			w.name,
			"delete",
			status,
		).Add(float64(len(req.IDs)))
	}()
}
