package stat

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/rocketlaunchr/dataframe-go"
)

// NewTaskCostFrame creates a new WriteTaskFrame.
func NewTaskCostFrame(expectedCnt int, totalCnt int) *TaskCostFrame {
	cfg := &dataframe.SeriesInit{
		Capacity: expectedCnt,
	}
	return &TaskCostFrame{
		mu: sync.Mutex{},
		DataFrame: dataframe.NewDataFrame(
			dataframe.NewSeriesTime("time", cfg),
			dataframe.NewSeriesInt64("state", cfg),
			dataframe.NewSeriesInt64("msgSize", cfg),
			dataframe.NewSeriesFloat64("cost", cfg),
		),
		sampler: NewAutoSampler(expectedCnt, totalCnt),
	}
}

// TaskCostFrame is a data frame for write task.
type TaskCostFrame struct {
	mu sync.Mutex
	*dataframe.DataFrame
	sampler *Sampler
}

func (wf *TaskCostFrame) Append(msgSize int, cost time.Duration, err error) {
	if !wf.sampler.Hit() {
		return
	}
	st := int64(newStateError(err))
	microSeconds := cost.Microseconds()
	wf.mu.Lock()
	now := time.Now()
	wf.DataFrame.Append(&dataframe.DontLock, now, st, msgSize, microSeconds)
	wf.mu.Unlock()
}

func (wf *TaskCostFrame) Report(ctx context.Context, w io.Writer) error {
	if err := reportOnFilter(ctx, w, wf.DataFrame, success); err != nil {
		return err
	}
	if err := reportOnFilter(ctx, w, wf.DataFrame, failed); err != nil {
		return err
	}
	if err := reportOnFilter(ctx, w, wf.DataFrame, timeout); err != nil {
		return err
	}
	return reportOnFilter(ctx, w, wf.DataFrame, all)
}

func reportOnFilter(ctx context.Context, w io.Writer, f *dataframe.DataFrame, s state) error {
	msgSize, cost := filterAndStat(ctx, f, newStateFilter(s))
	_, err := fmt.Fprintf(w, "state %s: msgSize[%s](bytes); cost[%s](us)\n", s, &msgSize, &cost)
	return err
}

func filterAndStat(ctx context.Context, f *dataframe.DataFrame, filter interface{}) (msgSize Stat, cost Stat) {
	var data *dataframe.DataFrame
	if filter == nil {
		data = f
	} else {
		tmp, _ := dataframe.Filter(ctx, f, filter)
		data = tmp.(*dataframe.DataFrame)
	}
	return GetStat(ctx, data.Series[2]), GetStat(ctx, data.Series[3])
}

func newStateFilter(s state) interface{} {
	return dataframe.FilterDataFrameFn(func(vals map[interface{}]interface{}, row, nRows int) (dataframe.FilterAction, error) {
		if s == all || vals["state"].(int64) == int64(s) {
			return dataframe.KEEP, nil
		}
		return dataframe.DROP, nil
	})
}
