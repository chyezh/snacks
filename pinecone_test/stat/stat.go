package stat

import (
	"context"
	"fmt"
	"math"

	"github.com/montanaflynn/stats"
	"github.com/rocketlaunchr/dataframe-go"
)

type Stat struct {
	mean   float64
	median float64
	stddev float64
}

func GetStat(ctx context.Context, s dataframe.Series) Stat {
	var v *dataframe.SeriesFloat64
	switch s := s.(type) {
	case *dataframe.SeriesFloat64:
		v = s
	case *dataframe.SeriesInt64:
		var err error
		v, err = s.ToSeriesFloat64(ctx, true)
		if err != nil {
			panic(err)
		}
	default:
		panic("unexpected dataframe series")
	}
	st := Stat{
		mean:   math.NaN(),
		median: math.NaN(),
		stddev: math.NaN(),
	}
	if mean, err := stats.Mean(v.Values); err == nil {
		st.mean = mean
	}
	if median, err := stats.Median(v.Values); err == nil {
		st.median = median
	}
	if stddev, err := stats.StdDevP(v.Values); err == nil {
		st.stddev = stddev
	}
	return st
}

func (s *Stat) String() string {
	return fmt.Sprintf("mean: %f, median: %f, stddev: %f", s.mean, s.median, s.stddev)
}
