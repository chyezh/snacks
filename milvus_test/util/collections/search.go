package collections

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/remeh/sizedwaitgroup"
	"golang.org/x/time/rate"
)

func (coll *Collection) TestSearch(ctx context.Context, r *rate.Limiter, concurrency int, topk int, nq int, count int) {
	wg := sizedwaitgroup.New(concurrency)
	vecField := coll.VecField()
	dim := coll.DIM()

	sp, err := entity.NewIndexIvfHNSWSearchParam(8, 8)
	if err != nil {
		panic(err)
	}

	for i := 0; i < count; i++ {
		if err := r.Wait(ctx); err != nil {
			panic(err)
		}

		wg.Add()
		go func(i int) {
			defer wg.Done()
			targetVec := randFloatVectorEntitySlice(dim, nq)
			start := time.Now()
			result, err := coll.cli.Search(
				ctx,
				coll.Name(),
				[]string{},
				"",
				[]string{coll.schema.PKFieldName()},
				targetVec,
				vecField.Name,
				coll.metricType,
				topk,
				sp,
			)
			if err != nil {
				fmt.Printf("search failed, no: %d, nq: %d err: %s\n", i, nq, err)
			} else {
				fmt.Printf("search success, %s, nq: %d, len: %d, last.len:%d\n", time.Since(start), len(result), result[0].ResultCount, result[len(result)-1].ResultCount)
			}
		}(i)
	}
	wg.Wait()
}
