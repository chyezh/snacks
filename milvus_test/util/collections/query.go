package collections

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/remeh/sizedwaitgroup"
	"golang.org/x/time/rate"
)

func (coll *Collection) TestQuery(ctx context.Context, r *rate.Limiter, key string, exprType string, concurrency int, limit int, count int) {
	wg := sizedwaitgroup.New(concurrency)
	pkName := coll.schema.PKFieldName()

	for i := 0; i < count; i++ {
		if err := r.Wait(ctx); err != nil {
			panic(err)
		}
		wg.Add()
		go func(i int) {
			defer wg.Done()
			start := time.Now()
			var expr string
			if exprType == "in" {
				expr = randKeyInQueryExpr(key)
			} else if exprType == "range" {
				expr = randRangeQueryExpr(key)
			} else {
				panic("illegal expr type")
			}
			result, err := coll.cli.Query(ctx, coll.Name(), []string{}, expr, []string{coll.schema.PKFieldName()}, client.WithLimit(int64(limit)))
			if err != nil {
				fmt.Printf("query failed, no: %d, err: %s\n", i, err)
			} else {
				fmt.Printf("query success, %s, count: %d\n", time.Since(start), result.GetColumn(pkName).Len())
			}
		}(i)
	}
	wg.Wait()
}
