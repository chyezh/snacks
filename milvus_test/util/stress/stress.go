package stress

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/remeh/sizedwaitgroup"
	"golang.org/x/time/rate"
)

type StressTestConfig struct {
	Concurrency int
	Count       int
	RateLimit   rate.Limit
}

type StressTest struct{}

func NewStressTest() *StressTest {
	return &StressTest{}
}

func (*StressTest) ListCollections(ctx context.Context, c client.Client, config StressTestConfig) error {
	wg := sizedwaitgroup.New(config.Concurrency)
	r := rate.NewLimiter(config.RateLimit, 10)
	for i := 0; i < config.Count; i++ {
		if err := r.Wait(ctx); err != nil {
			panic(err)
		}
		wg.Add()
		func(i int) {
			defer wg.Done()
			_, err := c.ListCollections(ctx)
			if err != nil {
				fmt.Printf("list connection failed, iter: %d\n", i)
				return
			}
		}(i)
	}
	wg.Wait()
	return nil
}
