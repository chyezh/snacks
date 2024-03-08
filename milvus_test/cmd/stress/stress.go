package main

import (
	"context"
	"flag"
	"time"

	"github.com/chyezh/snacks/milvus_test/util/stress"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"golang.org/x/time/rate"
)

var (
	concurrency = flag.Int("concurrency", 10, "concurrency")
	rateMS      = flag.Int("rate-ms", 10, "rate-ms")
	count       = flag.Int("count", 10000, "count")
)

func main() {
	ctx := context.Background()
	st := stress.NewStressTest()
	c, err := client.NewClient(ctx, client.Config{
		Address: "localhost:9401",
		APIKey:  "9a0523892543e3ebeb2621f4b0025d32f82239a437eed64eff6265b9692df23232aa31fa7701f280e7b0731694bbf61de51aa321",
	})
	if err != nil {
		panic(err)
	}
	if err := st.ListCollections(ctx, c, stress.StressTestConfig{
		Concurrency: *concurrency,
		Count:       *count,
		RateLimit:   rate.Every(time.Duration(*rateMS) * time.Millisecond),
	}); err != nil {
		panic(err)
	}
}
