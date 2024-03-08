package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/chyezh/snacks/milvus_test/util/collections"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"golang.org/x/time/rate"
)

var (
	limit         = flag.Int("limit", 1000, "limit")
	userCount     = flag.Int("user-count", 10, "user-count")
	topK          = flag.Int("topK", 15, "top-k")
	nq            = flag.Int("nq", 1, "nq")
	count         = flag.Int("count", 500, "count")
	concurrency   = flag.Int("concurrency", 2, "concurrency")
	ratePerSecond = flag.Int("rate", 100, "rate")
	testType      = flag.String("test-type", "search", "search or query")
	key           = flag.String("key", "book_id", "key")
	exprType      = flag.String("expr", "in", "expr type")
)

func main() {
	flag.Parse()
	ctx := context.Background()

	cfg := client.Config{
		Address: "127.0.0.1:19530",
	}

	wg := &sync.WaitGroup{}
	wg.Add(*userCount)
	for i := 0; i < *userCount; i++ {
		time.Sleep(100 * time.Millisecond)
		go func(i int) {
			defer wg.Done()
			if err := testOneCollectionPerUser(ctx, i, cfg.Copy()); err != nil {
				fmt.Printf("fail to mock user %d, %s\n", i, err)
			}
		}(i)
	}
	wg.Wait()
}

func testOneCollectionPerUser(ctx context.Context, i int, c client.Config) error {
	username := fmt.Sprintf("user2_%d", i)
	// database := fmt.Sprintf("db_%s", username)
	collection := fmt.Sprintf("book_%s", username)
	c.Username = username
	c.Password = "password"
	// c.DBName = database

	cli, err := client.NewClient(ctx, c)
	if err != nil {
		return err
	}
	book := collections.NewCollection(collections.Book, cli, collections.OptName(collection))
	r := rate.NewLimiter(rate.Limit(*ratePerSecond), 2)
	if *testType == "search" {
		book.TestSearch(ctx, r, *concurrency, *topK, *nq, *count)
	} else if *testType == "query" {
		book.TestQuery(ctx, r, *key, *exprType, *concurrency, *limit, *count)
	}
	return nil
}
