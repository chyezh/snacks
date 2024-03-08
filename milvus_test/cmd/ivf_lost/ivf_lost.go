package main

import (
	"context"
	"fmt"
	"log"

	"github.com/chyezh/snacks/milvus_test/util/collections"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

func main() {
	ctx := context.Background()
	c, err := client.NewClient(ctx, client.Config{
		Address: "localhost:19530",
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		c.DropCollection(ctx, "book")
		if err := collections.MockCollection(ctx, collections.Book, 6000, c, collections.OptAutoID(false), collections.OptName("book"), collections.OptDIM(128)); err != nil {
			panic(err)
		}

		sp, _ := entity.NewIndexIvfFlatSearchParam(10)
		topk := 5
		vec := make([]float32, 128)
		for i := 0; i < len(vec); i++ {
			vec[i] = 1
		}

		result, err := c.Search(ctx, "book", nil, "word_count > 500", []string{"word_count", "book_intro"}, []entity.Vector{
			entity.FloatVector(vec),
		}, "book_intro", entity.L2, topk, sp)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", result)
		if result[0].ResultCount != topk {
			panic(fmt.Sprintf("don't equal, %d, %d", result[0].ResultCount, topk))
		}

		fmt.Printf("test pass %d\n", i)
	}
}
