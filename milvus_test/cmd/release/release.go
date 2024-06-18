package main

import (
	"context"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
)

func main() {
	ctx := context.Background()
	cfg := client.Config{
		Address: "localhost:19530",
	}
	cli, err := client.NewClient(ctx, cfg)
	if err != nil {
		panic(err)
	}
	coll, err := cli.ListCollections(ctx)
	for _, c := range coll {
		cli.ReleaseCollection(ctx, c.Name)
	}
}
