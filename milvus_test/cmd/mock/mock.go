package main

import (
	"context"
	"fmt"
	"time"

	"github.com/chyezh/snacks/milvus_test/util/collections"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/remeh/sizedwaitgroup"
)

func main() {
	ctx := context.Background()

	cfg := client.Config{
		Address: "localhost:19530",
	}

	wg := sizedwaitgroup.New(1)
	for i := 0; i < 1; i++ {
		time.Sleep(time.Second)
		wg.Add()
		go func(i int) {
			defer wg.Done()
			if err := mockOneCollectionPerUser(ctx, fmt.Sprintf("user3_%d", i), cfg.Copy()); err != nil {
				fmt.Printf("fail to mock user %d, %s\n", i, err)
			}
		}(i)
	}
	wg.Wait()
}

func mockOneCollectionPerUser(ctx context.Context, username string, c client.Config) error {
	cli, err := client.NewClient(ctx, c)
	if err != nil {
		return err
	}
	password := "password"
	if err := cli.CreateCredential(ctx, username, password); err != nil {
		return err
	}
	cli.AddUserRole(ctx, username, "admin")
	database := fmt.Sprintf("db_%s", username)
	if err := cli.CreateDatabase(ctx, database); err != nil {
		return err
	}
	cli.Close()
	c.Username = username
	c.Password = password
	// c.DBName = database
	cli, err = client.NewClient(ctx, c)
	if err != nil {
		return err
	}
	return collections.MockCollection(ctx, collections.Book, 1, cli, collections.OptName(fmt.Sprintf("book_%s", username)))
}
