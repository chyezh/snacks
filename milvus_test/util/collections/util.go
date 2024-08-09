package collections

import (
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"golang.org/x/net/context"
)

// Mock a Collection
func MockCollection(ctx context.Context, e collectionEnum, count int, cli client.Client, opts ...func(opt *options)) error {
	book := NewCollection(e, cli, opts...)
	book.DropCollection(ctx)
	if err := book.CreateCollection(ctx); err != nil {
		return err
	}
	if err := book.ApplyRandomCase(ctx, count, false); err != nil {
		return err
	}
	if err := book.ApplyIndex(ctx); err != nil {
		return err
	}
	return book.Load(ctx)
}

func DeleteCollectionRows(ctx context.Context, e collectionEnum, count int, cli client.Client, opts ...func(opt *options)) error {
	book := NewCollection(e, cli, opts...)
	return book.DeleteRows(ctx, 1000)
}

func UpsertCollectionRows(ctx context.Context, e collectionEnum, count int, cli client.Client, opts ...func(opt *options)) error {
	book := NewCollection(e, cli, opts...)
	err := book.ApplyRandomCase(ctx, count, true)
	if err != nil {
		panic(err)
	}
	return nil
}
