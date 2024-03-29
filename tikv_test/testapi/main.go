package main

import (
	"context"
	"fmt"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv"
)

func main() {
	writeTxn(context.Background())
	getTxn(context.Background())
	writeRaw(context.Background())
}

func getTxnV2(ctx context.Context) {
	client, err := txnkv.NewClient([]string{"127.0.0.1:2379"}, txnkv.WithAPIVersion(kvrpcpb.APIVersion_V2))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	ts, err := client.CurrentTimestamp("global")
	if err != nil {
		panic(err)
	}
	txn := client.GetSnapshot(ts)
	val, err := txn.Get(ctx, []byte("key"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))
}

func writeTxnV2(ctx context.Context) {
	client, err := txnkv.NewClient([]string{"127.0.0.1:2379"}, txnkv.WithAPIVersion(kvrpcpb.APIVersion_V2))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	txn, err := client.Begin()
	if err != nil {
		panic(err)
	}
	err = txn.Set([]byte("key"), []byte("value_txn_v2"))
	if err != nil {
		panic(err)
	}
	err = txn.Commit(ctx)
	if err != nil {
		panic(err)
	}
}

func getTxn(ctx context.Context) {
	client, err := txnkv.NewClient([]string{"127.0.0.1:2379"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	ts, err := client.CurrentTimestamp("global")
	if err != nil {
		panic(err)
	}
	txn := client.GetSnapshot(ts)
	val, err := txn.Get(ctx, []byte("key"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))
}

func writeTxn(ctx context.Context) {
	client, err := txnkv.NewClient([]string{"127.0.0.1:2379"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	txn, err := client.Begin()
	if err != nil {
		panic(err)
	}
	err = txn.Set([]byte("key"), []byte("value_txn"))
	if err != nil {
		panic(err)
	}
	err = txn.Commit(ctx)
	if err != nil {
		panic(err)
	}
}

func writeRaw(ctx context.Context) {
	client, err := rawkv.NewClientWithOpts(ctx, []string{"127.0.0.1:2379"})
	if err != nil {
		panic(err)
	}
	if err := client.Put(ctx, []byte("key"), []byte("value_raw")); err != nil {
		panic(err)
	}
	if val, err := client.Get(ctx, []byte("key")); err != nil {
		panic(err)
	} else {
		fmt.Println(string(val))
	}
}
