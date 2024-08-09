package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var etcdCfg = flag.String("etcd-cfg", "", "")

func main() {
	flag.Parse()
	cfgBytes, err := os.ReadFile(*etcdCfg)
	if err != nil {
		panic(err)
	}
	var cfg clientv3.Config
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", cfg)
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	cfg.Logger = logger
	client, err := clientv3.New(cfg)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	r, err := client.Lease.Grant(ctx, 10)
	if err != nil {
		panic(err)
	}
	_, err = client.Put(ctx, "foo2", "bar", clientv3.WithLease(r.ID))
	if err != nil {
		panic(err)
	}

	ch, err := client.KeepAlive(ctx, r.ID)
	if err != nil {
		panic(err)
	}
	start := time.Now()
	// should not break forever.
	for resp := range ch {
		logger.Info("keep alive", zap.Duration("cost", time.Since(start)), zap.Any("resp", resp))
	}
	logger.Info("keep alive break", zap.Duration("cost", time.Since(start)))
}
