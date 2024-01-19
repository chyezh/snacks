package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"pinecone_test/dataset/cohere"
	"pinecone_test/pinecone"
	testagent "pinecone_test/test_agent"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
)

const (
	batchSize = 5000000
)

var queryLimit = []int{100, 500, 1000}

func main() {
	initLogger()
	startHTTPServer()
	defer zap.L().Sync()
	if len(os.Args) < 3 {
		panic("usage: ./bin <path>")
	}
	r, err := cohere.NewReader(os.Args[1])
	if err != nil {
		panic(err)
	}
	testCase := cohere.NewTestCase(r, "htest")
	defer testCase.Close()

	f, err := os.OpenFile("op.log", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(f)

	a := &testagent.Agent{
		Client: &pinecone.Client{
			APIKey:    "35f8834b-7bf6-4b91-a67e-69e89fd9bfb3",
			IndexHost: "https://cohere-sc3ybx5.svc.apw5-4e34-81fa.pinecone.io",
		},
		Mu:       sync.Mutex{},
		OpLogger: w,
	}
	defer func() {
		a.Mu.Lock()
		defer w.Flush()
		defer f.Close()
		a.Mu.Unlock()
	}()

	for i := 0; i < 6; i++ {
		ReadWriteTask(a, testCase)
		WriteOnlyTask(a, testCase)
		time.Sleep(30 * time.Minute)
		ReadOnlyTask(a, testCase)
	}
}

func ReadWriteTask(a *testagent.Agent, testCase *cohere.TestCase) {
	wg := sync.WaitGroup{}
	wg.Add(3)
	defer wg.Wait()

	go func() {
		defer wg.Done()
		upsertTask := a.UpsertTask("rw", 200, 10000)
		upsertTask.Do(testCase.UpsertChan(batchSize))
	}()

	go func() {
		defer wg.Done()
		deleteTask := a.DeleteTask("rw", 100, 500)
		deleteTask.Do(testCase.DeleteChan(batchSize / 20))
	}()

	time.Sleep(5 * time.Second)
	go func() {
		defer wg.Done()
		for _, limit := range queryLimit {
			queryTask := a.QueryTask("rw", 200, rate.Limit(limit))
			queryTask.Do(testCase.QueryChan(limit * 30))
		}
	}()
}

func WriteOnlyTask(a *testagent.Agent, testCase *cohere.TestCase) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		upsertTask := a.UpsertTask("wo", 200, 10000)
		upsertTask.Do(testCase.UpsertChan(batchSize))
	}()

	go func() {
		defer wg.Done()
		deleteTask := a.DeleteTask("wo", 100, 500)
		deleteTask.Do(testCase.DeleteChan(batchSize / 20))
	}()
}

func ReadOnlyTask(a *testagent.Agent, testCase *cohere.TestCase) {
	queryTask := a.QueryTask("cro", 200, rate.Limit(5))
	queryTask.Do(testCase.QueryChan(5 * 30))

	for _, limit := range queryLimit {
		queryTask := a.QueryTask("ro", 200, rate.Limit(limit))
		queryTask.Do(testCase.QueryChan(limit * 30))
	}
}

func initLogger() {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig = encoderCfg
	logger, err := cfg.Build()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	zap.ReplaceGlobals(logger)
}

func startHTTPServer() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", 19470), nil); err != nil {
			panic(err)
		}
	}()
}
