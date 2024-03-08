package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
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
	batchSize = 50000
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
			APIKey:    "",
			IndexHost: "",
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

	TopKStart(a, testCase)

	// for i := 0; i < 6; i++ {
	// 	ReadWriteTask(a, testCase)
	// 	WriteOnlyTask(a, testCase)
	// 	time.Sleep(1 * time.Minute)
	// 	ReadOnlyTask(a, testCase)
	// }
}

func TopKStart(a *testagent.Agent, testCase *cohere.TestCase) {
	queryTask := a.QueryTask("cro", 1, rate.Limit(10))
	queryTask.Do(testCase.QueryChan(99))
}

func ColdStart(a *testagent.Agent, testCase *cohere.TestCase) {
	queryTask := a.QueryTask("cro", 1, rate.Limit(10))
	queryTask.Do(testCase.QueryChan(5 * 30))
}

func Multinamespace(a *testagent.Agent, testCase *cohere.TestCase) {
	upsertTask := a.UpsertTask("rw", 200, 50)
	upsertTask.Do(testCase.UpsertChan(batchSize))
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
	queryTask := a.QueryTask("cro", 1, rate.Limit(10))
	queryTask.Do(testCase.QueryChan(5 * 30))

	for _, limit := range queryLimit {
		queryTask := a.QueryTask("ro", 200, rate.Limit(limit))
		queryTask.Do(testCase.QueryChan(limit * 30))
	}
}

func ConsistencyTask(c *pinecone.Client, r cohere.Reader) {
	namespace := "ctest"
	qErrCount := 0
	qCount := 0
	qHit := 0
	qNotHit := 0
	qDCount := 0
	qDHit := 0
	qDNotHit := 0
	for i := 0; i < 100; i++ {
		req := pinecone.UpsertRequest{
			Namespace: namespace,
			Vectors:   make([]pinecone.UpsertVector, 0),
			Done:      make(chan struct{}),
		}
		for msg := range r.Chan() {
			req.Vectors = append(req.Vectors, pinecone.UpsertVector{
				ID:     strconv.FormatInt(int64(msg.Id), 10),
				Values: msg.Emb,
				Metadata: map[string]string{
					"title": msg.Title,
					"langs": strconv.FormatInt(int64(msg.Lang), 10),
				},
			},
			)
			if len(req.Vectors) >= 20 {
				break
			}
		}
		_, err := c.Upsert(context.Background(), req)
		if err != nil {
			zap.L().Error("failed to upsert", zap.Error(err))
			continue
		}
		// Search all inserted vectors.
		for _, v := range req.Vectors {
			resp, err := c.Query(context.Background(), pinecone.QueryRequest{
				Namespace:       namespace,
				TopK:            5,
				Vector:          v.Values,
				IncludeValues:   true,
				IncludeMetadata: true,
			})
			qCount++
			if err != nil {
				zap.L().Error("failed to query", zap.Error(err))
				qErrCount++
				continue
			}
			hit := false
			for _, m := range resp.Matches {
				if m.ID == v.ID {
					hit = true
				}
			}
			if hit {
				qHit++
			} else {
				qNotHit++
			}
		}

		// Delete half of the inserted vectors.
		ids := make([]string, 0)
		for _, v := range req.Vectors[0:10] {
			ids = append(ids, v.ID)
		}

		if err = c.Delete(context.Background(), pinecone.DeleteRequest{
			Namespace: namespace,
			IDs:       ids,
		}); err != nil {
			zap.L().Error("failed to delete", zap.Error(err))
			continue
		}

		for _, v := range req.Vectors[0:10] {
			resp, err := c.Query(context.Background(), pinecone.QueryRequest{
				Namespace:       namespace,
				TopK:            5,
				Vector:          v.Values,
				IncludeValues:   true,
				IncludeMetadata: true,
			})
			qDCount++
			if err != nil {
				zap.L().Error("failed to query", zap.Error(err))
				qErrCount++
				continue
			}
			hit := false
			for _, m := range resp.Matches {
				if m.ID == v.ID {
					hit = true
				}
			}
			if hit {
				qDHit++
			} else {
				qDNotHit++
			}
		}
	}
	fmt.Printf("qErrCount: %d, qCount: %d, qHit: %d, qNotHit: %d, qDCount: %d, qDHit: %d, qDNotHit: %d\n",
		qErrCount, qCount, qHit, qNotHit, qDCount, qDHit, qDNotHit)
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
