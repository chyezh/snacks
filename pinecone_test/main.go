package main

import (
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig = encoderCfg
	logger, err := cfg.Build()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)
	zap.L().Warn("test")
	zap.L().Info("test")
	// cli := pinecone.Client{
	// 	IndexHost: `https://client-test-sc3ybx5.svc.apw5-4e34-81fa.pinecone.io`,
	// 	APIKey:    "35f8834b-7bf6-4b91-a67e-69e89fd9bfb3",
	// 	Namespace: "test",
	// }
	// ctx := context.Background()

	// for i := 0; i < 10; i++ {
	// 	vector := make([]float32, 128)
	// 	for j := 0; j < 128; j++ {
	// 		vector[j] = rand.Float32()
	// 	}

	// 	err := cli.Delete(ctx, pinecone.DeleteRequest{
	// 		Namespace: "test",
	// 		IDs: []string{
	// 			strconv.FormatInt(int64(i), 10),
	// 		},
	// 		DeleteAll: false,
	// 	})
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	//	for i := 0; i < 10; i++ {
	//		vector := make([]float32, 128)
	//		for j := 0; j < 128; j++ {
	//			vector[j] = rand.Float32()
	//		}
	//
	//		result, err := cli.Query(ctx, pinecone.QueryRequest{
	//			Namespace:       "test",
	//			IncludeValues:   true,
	//			IncludeMetadata: true,
	//			TopK:            10,
	//			Vector:          vector,
	//		})
	//		if err != nil {
	//			panic(err)
	//		}
	//		fmt.Printf("result: %+v\n", result)
	//	}

	// for i := 0; i < 10; i++ {
	// 	vector := make([]float32, 128)
	// 	for j := 0; j < 128; j++ {
	// 		vector[j] = rand.Float32()
	// 	}
	// 	result, err := cli.Upsert(ctx, pinecone.UpsertRequest{
	// 		Namespace: "test",
	// 		Vectors: []pinecone.UpsertVector{
	// 			{
	// 				ID:     strconv.FormatInt(int64(i), 10),
	// 				Values: vector,
	// 			},
	// 		},
	// 	})
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Printf("upserted count: %d\n", result.UpsertedCount)
	// }
}
