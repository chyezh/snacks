package main

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	client, err := minio.New("127.0.0.1:9000", &minio.Options{
		BucketLookup: minio.BucketLookupAuto,
		Creds:        credentials.NewStaticV4("minioadmin", "minioadmin", ""),
	})
	if err != nil {
		panic(err)
	}
	// _, err = client.PutObject(context.TODO(), "a-bucket", "files/insert_log/447031349293500360/447031349293500364/447031349321777298/105/447031349320567088", bytes.NewReader([]byte{}), 0, minio.PutObjectOptions{})
	// if err != nil {
	// 	panic(err)
	// }
	obj, err := client.GetObject(context.TODO(), "a-bucket", "files/insert_log/147031349293500360/447031349293500364/447031349321777298/105/447031349320567088", minio.GetObjectOptions{})
	if err != nil {
		panic(err)
	}
	n, err := obj.Read(make([]byte, 0))
	fmt.Printf("n: %d, err: %v\n", n, err)
}
