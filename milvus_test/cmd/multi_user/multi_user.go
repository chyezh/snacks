package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/remeh/sizedwaitgroup"
)

const (
	testUserFairQuery    = 1
	testUserUnFairQuery  = 2
	testUserFairSearch   = 3
	testUserUnFairSearch = 4

	address  = ""
	user     = "root"
	password = ""
)

func main() {
	userCount := 10
	testType := testUserFairSearch
	// c, err := client.NewClient(context.Background(), client.Config{
	// 	Address:  address,
	// 	Username: "root",
	// 	Password: "",
	// })
	// // c, err := client.NewDefaultGrpcClientWithAuth(context.Background(), "localhost:19530", "root", "Milvus")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println("connected")
	// loadAll(c, userCount)
	// fmt.Println("loaded")

	testFairly(userCount, testType)
}

func loadAll(c client.Client, userCount int) {
	for i := 0; i < userCount; i++ {
		c.UsingDatabase(context.Background(), fmt.Sprintf("user_%d", i))
		if err := c.LoadCollection(context.Background(), "book", true); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d, loaded\n", i)
	}
}

func testFairly(userCount int, testType int) {
	wg := sizedwaitgroup.New(10)
	for i := 0; i < userCount; i++ {
		wg.Add()
		go func(i int) {
			defer wg.Done()
			switch testType {
			case testUserFairQuery:
				testNewUserQuery(i, 1)
			case testUserUnFairQuery:
				testNewUserQuery(i, 6*(i%3+1))
			case testUserFairSearch:
				testNewUserSearch(i, 3, 1)
			case testUserUnFairSearch:
				nq := 1
				if i < 2 {
					nq = 10
				}
				testNewUserSearch(i, 3*(i%3+1), nq)
			}
		}(i)
	}
	wg.Wait()
}

func testNewUserQuery(i int, concurrency int) {
	userName := fmt.Sprintf("user_%d", i)
	c, err := client.NewDefaultGrpcClientWithAuth(context.Background(), address, userName, "password")
	if err != nil {
		log.Fatal(err)
	}
	c.UsingDatabase(context.Background(), userName)
	collectionName := "book"
	// if err := c.LoadCollection(context.Background(), collectionName, false); err != nil {
	// 	log.Fatal(err)
	// }
	wg := sizedwaitgroup.New(concurrency)
	for j := 0; j < 100*concurrency; j++ {
		wg.Add()
		go func(j int) {
			defer wg.Done()
			expr := fmt.Sprintf("book_id > %d", rand.Int31n(int32(j+100)))
			if _, err := c.Query(context.Background(), collectionName, nil, expr, []string{"book_id", "word_count"}); err != nil {
				log.Printf("query failed, user: %s, expr:%s, no: %d", userName, expr, j)
			}
		}(j)
	}
	wg.Wait()
}

func testNewUserSearch(i int, concurrency int, nq int) {
	userName := fmt.Sprintf("user_%d", i)
	c, err := client.NewDefaultGrpcClientWithAuth(context.Background(), address, userName, "password")
	if err != nil {
		log.Fatal(err)
	}
	c.UsingDatabase(context.Background(), userName)
	collectionName := "book"
	// if err := c.LoadCollection(context.Background(), collectionName, false); err != nil {
	// 	log.Fatal(err)
	// }
	wg := sizedwaitgroup.New(concurrency)

	caseCount := 500 * concurrency

	testCase := make([][]entity.Vector, caseCount)
	for i := range testCase {
		searchPatch := make([]entity.Vector, nq)
		for j := range searchPatch {
			newVec := make([]float32, 768)
			for k := range newVec {
				newVec[k] = rand.Float32()
			}
			searchPatch[j] = entity.FloatVector(newVec)
		}
		testCase[i] = searchPatch
	}

	for j := 0; j < caseCount; j++ {
		wg.Add()
		go func(j int) {
			defer wg.Done()
			sp, _ := entity.NewIndexIvfHNSWSearchParam(8, 8)
			if _, err := c.Search(context.Background(), collectionName, nil, "", []string{"book_id", "word_count"}, testCase[j], "book_intro", entity.L2, 5, sp); err != nil {
				log.Printf("query failed, user: %s, no: %d, err: %s", userName, j, err)
			}
		}(j)
	}
	wg.Wait()
}
