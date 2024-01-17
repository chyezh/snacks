package testagent

import (
	"pinecone_test/pinecone"
)

type Agent struct {
	client     *pinecone.Client
	dataSource <-chan pinecone.UpsertVector
}
