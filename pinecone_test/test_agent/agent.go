package testagent

import (
	"context"

	"pinecone_test/pinecone"

	"github.com/pkg/errors"
)

type Agent struct {
	client    *pinecone.Client
	Namespace string
}

func getError(err error) string {
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "timeout"
		}
		return "error"
	}
	return "success"
}
