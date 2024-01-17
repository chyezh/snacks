package pinecone

import (
	"context"
	"io"
	"net/http"

	"github.com/pkg/errors"
)

type Client struct {
	APIKey    string
	IndexHost string
	Namespace string
}

// restError is the error response from the Pinecone REST API.
type restError struct {
	Code    int
	Message string
}

func (c *Client) doRequest(ctx context.Context, method, methodPath string, payload io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.IndexHost+methodPath, payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}
	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")
	req.Header.Add("Api-Key", c.APIKey)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to do request, method: %s", method)
	}
	return res, nil
}
