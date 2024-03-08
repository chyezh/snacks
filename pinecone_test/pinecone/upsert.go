package pinecone

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

type UpsertRequest struct {
	Namespace string         `json:"namespace"`
	Vectors   []UpsertVector `json:"vectors"`
	Done      chan struct{}  `json:"-"`
}

type UpsertVector struct {
	ID       string            `json:"id"`
	Values   []float32         `json:"values"`
	Metadata map[string]string `json:"metadata"`
}

type UpsertResult struct {
	UpsertedCount int `json:"upsertedCount"`
}

func (c *Client) Upsert(ctx context.Context, request UpsertRequest) (*UpsertResult, error) {
	defer close(request.Done)

	req, err := json.Marshal(request)
	if err != nil {
		return nil, errors.Errorf("failed to marshal upsert request, error: %+v", err)
	}
	res, err := c.doRequest(ctx, "POST", "/vectors/upsert", bytes.NewReader(req))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		var result UpsertResult
		if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
			return nil, errors.Wrap(err, "failed to decode error response at query OK response")
		}
		return &result, nil
	}
	var e restError
	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		return nil, errors.Wrapf(err, "failed to decode error response at upsert failure response, http status: %d", res.StatusCode)
	}
	return nil, errors.Errorf("failed to upsert, http status: %d, code: %d, msg: %s", res.StatusCode, e.Code, e.Message)
}
