package pinecone

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

type QueryRequest struct {
	Namespace       string    `json:"namespace"`
	IncludeValues   bool      `json:"includeValues"`
	IncludeMetadata bool      `json:"includeMetadata"`
	TopK            int       `json:"topK"`
	Vector          []float32 `json:"vector"`
	ID              string    `json:"-"`
}

type QueryResult struct {
	Namespace string            `json:"namespace"`
	Usage     QueryUsage        `json:"usage"`
	Matches   []QueryResultItem `json:"matches"`
}

type QueryUsage struct {
	ReadUnits int `json:"readUnits"`
}

type QueryResultItem struct {
	ID     string    `json:"id"`
	Score  float32   `json:"score"`
	Values []float32 `json:"values"`
}

func (c *Client) Query(ctx context.Context, request QueryRequest) (result *QueryResult, err error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, errors.Errorf("failed to marshal query request, error: %+v", err)
	}
	res, err := c.doRequest(ctx, "POST", "/query", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		var result QueryResult
		if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
			return nil, errors.Wrap(err, "failed to decode error response at query OK response")
		}
		return &result, nil
	}
	var e restError
	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		return nil, errors.Wrapf(err, "failed to decode error response at query failure response, http status: %d", res.StatusCode)
	}
	return nil, errors.Errorf("failed to query, http status: %d, code: %d, msg: %s", res.StatusCode, e.Code, e.Message)
}
