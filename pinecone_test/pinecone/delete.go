package pinecone

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

type DeleteRequest struct {
	Namespace string            `json:"namespace"`
	IDs       []string          `json:"ids,omitempty"`
	Filter    map[string]string `json:"filter,omitempty"`
	DeleteAll bool              `json:"deleteAll"`
}

func (c *Client) Delete(ctx context.Context, request DeleteRequest) error {
	req, err := json.Marshal(request)
	if err != nil {
		return errors.Errorf("failed to marshal delete request, error: %+v", err)
	}
	res, err := c.doRequest(ctx, "POST", "/vectors/delete", bytes.NewReader(req))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		return nil
	}
	var e restError
	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
		return errors.Wrapf(err, "failed to decode error response at delete failure response, http status: %d", res.StatusCode)
	}
	return errors.Errorf("failed to delete, http status: %d, code: %d, msg: %s", res.StatusCode, e.Code, e.Message)
}
