// Package apiclient is a thin HTTP client for the pbm's web API (hosted on ctrl-agent).
package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/percona/percona-backup-mongodb/x/pbm/api"
)

// requestTimeout bounds a single HTTP request to a ctrl-agent.
const requestTimeout = 5 * time.Second

// ErrNotFound is returned when the requested resource does not exist (HTTP 404).
var ErrNotFound = errors.New("not found")

// Client talks to the ctrl-agent web API, resolving the leader automatically.
type Client struct {
	endpoints []string
	http      *http.Client
}

// New returns a Client that tries the given endpoints (host:port, optionally
// scheme-prefixed).
func New(endpoints []string) *Client {
	return &Client{
		endpoints: endpoints,
		http:      &http.Client{Timeout: requestTimeout},
	}
}

// get issues GET path, decoding a 200 response body into out.
func (c *Client) get(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

// put issues PUT path with a JSON body, expecting an empty 204 response.
func (c *Client) put(ctx context.Context, path string, body []byte) error {
	return c.do(ctx, http.MethodPut, path, body, nil)
}

// post issues POST path with no body, expecting an empty 204 response.
func (c *Client) post(ctx context.Context, path string) error {
	return c.do(ctx, http.MethodPost, path, nil, nil)
}

// do issues the request against each endpoint in turn, following at most one
// leader redirect per endpoint.
func (c *Client) do(ctx context.Context, method, path string, body []byte, out any) error {
	var errs []error
	for _, ep := range c.endpoints {
		err := c.doFrom(ctx, method, normalizeEndpoint(ep), path, body, out, true)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", ep, err))
	}
	if len(errs) == 0 {
		return errors.New("no api endpoints configured")
	}
	return fmt.Errorf("all api endpoints failed: %w", errors.Join(errs...))
}

// doFrom issues method base+path. On 421 it follows the leader redirect once
// via a recursive call with redirect=false. A 200 body is decoded into out
// (when non-nil); 204 is treated as success.
func (c *Client) doFrom(
	ctx context.Context,
	method, base, path string,
	body []byte,
	out any,
	redirect bool,
) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, base+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		if out != nil {
			if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}
		}
		return nil

	case http.StatusNoContent:
		return nil

	case http.StatusMisdirectedRequest:
		if !redirect {
			return errors.New("redirected to leader more than once")
		}
		var rr api.RoutingResponse
		if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
			return fmt.Errorf("decode leader routing: %w", err)
		}
		if rr.Leader.Address == "" {
			return errors.New("leader routing response missing address")
		}
		return c.doFrom(ctx, method, normalizeEndpoint(rr.Leader.Address), path, body, out, false)

	case http.StatusServiceUnavailable:
		return errors.New("no leader elected yet")

	case http.StatusNotFound:
		return ErrNotFound

	default:
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
}

// normalizeEndpoint prepends http:// when the endpoint carries no scheme.
func normalizeEndpoint(ep string) string {
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return ep
	}
	return "http://" + ep
}
