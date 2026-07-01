// Package apiclient is a thin HTTP client for the pbm's web API (hosted on ctrl-agent).
package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/percona/percona-backup-mongodb/x/pbm/api"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// requestTimeout bounds a single HTTP request to a ctrl-agent.
const requestTimeout = 5 * time.Second

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

// Status fetches every cluster member from the leader's /status endpoint.
func (c *Client) Status(ctx context.Context) ([]status.AgentInfo, error) {
	var members []status.AgentInfo
	if err := c.get(ctx, "/status", &members); err != nil {
		return nil, err
	}
	return members, nil
}

// get issues GET path against each endpoint in turn, following at most one
// leader redirect, and decodes a 200 response body into out.
func (c *Client) get(ctx context.Context, path string, out any) error {
	var errs []error
	for _, ep := range c.endpoints {
		err := c.getFrom(ctx, normalizeEndpoint(ep), path, out, true)
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

// getFrom issues GET base+path. On 421 it follows the leader redirect once
// by using recursive call with redirect=false.
func (c *Client) getFrom(ctx context.Context, base, path string, out any, redirect bool) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
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
		return c.getFrom(ctx, normalizeEndpoint(rr.Leader.Address), path, out, false)

	case http.StatusServiceUnavailable:
		return errors.New("no leader elected yet")

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
