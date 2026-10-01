// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package archive

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPSource reads immutable snapshots from a fixed, trusted API origin.
// It never follows redirects or accepts a caller-supplied object URL.
type HTTPSource struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

// NewHTTPSource validates transport security and bounds response time.
// Plain HTTP is allowed only through the explicit local-development option.
func NewHTTPSource(base, token string, allowHTTP bool) (*HTTPSource, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && (!allowHTTP || u.Scheme != "http")) || len(token) < 24 {
		return nil, fmt.Errorf("valid HTTPS API origin and service credential required")
	}
	return &HTTPSource{BaseURL: strings.TrimRight(base, "/"), Token: token, Client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Read retrieves a fragment by immutable identity, enforcing the declared size.
func (s *HTTPSource) Read(ctx context.Context, j Job, part Source) ([]byte, error) {
	if err := j.Validate(); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.BaseURL+"/internal/v1/archive-snapshots/"+j.SnapshotID+"/fragments/"+part.ID, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.Token)
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("snapshot returned HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, part.Bytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != part.Bytes {
		return nil, fmt.Errorf("snapshot fragment size mismatch")
	}
	return b, nil
}
