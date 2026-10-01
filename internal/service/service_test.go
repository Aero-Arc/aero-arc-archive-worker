package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestJobRoutesRequireBearerCredential(t *testing.T) {
	s := &Service{Token: strings.Repeat("x", 24)}
	h := s.Handler()
	for _, header := range []string{"", s.Token, "Bearer wrong"} {
		r := httptest.NewRequest(http.MethodPost, "/internal/v1/archive-jobs", strings.NewReader("{}"))
		r.Header.Set("Authorization", header)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("unauthorized request returned %d", w.Code)
		}
	}
}

func TestBuildCompletionDoesNotCancelInFlightRenewal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ticks := make(chan time.Time, 1)
	stop := make(chan struct{})
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- renewLease(ctx, stop, ticks, func(renewal context.Context) error {
			close(started)
			select {
			case <-release:
				return renewal.Err()
			case <-renewal.Done():
				return renewal.Err()
			}
		})
	}()
	ticks <- time.Now()
	<-started
	close(stop)
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("normal stop canceled renewal: %v", err)
	}
}
func TestRenewalStillReportsLeaseLoss(t *testing.T) {
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	want := errors.New("lease lost")
	if err := renewLease(context.Background(), make(chan struct{}), ticks, func(context.Context) error { return want }); !errors.Is(err, want) {
		t.Fatalf("lease loss hidden: %v", err)
	}
}
