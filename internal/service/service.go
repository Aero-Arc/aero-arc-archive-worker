// This Source Code Form is subject to the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package service

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aero-arc/aero-arc-archive-worker/internal/archive"
	"github.com/aero-arc/aero-arc-archive-worker/internal/jobs"
	"github.com/jackc/pgx/v5"
)

// Service accepts authorized archive requests and executes durable jobs.
type Service struct {
	Jobs    *jobs.Store
	Source  archive.Snapshot
	Objects archive.Objects
	Token   string
	Log     *slog.Logger
}

// Handler exposes health and authenticated job acceptance/status routes.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Jobs.DB.Ping(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("POST /internal/v1/archive-jobs", s.submit)
	mux.HandleFunc("GET /internal/v1/archive-jobs/{id}", s.status)
	return mux
}
func (s *Service) authorized(w http.ResponseWriter, r *http.Request) bool {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	token := strings.TrimPrefix(header, "Bearer ")
	if len(s.Token) < 24 || subtle.ConstantTimeCompare([]byte(token), []byte(s.Token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}
func reply(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
func (s *Service) submit(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	dec.DisallowUnknownFields()
	var j archive.Job
	if err := dec.Decode(&j); err != nil {
		http.Error(w, "invalid archive request", http.StatusBadRequest)
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		http.Error(w, "trailing request content", http.StatusBadRequest)
		return
	}
	if err := j.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := s.Jobs.Enqueue(r.Context(), j)
	if errors.Is(err, jobs.ErrConflict) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		s.Log.Error("archive admission failed", "error", err)
		http.Error(w, "archive store unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Location", "/internal/v1/archive-jobs/"+j.EventID)
	reply(w, 202, result)
}
func (s *Service) status(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	record, err := s.Jobs.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, pgx.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, "archive store unavailable", http.StatusServiceUnavailable)
		return
	}
	reply(w, 200, record)
}

// Run drains jobs until cancellation. Each attempt renews its fenced lease and
// has a finite execution budget; failures remain durable for retry.
func (s *Service) Run(ctx context.Context) {
	for ctx.Err() == nil {
		job, err := s.Jobs.Claim(ctx)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) && ctx.Err() == nil {
				s.Log.Error("archive claim failed", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		s.execute(ctx, job)
	}
}
func (s *Service) execute(ctx context.Context, job jobs.Record) {
	attempt, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	renewDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-attempt.Done():
				renewDone <- nil
				return
			case <-ticker.C:
				renewal, stop := context.WithTimeout(attempt, 5*time.Second)
				err := s.Jobs.Renew(renewal, job)
				stop()
				if err != nil {
					cancel()
					renewDone <- err
					return
				}
			}
		}
	}()
	result, err := archive.Build(attempt, job.Job, s.Source, s.Objects)
	cancel()
	if renewErr := <-renewDone; renewErr != nil {
		s.Log.Error("archive lease lost", "event_id", job.Job.EventID, "error", renewErr)
		return
	}
	if ctx.Err() != nil {
		return
	}
	finish, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if finishErr := s.Jobs.Finish(finish, job, result, err); finishErr != nil {
		s.Log.Error("archive commit failed", "event_id", job.Job.EventID, "error", finishErr)
		return
	}
	if err != nil {
		s.Log.Warn("archive retry scheduled", "event_id", job.Job.EventID, "error", err)
	} else {
		s.Log.Info("archive ready", "event_id", job.Job.EventID, "manifest", result.Key)
	}
}

// Serve binds management and internal HTTP routes and shuts down on cancellation.
func (s *Service) Serve(ctx context.Context, address string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{Addr: address, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		case <-stopped:
		}
	}()
	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		cancel()
		<-done
		return fmt.Errorf("serve archive worker: %w", err)
	}
	<-done
	return nil
}
