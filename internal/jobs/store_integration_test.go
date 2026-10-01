//go:build integration

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aero-arc/aero-arc-archive-worker/internal/archive"
	"github.com/aero-arc/aero-arc-archive-worker/internal/objectstore"
	"github.com/jackc/pgx/v5"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDurableAdmissionAndLeaseTakeover(t *testing.T) {
	dsn := os.Getenv("AERO_ARCHIVE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AERO_ARCHIVE_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.DB.Close()
	if _, err = s.DB.Exec(ctx, "TRUNCATE archive_jobs"); err != nil {
		t.Fatal(err)
	}
	j := archive.Job{EventID: "event", OperatorID: "operator", FlightID: "flight", Revision: "1", SnapshotID: "snapshot", FinalizedAt: time.Now().UTC(), Coverage: "complete"}
	for _, k := range []string{"flight", "telemetry", "commands", "findings", "assignments", "missions"} {
		j.Sources = append(j.Sources, archive.Source{ID: k, Kind: k, SHA256: archive.Hash([]byte("[]")), Bytes: 2})
	}
	if _, err = s.Enqueue(ctx, j); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Enqueue(ctx, j); err != nil {
		t.Fatal(err)
	}
	other := j
	other.SnapshotID = "other"
	if _, err = s.Enqueue(ctx, other); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict=%v", err)
	}
	first, err := s.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("double claim %v", err)
	}
	if _, err = s.DB.Exec(ctx, "UPDATE archive_jobs SET lease_until=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	if err = s.Renew(ctx, first); !errors.Is(err, ErrLease) {
		t.Fatalf("revived expired lease: %v", err)
	}
	replacement, err := s.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(ctx, first, archive.Result{}, nil); !errors.Is(err, ErrLease) {
		t.Fatalf("stale commit %v", err)
	}
	if err = s.Finish(ctx, replacement, archive.Result{}, errors.New("object store unavailable")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec(ctx, "UPDATE archive_jobs SET available_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	replacement, err = s.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result := archive.Result{Key: "manifest.json", SHA256: archive.Hash([]byte("manifest")), Bytes: 8}
	if err = s.Finish(ctx, replacement, result, nil); err != nil {
		t.Fatal(err)
	}
	recovered, err := s.Get(ctx, j.EventID)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(recovered)
	if recovered.State != "ready" || recovered.Result == nil || *recovered.Result != result {
		t.Fatalf("bad ready record %s", raw)
	}
	if _, err = s.Claim(ctx); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("ready reclaimed %v", err)
	}
}

type publicationObjects struct {
	calls int
	delay time.Duration
	renew func() error
}

func (o *publicationObjects) PutVerified(context.Context, string, []byte, string) error {
	o.calls++
	if o.renew != nil {
		if err := o.renew(); err != nil {
			return err
		}
	}
	time.Sleep(o.delay)
	return nil
}

func TestManifestPublicationFencesExpiredAndReplacedWorkers(t *testing.T) {
	dsn := os.Getenv("AERO_ARCHIVE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AERO_ARCHIVE_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.DB.Close()
	if _, err = s.DB.Exec(ctx, "TRUNCATE archive_jobs"); err != nil {
		t.Fatal(err)
	}
	j := archive.Job{EventID: "event", OperatorID: "operator", FlightID: "flight", Revision: "1", SnapshotID: "snapshot", FinalizedAt: time.Now().UTC(), Coverage: "complete"}
	for _, k := range []string{"flight", "telemetry", "commands", "findings", "assignments", "missions"} {
		j.Sources = append(j.Sources, archive.Source{ID: k, Kind: k, SHA256: archive.Hash([]byte("[]")), Bytes: 2})
	}
	if _, err = s.Enqueue(ctx, j); err != nil {
		t.Fatal(err)
	}
	first, err := s.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Exec(ctx, "UPDATE archive_jobs SET lease_until=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	o := &publicationObjects{}
	if _, err = s.Publish(ctx, first, "prefix/manifest.json", []byte("{}"), o); !errors.Is(err, ErrLease) || o.calls != 0 {
		t.Fatalf("expired worker published: %v calls=%d", err, o.calls)
	}
	replacement, err := s.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Publish(ctx, first, "prefix/manifest.json", []byte("{}"), o); !errors.Is(err, ErrLease) || o.calls != 0 {
		t.Fatalf("retired generation published: %v", err)
	}
	// Expiry during an upload may leave a candidate, but cannot publish readiness.
	if _, err = s.DB.Exec(ctx, "UPDATE archive_jobs SET lease_until=clock_timestamp()+interval '50 milliseconds'"); err != nil {
		t.Fatal(err)
	}
	o.delay = 100 * time.Millisecond
	if _, err = s.Publish(ctx, replacement, "prefix/manifest.json", []byte("{}"), o); !errors.Is(err, ErrLease) {
		t.Fatalf("late upload became ready: %v", err)
	}
	record, err := s.Get(ctx, j.EventID)
	if err != nil || record.State == "ready" || record.Result != nil {
		t.Fatalf("stale candidate discoverable: %+v %v", record, err)
	}
	current, err := s.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	o.delay = 0
	// Renewal must remain available while publication is inside object I/O.
	o.renew = func() error {
		renewCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		return s.Renew(renewCtx, current)
	}
	if _, err = s.Publish(ctx, current, "probe/manifest.json", []byte("{}"), o); err != nil {
		t.Fatalf("upload blocked its own renewal: %v", err)
	}
	// Use another job for the real filesystem verification below.
	j.EventID = "local-event"
	j.Revision = "2"
	if _, err = s.Enqueue(ctx, j); err != nil {
		t.Fatal(err)
	}
	current, err = s.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	local, err := objectstore.Open(ctx, "local", dir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = local.Close() }()
	result, err := s.Publish(ctx, current, "prefix/manifest.json", []byte("{}"), local)
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(dir, result.Key)); err != nil || archive.Hash(raw) != result.SHA256 {
		t.Fatalf("published bytes not verified: %v", err)
	}
	if result.Key != "prefix/generations/1/manifest.json" {
		t.Fatalf("manifest key lacks generation: %s", result.Key)
	}
	record, err = s.Get(ctx, j.EventID)
	if err != nil || record.State != "ready" || record.Result == nil || *record.Result != result {
		t.Fatalf("publication missing: %+v %v", record, err)
	}
}
