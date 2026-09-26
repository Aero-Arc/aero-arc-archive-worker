//go:build integration

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aero-arc/aero-arc-archive-worker/internal/archive"
	"github.com/jackc/pgx/v5"
	"os"
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
