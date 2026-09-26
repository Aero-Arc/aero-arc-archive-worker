package archive_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-archive-worker/internal/archive"
)

type source map[string][]byte

func (s source) Read(_ context.Context, _ archive.Job, p archive.Source) ([]byte, error) {
	return s[p.ID], nil
}

type objects struct {
	values map[string][]byte
	order  []string
	fail   int
}

func (o *objects) PutVerified(_ context.Context, k string, b []byte, _ string) error {
	if o.fail > 0 && len(o.order) == o.fail {
		return errors.New("unavailable")
	}
	if old, ok := o.values[k]; ok && !bytes.Equal(old, b) {
		return errors.New("overwrite")
	}
	o.values[k] = append([]byte(nil), b...)
	o.order = append(o.order, k)
	return nil
}
func fixture() (archive.Job, source) {
	j := archive.Job{EventID: "event-1", OperatorID: "operator-1", FlightID: "flight-1", Revision: "1", SnapshotID: "snapshot-1", FinalizedAt: time.Unix(1000, 0).UTC(), Coverage: "complete"}
	s := source{}
	for _, kind := range []string{"flight", "telemetry", "commands", "findings", "assignments", "missions"} {
		b := []byte(`[{"id":"original","occurred_at":"2026-09-26T10:00:00.123456789Z"}]`)
		s[kind] = b
		j.Sources = append(j.Sources, archive.Source{ID: kind, Kind: kind, SHA256: archive.Hash(b), Bytes: int64(len(b))})
	}
	return j, s
}
func TestManifestLastAndDeterministicRetry(t *testing.T) {
	j, s := fixture()
	o := &objects{values: map[string][]byte{}}
	result, err := archive.Build(context.Background(), j, s, o)
	if err != nil {
		t.Fatal(err)
	}
	if o.order[len(o.order)-1] != result.Key {
		t.Fatal("manifest not last")
	}
	again, err := archive.Build(context.Background(), j, s, o)
	if err != nil || result != again {
		t.Fatalf("retry changed identity: %v", err)
	}
	var manifest archive.Manifest
	if err = json.Unmarshal(o.values[result.Key], &manifest); err != nil {
		t.Fatal(err)
	}
	for _, part := range manifest.Objects {
		r, err := gzip.NewReader(bytes.NewReader(o.values[part.Key]))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if err = r.Close(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, s[part.Source.ID]) {
			t.Fatal("source evidence altered")
		}
	}
}
func TestCorruptSourceCannotPublish(t *testing.T) {
	j, s := fixture()
	s["telemetry"] = []byte("[]")
	o := &objects{values: map[string][]byte{}}
	if _, err := archive.Build(context.Background(), j, s, o); err == nil {
		t.Fatal("corrupt source accepted")
	}
	for k := range o.values {
		if bytes.HasSuffix([]byte(k), []byte("manifest.json")) {
			t.Fatal("manifest published")
		}
	}
}
func TestInterruptedUploadCanResume(t *testing.T) {
	j, s := fixture()
	o := &objects{values: map[string][]byte{}, fail: 2}
	if _, err := archive.Build(context.Background(), j, s, o); err == nil {
		t.Fatal("failure ignored")
	}
	o.fail = 0
	if _, err := archive.Build(context.Background(), j, s, o); err != nil {
		t.Fatal(err)
	}
}
func TestCoverageRequiresAllCategoriesAndExplicitGaps(t *testing.T) {
	j, _ := fixture()
	j.Coverage = "partial"
	if j.Validate() == nil {
		t.Fatal("missing gap accepted")
	}
	j.Gaps = []string{"telemetry arrival watermark unknown"}
	if err := j.Validate(); err != nil {
		t.Fatal(err)
	}
	j.Sources = j.Sources[:len(j.Sources)-1]
	if j.Validate() == nil {
		t.Fatal("missing missions accepted")
	}
}
