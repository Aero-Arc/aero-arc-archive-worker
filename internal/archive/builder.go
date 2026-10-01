// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
)

// Snapshot reads one bounded immutable source fragment under API authorization.
type Snapshot interface {
	Read(context.Context, Job, Source) ([]byte, error)
}

// Objects writes immutable objects and verifies bytes retrieved from storage.
type Objects interface {
	PutVerified(context.Context, string, []byte, string) error
}

// Result identifies the verified manifest. Object keys are relative to the configured bucket.
type Result struct {
	Key    string `json:"key"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// PublishManifest must fence publication under the current durable job lease.
// It returns verified metadata only after the ready result commits.
type PublishManifest func(context.Context, string, []byte) (Result, error)

// Build verifies snapshot fragments, uploads deterministic compressed chunks,
// and passes the manifest to the fenced publisher last. Partial failures are safe to retry with the
// identical job; a mutable snapshot or corrupt upload fails without publication.
func Build(ctx context.Context, j Job, source Snapshot, objects Objects, publish PublishManifest) (Result, error) {
	if publish == nil {
		return Result{}, fmt.Errorf("fenced manifest publisher required")
	}
	if err := j.Validate(); err != nil {
		return Result{}, err
	}
	prefix := fmt.Sprintf("flights/%s/%s/%s/%s", j.OperatorID, j.FlightID, j.Revision, j.Fingerprint())
	manifest := Manifest{SchemaVersion: 1, Job: j, Objects: make([]Object, 0, len(j.Sources))}
	for _, s := range j.Sources {
		raw, err := source.Read(ctx, j, s)
		if err != nil {
			return Result{}, fmt.Errorf("source %s: %w", s.ID, err)
		}
		if int64(len(raw)) != s.Bytes || Hash(raw) != s.SHA256 {
			return Result{}, fmt.Errorf("source %s integrity mismatch", s.ID)
		}
		if !json.Valid(raw) {
			return Result{}, fmt.Errorf("source %s is not JSON", s.ID)
		}
		var records []json.RawMessage
		if err := json.Unmarshal(raw, &records); err != nil || records == nil {
			return Result{}, fmt.Errorf("source %s must be a JSON record array", s.ID)
		}
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err = w.Write(raw); err != nil {
			return Result{}, err
		}
		if err = w.Close(); err != nil {
			return Result{}, err
		}
		b := buf.Bytes()
		key := prefix + "/" + s.Kind + "/" + Hash(b) + ".json.gz"
		if err = objects.PutVerified(ctx, key, b, "application/gzip"); err != nil {
			return Result{}, err
		}
		manifest.Objects = append(manifest.Objects, Object{Source: s, Key: key, SHA256: Hash(b), Bytes: int64(len(b)), Encoding: "gzip"})
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return Result{}, err
	}
	key := prefix + "/manifest.json"
	return publish(ctx, key, raw)
}
