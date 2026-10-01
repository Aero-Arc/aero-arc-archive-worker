// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package archive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

// MaxSourceBytes bounds each immutable source fragment before compression.
const MaxSourceBytes int64 = 8 << 20

var identity = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)
var digest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Source identifies an immutable API snapshot fragment, not a mutable live query.
type Source struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// Job is the immutable API-owned finalized-flight archive request.
// Revision must change whenever evidence or its coverage changes.
type Job struct {
	EventID     string    `json:"event_id"`
	OperatorID  string    `json:"operator_id"`
	FlightID    string    `json:"flight_id"`
	Revision    string    `json:"revision"`
	SnapshotID  string    `json:"snapshot_id"`
	FinalizedAt time.Time `json:"finalized_at"`
	Coverage    string    `json:"coverage"`
	Gaps        []string  `json:"gaps"`
	Sources     []Source  `json:"sources"`
}

// Validate rejects unbounded, ambiguous, or incomplete archive requests.
func (j Job) Validate() error {
	for _, id := range []string{j.EventID, j.OperatorID, j.FlightID, j.Revision, j.SnapshotID} {
		if !identity.MatchString(id) {
			return fmt.Errorf("invalid archive identity")
		}
	}
	if j.FinalizedAt.IsZero() || (j.Coverage != "complete" && j.Coverage != "partial") {
		return fmt.Errorf("finalization time and explicit coverage required")
	}
	if len(j.Gaps) > 100 || (j.Coverage == "complete" && len(j.Gaps) > 0) || (j.Coverage == "partial" && len(j.Gaps) == 0) {
		return fmt.Errorf("coverage and gaps disagree")
	}
	if len(j.Sources) == 0 || len(j.Sources) > 4096 {
		return fmt.Errorf("snapshot requires 1..4096 bounded fragments")
	}
	seen := map[string]bool{}
	kinds := map[string]bool{}
	for _, s := range j.Sources {
		if !identity.MatchString(s.ID) || seen[s.ID] || !digest.MatchString(s.SHA256) || s.Bytes < 1 || s.Bytes > MaxSourceBytes {
			return fmt.Errorf("invalid or duplicate source fragment")
		}
		switch s.Kind {
		case "telemetry", "commands", "findings", "assignments", "flight", "missions":
		default:
			return fmt.Errorf("unsupported source kind %q", s.Kind)
		}
		seen[s.ID] = true
		kinds[s.Kind] = true
	}
	for _, kind := range []string{"telemetry", "commands", "findings", "assignments", "flight", "missions"} {
		if !kinds[kind] {
			return fmt.Errorf("missing %s snapshot (use an explicit empty record set)", kind)
		}
	}
	return nil
}

// Hash returns the lowercase SHA-256 of exact stored bytes.
func Hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// Fingerprint identifies the entire request independently of JSON whitespace.
func (j Job) Fingerprint() string { b, _ := json.Marshal(j); return Hash(b) }

// Object records both compressed and uncompressed integrity identities.
type Object struct {
	Source   Source `json:"source"`
	Key      string `json:"key"`
	SHA256   string `json:"sha256"`
	Bytes    int64  `json:"bytes"`
	Encoding string `json:"encoding"`
}

// Manifest is the portable, versioned root of a flight evidence bundle.
type Manifest struct {
	SchemaVersion int      `json:"schema_version"`
	Job           Job      `json:"job"`
	Objects       []Object `json:"objects"`
}
