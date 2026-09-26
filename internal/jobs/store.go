// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aero-arc/aero-arc-archive-worker/internal/archive"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrConflict = errors.New("archive event or revision identity conflict")
var ErrLease = errors.New("archive job lease lost")

// Store owns the durable archive inbox and fenced job lifecycle.
type Store struct{ DB *pgxpool.Pool }

// Record exposes durable progress independently of worker process lifetime.
type Record struct {
	Job        archive.Job     `json:"job"`
	State      string          `json:"state"`
	Attempts   int             `json:"attempts"`
	Generation int64           `json:"-"`
	Result     *archive.Result `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// Open connects to PostgreSQL and applies the initial idempotent schema.
func Open(ctx context.Context, dsn string) (*Store, error) {
	if dsn == "" {
		return nil, fmt.Errorf("database URL required")
	}
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{DB: db}
	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS archive_jobs (
 event_id text PRIMARY KEY, operator_id text NOT NULL, flight_id text NOT NULL, revision text NOT NULL,
 fingerprint text NOT NULL, payload jsonb NOT NULL,
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','working','retrying','ready')),
 attempts integer NOT NULL DEFAULT 0, generation bigint NOT NULL DEFAULT 0,
 lease_until timestamptz, available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 result jsonb, last_error text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(operator_id,flight_id,revision)
 ); CREATE INDEX IF NOT EXISTS archive_jobs_due ON archive_jobs(available_at) WHERE state <> 'ready';`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Enqueue durably admits an immutable event or returns its exact previous record.
// A duplicate revision with different identity/content cannot replace the job.
func (s *Store) Enqueue(ctx context.Context, j archive.Job) (Record, error) {
	if err := j.Validate(); err != nil {
		return Record{}, err
	}
	raw, err := json.Marshal(j)
	if err != nil {
		return Record{}, err
	}
	_, err = s.DB.Exec(ctx, `INSERT INTO archive_jobs(event_id,operator_id,flight_id,revision,fingerprint,payload) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, j.EventID, j.OperatorID, j.FlightID, j.Revision, j.Fingerprint(), raw)
	if err != nil {
		return Record{}, err
	}
	r, err := s.Get(ctx, j.EventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrConflict
	}
	if err != nil {
		return Record{}, err
	}
	if r.Job.Fingerprint() != j.Fingerprint() {
		return Record{}, ErrConflict
	}
	return r, nil
}

func scan(row pgx.Row) (Record, error) {
	var r Record
	var payload, result []byte
	if err := row.Scan(&payload, &r.State, &r.Attempts, &r.Generation, &result, &r.Error); err != nil {
		return r, err
	}
	if err := json.Unmarshal(payload, &r.Job); err != nil {
		return r, err
	}
	if len(result) > 0 {
		if err := json.Unmarshal(result, &r.Result); err != nil {
			return r, err
		}
	}
	return r, nil
}

const columns = `payload,state,attempts,generation,result,last_error`

// Get returns one persisted archive status or pgx.ErrNoRows.
func (s *Store) Get(ctx context.Context, id string) (Record, error) {
	return scan(s.DB.QueryRow(ctx, `SELECT `+columns+` FROM archive_jobs WHERE event_id=$1`, id))
}

// Claim leases one due job using database time and skips work owned by others.
func (s *Store) Claim(ctx context.Context) (Record, error) {
	return scan(s.DB.QueryRow(ctx, `WITH candidate AS (
 SELECT event_id FROM archive_jobs WHERE state <> 'ready' AND available_at <= clock_timestamp()
 AND (lease_until IS NULL OR lease_until <= clock_timestamp()) ORDER BY available_at,event_id FOR UPDATE SKIP LOCKED LIMIT 1
 ) UPDATE archive_jobs j SET state='working',attempts=attempts+1,generation=generation+1,lease_until=clock_timestamp()+interval '45 seconds'
 FROM candidate c WHERE j.event_id=c.event_id RETURNING j.`+`payload,j.state,j.attempts,j.generation,j.result,j.last_error`))
}

// Renew extends only the current unexpired worker generation.
func (s *Store) Renew(ctx context.Context, r Record) error {
	tag, err := s.DB.Exec(ctx, `UPDATE archive_jobs SET lease_until=clock_timestamp()+interval '45 seconds' WHERE event_id=$1 AND generation=$2 AND state='working' AND lease_until>clock_timestamp()`, r.Job.EventID, r.Generation)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLease
	}
	return nil
}

// Finish publishes verified manifest metadata only under the live job lease.
// A failed attempt remains durable and retries with bounded exponential backoff.
func (s *Store) Finish(ctx context.Context, r Record, result archive.Result, buildErr error) error {
	var raw []byte
	state := "ready"
	message := ""
	if buildErr != nil {
		state = "retrying"
		message = buildErr.Error()
		if len(message) > 2048 {
			message = message[:2048]
		}
	} else {
		var err error
		raw, err = json.Marshal(result)
		if err != nil {
			return err
		}
	}
	tag, err := s.DB.Exec(ctx, `UPDATE archive_jobs SET state=$3,result=$4,last_error=$5,lease_until=NULL,
 available_at=clock_timestamp()+make_interval(secs => LEAST(300,power(2,LEAST(attempts,8)))::double precision)
 WHERE event_id=$1 AND generation=$2 AND state='working' AND lease_until>clock_timestamp()`, r.Job.EventID, r.Generation, state, raw, message)
	if err != nil {
		return fmt.Errorf("finish archive: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLease
	}
	return nil
}
