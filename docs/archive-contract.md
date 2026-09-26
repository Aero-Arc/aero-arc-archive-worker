# Finalized-flight archive contract

The API publishes a stable `flight.finalized` event only after the authoritative flight transition commits. Its delivery outbox submits `POST /internal/v1/archive-jobs` with a dedicated service bearer credential. A 202 means the worker inbox committed, not that a bundle is ready. The API can recover progress through authenticated `GET /internal/v1/archive-jobs/{event_id}`.

A job contains event/operator/flight/revision/snapshot IDs, the recorded finalization time, explicit `complete` or `partial` coverage, gap descriptions, and bounded immutable source fragments. All six categories are required: flight, missions, assignments, telemetry, commands, findings. Empty categories are explicit JSON arrays. Each fragment specifies its ID, category, exact byte count (maximum 8 MiB), and SHA-256. There may be at most 4096 fragments per job. The snapshot producer must split large categories and must not silently cap records.

The worker reads each fragment from the configured API origin at `/internal/v1/archive-snapshots/{snapshot_id}/fragments/{fragment_id}`. URLs never come from the request. The API must freeze these bytes, enforce service authorization, and retain them until archive completion. This source contract is deliberately distinct from the existing bounded live replay endpoint: that endpoint cannot prove complete telemetry or immutable pagination.

Record payloads preserve original event and receipt timestamps, Agent/WAL/frame identity, exact mission and intent versions, assignment authority intervals, command evidence, and recorded incident transitions. The worker does not synthesize synchronized telemetry or rerun conformance. `complete` is the source's explicit coverage assertion; the worker verifies fragment integrity but cannot independently prove that an upstream producer captured every physical observation.

Chunks are gzip-compressed JSON arrays, named by compressed SHA-256 beneath an immutable operator/flight/revision/request-fingerprint prefix. The manifest carries schema version 1, full source identity/coverage, object keys, encoding, lengths, and compressed/uncompressed hashes. Every uploaded object is read back and hashed. The manifest is uploaded last, then the current unexpired job lease commits `ready` with its key and hash.

A replacement worker receives a higher database lease generation. A stale worker cannot publish a ready database result. Concurrent content-addressed writes are harmless because identical jobs generate identical bytes; pre-existing different bytes fail verification. Attempts renew every 15 seconds, expire after 45 seconds without renewal, and have a 30-minute budget. Failures retry with capped exponential backoff. Late evidence needs a new revision and snapshot; the old manifest is never overwritten.

## Integration status

This repository implements worker admission, durable jobs, snapshot consumption, object storage, and bundle publication. API flight-finalization event production, immutable source snapshot endpoints, API archive discovery, and Ops bundle playback must be connected by the coordinated lifecycle work. A standalone worker does not itself complete a flight or make the existing replay UI archive-backed.

## Deployment limits

Expose internal routes only on trusted service networking behind TLS. Credentials are environment-backed; AWS and GCS use workload/default credentials. PostgreSQL is worker-owned. The initial schema is additive and idempotent; published schema changes require numbered migrations. Object lifecycle policy must not delete referenced chunks while a manifest is retained. Source staging retention and archive job retention are not automated yet.

Gzip JSON prioritizes a portable first contract over analytical columnar efficiency. A future schema can add Parquet or other codecs without reinterpreting existing manifests. ZIP download packaging is separate from storage layout.
