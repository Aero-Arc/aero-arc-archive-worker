# Aero Arc Archive Worker

Aero Arc Archive Worker builds immutable flight evidence bundles from API-finalized flights and publishes verified objects to S3, GCS, or local development storage. It owns a PostgreSQL job inbox, fenced worker leases, retries, deterministic gzip chunks, and manifest-last publication.

Flight finalization remains API authority. The worker never decides that a flight ended from stale telemetry or a disconnected Agent. See [the archive contract](docs/archive-contract.md) for integration status and completeness semantics.

## Run

Use Go 1.24 or newer and PostgreSQL. Copy the values in [configs/environment.example](configs/environment.example) into your deployment environment, then:

```sh
make build
./bin/aero-arc-archive-worker
```

`/healthz` reports process health. `/readyz` checks PostgreSQL reachability; it does not certify source availability or cloud bucket permissions. Internal job admission/status routes require the service bearer credential. Bind locally or deploy behind a TLS ingress; do not expose an unencrypted service credential publicly.

## Validate

```sh
make test test-race vet
AERO_ARCHIVE_TEST_DATABASE_URL='postgres://postgres:archive-test@localhost:5432/archive?sslmode=disable' make integration
```

Integration tests require an isolated database. See [CONTRIBUTING.md](CONTRIBUTING.md) for optional S3-compatible integration configuration.

## License

Mozilla Public License 2.0. Contributions require a DCO sign-off.

Manifest publication checks the active PostgreSQL lease under a row lock before
upload and again before committing readiness. Manifest keys include the worker
lease generation. An upload that finishes after lease expiry can leave an orphan
candidate, but cannot become a ready result or overwrite another generation.
Consumers must obtain the manifest key from a job in `ready` state; bucket listing
is not archive discovery or proof of publication.
