# Archive Worker contributor context

- API owns flight finalization, authorization, and source snapshot identity. The worker must never infer completion from a missing aircraft or stale telemetry.
- Durable jobs are idempotent by event identity and immutable source revision. Conflicting duplicates fail closed.
- Job claims and publication are fenced by a database lease generation; stale workers cannot publish a ready result.
- Input pages are bounded. A truncated or unavailable source is never a complete archive.
- Preserve event and receipt times, mission/intent versions, conformance authority boundaries, stable identities, and explicit coverage gaps.
- Upload immutable compressed chunks first, verify their stored SHA-256 values, then publish the manifest last. Mark a job ready only after manifest verification.
- Late data creates another revision. Never overwrite published evidence or silently recompute recorded findings.
- Use the Aero Arc Go service layout, structured slog logging, health/readiness endpoints, environment-backed secrets, and MPL 2.0.
- Add identifier-led documentation to exported Go APIs. Commit with DCO sign-off.
- Validate with gofmt, go test ./..., go test -race ./..., go vet ./..., and real database/object-store integration tests for persistence changes.
