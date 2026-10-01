# Contributing

Start feature branches from `main`. Use signed-off commits (`git commit -s`) for the Developer Certificate of Origin and preserve MPL 2.0 notices.

Run `make build test test-race vet integration` before review. Integration tests require an isolated PostgreSQL database through `AERO_ARCHIVE_TEST_DATABASE_URL`; they truncate only the worker's `archive_jobs` table. Never point tests at a deployment database.

Optional S3 integration uses `AERO_ARCHIVE_TEST_S3_ENDPOINT`, `AERO_ARCHIVE_TEST_S3_BUCKET`, and the standard AWS credential environment variables. Use an isolated local S3-compatible server. GCS production credentials must not be committed or used for automated tests.
