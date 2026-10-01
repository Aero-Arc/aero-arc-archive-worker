// This Source Code Form is subject to the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aero-arc/aero-arc-archive-worker/internal/archive"
	"github.com/aero-arc/aero-arc-archive-worker/internal/jobs"
	"github.com/aero-arc/aero-arc-archive-worker/internal/objectstore"
	"github.com/aero-arc/aero-arc-archive-worker/internal/service"
)

func main() {
	if err := run(); err != nil {
		slog.Error("archive worker failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	token := os.Getenv("AERO_ARCHIVE_SERVICE_TOKEN")
	source, err := archive.NewHTTPSource(os.Getenv("AERO_ARCHIVE_API_URL"), token, os.Getenv("AERO_ARCHIVE_ALLOW_HTTP") == "true")
	if err != nil {
		return err
	}
	store, err := jobs.Open(ctx, os.Getenv("AERO_ARCHIVE_DATABASE_URL"))
	if err != nil {
		return err
	}
	defer store.DB.Close()
	objects, err := objectstore.Open(ctx, os.Getenv("AERO_ARCHIVE_BACKEND"), os.Getenv("AERO_ARCHIVE_BUCKET"), os.Getenv("AWS_REGION"), os.Getenv("AERO_ARCHIVE_S3_ENDPOINT"))
	if err != nil {
		return err
	}
	defer func() { _ = objects.Close() }()
	address := os.Getenv("AERO_ARCHIVE_ADDRESS")
	if address == "" {
		address = "127.0.0.1:8092"
	}
	return (&service.Service{Jobs: store, Source: source, Objects: objects, Token: token, Log: log}).Serve(ctx, address)
}
