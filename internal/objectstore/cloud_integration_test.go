//go:build integration

package objectstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestS3ImmutableUploadAndReadback(t *testing.T) {
	endpoint := os.Getenv("AERO_ARCHIVE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("AERO_ARCHIVE_TEST_S3_ENDPOINT required")
	}
	s, err := Open(context.Background(), "s3", os.Getenv("AERO_ARCHIVE_TEST_S3_BUCKET"), "us-east-1", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("tests/%d", time.Now().UnixNano())
	for i := 0; i < 2; i++ {
		if err = s.PutVerified(context.Background(), key, []byte("evidence"), "application/json"); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.PutVerified(context.Background(), key, []byte("different"), "application/json"); err == nil {
		t.Fatal("immutable overwrite accepted")
	}
}
