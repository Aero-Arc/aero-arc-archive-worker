//go:build integration

package objectstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	ctx := context.Background()
	bucket := fmt.Sprintf("archive-review-%d", time.Now().UnixNano())
	s.bucket = bucket
	if _, err = s.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = s.s3.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}) }()
	key := fmt.Sprintf("tests/%d", time.Now().UnixNano())
	defer func() {
		_, _ = s.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	}()
	for i := 0; i < 2; i++ {
		if err = s.PutVerified(context.Background(), key, []byte("evidence"), "application/json"); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.PutVerified(context.Background(), key, []byte("different"), "application/json"); err == nil {
		t.Fatal("immutable overwrite accepted")
	}
}

func TestGCSImmutableUploadAndReadback(t *testing.T) {
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Skip("STORAGE_EMULATOR_HOST required")
	}
	ctx := context.Background()
	bucket := fmt.Sprintf("archive-review-%d", time.Now().UnixNano())
	s, err := Open(ctx, "gcs", bucket, "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err = s.gcs.Bucket(bucket).Create(ctx, "archive-review", nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.gcs.Bucket(bucket).Delete(ctx) }()
	key := "tests/evidence"
	defer func() { _ = s.gcs.Bucket(bucket).Object(key).Delete(ctx) }()
	for range 2 {
		if err = s.PutVerified(ctx, key, []byte("evidence"), "application/json"); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.PutVerified(ctx, key, []byte("different"), "application/json"); err == nil {
		t.Fatal("immutable overwrite accepted")
	}
}
