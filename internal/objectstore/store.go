// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"errors"
	"github.com/aero-arc/aero-arc-archive-worker/internal/archive"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"google.golang.org/api/googleapi"
)

// Store uses immutable cloud writes and read-after-write hash verification.
// Local mode exists only for development and tests.
type Store struct {
	local  string
	bucket string
	s3     *s3.Client
	gcs    *storage.Client
}

// Open constructs a backend using the provider's default credential chain.
// S3 endpoint overrides are allowed for explicitly configured compatible stores.
func Open(ctx context.Context, backend, bucket, region, endpoint string) (*Store, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket or local directory required")
	}
	s := &Store{bucket: bucket}
	switch backend {
	case "local":
		var err error
		s.local, err = filepath.Abs(bucket)
		if err != nil {
			return nil, err
		}
		if err = os.MkdirAll(s.local, 0700); err != nil {
			return nil, err
		}
	case "s3":
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, err
		}
		s.s3 = s3.NewFromConfig(cfg, func(o *s3.Options) {
			if endpoint != "" {
				o.BaseEndpoint = aws.String(endpoint)
				o.UsePathStyle = true
			}
		})
	case "gcs":
		var err error
		s.gcs, err = storage.NewClient(ctx)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("backend must be s3, gcs, or local")
	}
	return s, nil
}

// Close releases cloud-client resources.
func (s *Store) Close() error {
	if s.gcs != nil {
		return s.gcs.Close()
	}
	return nil
}

// PutVerified conditionally creates an object and verifies its exact bytes.
// Existing mismatched bytes are an error; they are never overwritten.
func (s *Store) PutVerified(ctx context.Context, key string, b []byte, contentType string) error {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "..") || strings.Contains(key, "\\") {
		return fmt.Errorf("invalid object key")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var reader io.ReadCloser
	if s.s3 != nil {
		_, err := s.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: bytes.NewReader(b), ContentType: aws.String(contentType), IfNoneMatch: aws.String("*")})
		if err != nil {
			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "PreconditionFailed" {
				return err
			}
		}
		result, err := s.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
		if err != nil {
			return err
		}
		reader = result.Body
	} else if s.gcs != nil {
		object := s.gcs.Bucket(s.bucket).Object(key)
		writer := object.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
		writer.ContentType = contentType
		_, writeErr := writer.Write(b)
		closeErr := writer.Close()
		err := writeErr
		if err == nil {
			err = closeErr
		}
		if err != nil {
			failure, ok := err.(*googleapi.Error)
			if !ok || failure.Code != http.StatusPreconditionFailed {
				return err
			}
		}
		r, err := object.NewReader(ctx)
		if err != nil {
			return err
		}
		reader = r
	} else {
		path := filepath.Join(s.local, filepath.FromSlash(key))
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		f, err := os.CreateTemp(dir, ".upload-")
		if err != nil {
			return err
		}
		name := f.Name()
		defer func() { _ = os.Remove(name) }()
		if _, err = f.Write(b); err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if err = os.Link(name, path); err != nil && !os.IsExist(err) {
			return err
		}
		directory, err := os.Open(dir)
		if err != nil {
			return err
		}
		syncErr := directory.Sync()
		_ = directory.Close()
		if syncErr != nil {
			return syncErr
		}
		r, err := os.Open(path)
		if err != nil {
			return err
		}
		reader = r
	}
	defer func() { _ = reader.Close() }()
	stored, err := io.ReadAll(io.LimitReader(reader, int64(len(b))+1))
	if err != nil {
		return err
	}
	if len(stored) != len(b) || archive.Hash(stored) != archive.Hash(b) {
		return fmt.Errorf("stored object integrity mismatch: %s", key)
	}
	return nil
}
