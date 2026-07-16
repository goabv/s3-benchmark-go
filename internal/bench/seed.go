package bench

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/goabv/s3-benchmark-go/internal/config"
	"github.com/goabv/s3-benchmark-go/internal/metrics"
)

// RunSeed uploads the configured object sizes to the download data prefix
// (cfg.KeysFor naming), skipping any objects that already exist. It mirrors the
// JS runner's upload-test-data.js seed step so the download benchmark has data to
// read.
func RunSeed(ctx context.Context, client *s3.Client, cfg *config.Config, prog *metrics.Progress) error {
	if prog == nil {
		prog = &metrics.Progress{}
	}
	partSize, err := config.ParseSize(cfg.PartSize)
	if err != nil {
		return fmt.Errorf("parse partSize %q: %w", cfg.PartSize, err)
	}
	if partSize < 5<<20 {
		return fmt.Errorf("partSize %s is below the S3 multipart minimum of 5MiB", cfg.PartSize)
	}

	data := make([]byte, partSize)
	for i := range data {
		data[i] = byte(i * 31)
	}
	seeder := &uploadRun{
		client:   client,
		cfg:      cfg,
		prog:     prog,
		partSize: partSize,
		data:     data,
		ips:      newIPStats(),
		timing:   &metrics.Recorder{},
		parts:    &partRecorder{},
		iter:     -1, // never record per-part CSV rows while seeding
	}
	parallelism := cfg.Upload.Parallelism()

	fmt.Printf("=== Seeding download data to s3://%s/%s ===\n", cfg.Bucket, cfg.DataPrefix)
	for _, spec := range cfg.Sizes {
		sizeBytes, err := config.ParseSize(spec.Size)
		if err != nil {
			return fmt.Errorf("parse size %q: %w", spec.Size, err)
		}
		for _, key := range cfg.KeysFor(spec) {
			exists, err := objectExists(ctx, client, cfg.Bucket, key)
			if err != nil {
				return fmt.Errorf("head %q: %w", key, err)
			}
			if exists {
				fmt.Printf("  skip (exists): %s\n", key)
				continue
			}
			fmt.Printf("  seeding: %s (%s)\n", key, spec.Size)
			if _, err := seeder.uploadObject(ctx, key, sizeBytes, parallelism); err != nil {
				return fmt.Errorf("seed %q: %w", key, err)
			}
		}
	}
	fmt.Printf("=== Seeding complete ===\n")
	return nil
}

// objectExists reports whether an object is present via HeadObject.
func objectExists(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return false, nil
		}
	}
	return false, err
}
