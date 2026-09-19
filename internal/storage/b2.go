// Package storage wraps an S3-compatible object store (Backblaze B2) used to
// hold P2P payment-proof files, replacing the old Postgres BYTEA column.
package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type B2Store struct {
	client *s3.Client
	bucket string
}

// NewB2Store builds an S3 client pointed at a Backblaze B2 bucket. endpoint is
// the bucket's S3-compatible endpoint (e.g. https://s3.us-east-005.backblazeb2.com).
func NewB2Store(ctx context.Context, endpoint, region, bucket, keyID, appKey string) (*B2Store, error) {
	if endpoint == "" || bucket == "" || keyID == "" || appKey == "" {
		return nil, fmt.Errorf("storage: missing B2 configuration")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(keyID, appKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("storage: load config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return &B2Store{client: client, bucket: bucket}, nil
}

func (b *B2Store) Put(ctx context.Context, key, mimeType string, data []byte) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(mimeType),
	})
	if err != nil {
		return fmt.Errorf("storage: put object: %w", err)
	}
	return nil
}

func (b *B2Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("storage: get object: %w", err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("storage: read object body: %w", err)
	}
	return data, nil
}

func (b *B2Store) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("storage: delete object: %w", err)
	}
	return nil
}
