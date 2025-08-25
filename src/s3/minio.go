package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
)

// MinIOOperator implements S3 operations for MinIO
type MinIOOperator struct {
	client   *s3.Client
	bucket   string
	endpoint string
}

// NewMinIOOperator creates a new MinIO operator
func NewMinIOOperator(ctx context.Context, bucket, endpoint, accessKey, secretKey, region string) (*MinIOOperator, error) {
	logrus.Infof("Creating MinIO operator for bucket: %s at endpoint: %s", bucket, endpoint)

	// Create custom endpoint resolver for MinIO
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               endpoint,
			SigningRegion:     region,
			HostnameImmutable: true, // This prevents the SDK from prepending the bucket name to the hostname
		}, nil
	})

	// Load configuration
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	client := s3.NewFromConfig(cfg)

	// Test connection by listing buckets
	_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		logrus.Warnf("Failed to list buckets (this is normal for MinIO): %v", err)
	}

	return &MinIOOperator{
		client:   client,
		bucket:   bucket,
		endpoint: endpoint,
	}, nil
}

// Delete removes an object from MinIO
func (m *MinIOOperator) Delete(ctx context.Context, path string) error {
	logrus.Infof("MinIO Delete: %s/%s", m.bucket, path)

	_, err := m.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object %s: %w", path, err)
	}

	return nil
}

// Reader returns a reader for an object in MinIO
func (m *MinIOOperator) Reader(ctx context.Context, path string) (io.ReadCloser, error) {
	logrus.Infof("MinIO Reader: %s/%s", m.bucket, path)

	result, err := m.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", path, err)
	}

	return result.Body, nil
}

// Writer returns a writer for an object in MinIO
func (m *MinIOOperator) Writer(ctx context.Context, path string) (io.WriteCloser, error) {
	logrus.Infof("MinIO Writer: %s/%s", m.bucket, path)

	return &minioWriter{
		client: m.client,
		bucket: m.bucket,
		key:    path,
		ctx:    ctx,
	}, nil
}

// List returns a list of objects in MinIO with the given prefix
func (m *MinIOOperator) List(ctx context.Context, path string) ([]string, error) {
	logrus.Infof("MinIO List: %s/%s", m.bucket, path)

	result, err := m.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(m.bucket),
		Prefix: aws.String(path),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects with prefix %s: %w", path, err)
	}

	var objects []string
	for _, obj := range result.Contents {
		objects = append(objects, *obj.Key)
	}

	return objects, nil
}

// minioWriter implements io.WriteCloser for MinIO uploads
type minioWriter struct {
	client *s3.Client
	bucket string
	key    string
	ctx    context.Context
	buffer []byte
}

// Write implements io.Writer
func (mw *minioWriter) Write(p []byte) (n int, err error) {
	mw.buffer = append(mw.buffer, p...)
	return len(p), nil
}

// Close implements io.Closer and uploads the data to MinIO
func (mw *minioWriter) Close() error {
	logrus.Infof("MinIO Writer Close: uploading %s/%s with %d bytes", mw.bucket, mw.key, len(mw.buffer))

	_, err := mw.client.PutObject(mw.ctx, &s3.PutObjectInput{
		Bucket: aws.String(mw.bucket),
		Key:    aws.String(mw.key),
		Body:   bytes.NewReader(mw.buffer),
	})
	if err != nil {
		logrus.Errorf("MinIO Writer Close: failed to upload object %s: %v", mw.key, err)
		return fmt.Errorf("failed to upload object %s: %w", mw.key, err)
	}

	logrus.Infof("MinIO Writer Close: successfully uploaded %s/%s", mw.bucket, mw.key)
	return nil
}
