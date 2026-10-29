// Package s3 (reports adapter) implements ports.ReportStore: CSV objects in
// REPORTS_BUCKET.
//
// HEXAGONAL BOUNDARY: adapter. PutObject on an existing key REPLACES it, which is
// the idempotency we want. (With bucket versioning enabled, older versions are
// kept as history.)
//
// IAM: s3:PutObject on arn:aws:s3:::<REPORTS_BUCKET>/reports/*.
// Lambda in a VPC needs a route to S3: a gateway VPC endpoint for S3 (free) or a NAT.
// Failure looks like a connect timeout when that route is missing.
package s3

import (
	"bytes"
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/olagookundavid/gatepass-app/internal/reports/ports"
)

// Store implements ports.ReportStore.
type Store struct {
	Client *awss3.Client
	Bucket string
}

var _ ports.ReportStore = (*Store)(nil)

// Put uploads the CSV.
func (s *Store) Put(ctx context.Context, key string, csv []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := s.Client.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key),
		Body: bytes.NewReader(csv), ContentType: aws.String("text/csv; charset=utf-8")})
	return err
}
