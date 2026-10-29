// Package s3 (notifications adapter) implements ports.Tickets: ticket PNGs in an
// Amazon S3 bucket and presigned download links.
//
// HEXAGONAL BOUNDARY: adapter.
//
// Layout: tickets/<userId>/<orderId>.png in TICKETS_BUCKET (private bucket; the
// objects are never public, access is only through presigned URLs created by this
// service after an ownership check).
//
// IAM: s3:PutObject and s3:GetObject on arn:aws:s3:::<TICKETS_BUCKET>/tickets/*
// (GetObject is needed because a presigned GET URL has the permissions of the
// signer: a role without s3:GetObject produces URLs that return 403).
// Failure shapes: AccessDenied, NoSuchBucket, SlowDown (throttling, retried by the
// SDK), RequestTimeout.
//
// Presigned URL lifetime: min(requested TTL, remaining lifetime of the signing
// credentials). See the comment in app.Worker.process.
package s3

import (
	"bytes"
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/olagookundavid/gatepass-app/internal/notifications/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
)

// Store implements ports.Tickets.
type Store struct {
	Client  *awss3.Client
	Presign *awss3.PresignClient
	Bucket  string
}

// New builds a Store from a regular client.
func New(c *awss3.Client, bucket string) *Store {
	return &Store{Client: c, Presign: awss3.NewPresignClient(c), Bucket: bucket}
}

var _ ports.Tickets = (*Store)(nil)

// Put uploads the PNG (overwrites: re-processing an event is harmless).
func (s *Store) Put(ctx context.Context, key string, png []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := s.Client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.Bucket), Key: aws.String(key), Body: bytes.NewReader(png),
		ContentType: aws.String("image/png"), CacheControl: aws.String("private, max-age=0"),
	})
	awscfg.LogThrottle(ctx, "s3.PutObject", err)
	return err
}

// PresignGet returns a time-limited download URL (signed locally, no network call).
func (s *Store) PresignGet(ctx context.Context, key, downloadName string, ttl time.Duration) (string, error) {
	req, err := s.Presign.PresignGetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.Bucket), Key: aws.String(key),
		ResponseContentDisposition: aws.String(`attachment; filename="` + downloadName + `"`),
		ResponseContentType:        aws.String("image/png"),
	}, awss3.WithPresignExpires(ttl))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}
