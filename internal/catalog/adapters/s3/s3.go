// Package s3 (catalog adapter) implements ports.ImageSigner: presigned PUT URLs
// so browsers upload event images directly to Amazon S3.
//
// HEXAGONAL BOUNDARY: adapter.
//
// How presigning works: the SDK signs (SigV4, locally, no network call) a PUT
// request with the pod's own credentials. Whoever holds the URL can perform
// exactly that request until it expires, with the signed headers. Because we sign
// Content-Type and Content-Length, S3 rejects an upload whose headers differ
// (SignatureDoesNotMatch / 403). See the AWS docs "Uploading objects with presigned URLs".
//
// IAM: s3:PutObject on arn:aws:s3:::<IMAGES_BUCKET>/images/events/* for the role
// that signs. (The signer needs the permission itself: a URL signed by a role
// without it returns 403 when used.)
// Failure looks like: 403 AccessDenied at upload time (not at signing time!),
// ExpiredToken if the signing role's session ended before the URL expired.
package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/olagookundavid/gatepass-app/internal/catalog/ports"
)

// Signer implements ports.ImageSigner.
type Signer struct {
	Presign *awss3.PresignClient
	Bucket  string
}

// New builds a Signer from a regular S3 client.
func New(c *awss3.Client, bucket string) *Signer {
	return &Signer{Presign: awss3.NewPresignClient(c), Bucket: bucket}
}

var _ ports.ImageSigner = (*Signer)(nil)

// PresignPut returns a URL valid for ttl.
func (s *Signer) PresignPut(ctx context.Context, objectKey, contentType string, contentLength int64, ttl time.Duration) (string, error) {
	req, err := s.Presign.PresignPutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.Bucket),
		Key:           aws.String(objectKey),
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(contentLength),
	}, awss3.WithPresignExpires(ttl))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}
