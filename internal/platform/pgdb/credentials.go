package pgdb

// This file: the two Credentials implementations.
//
// AWS services touched: RDS (IAM database authentication token), Secrets Manager.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// IAMAuth generates an RDS IAM authentication token for every new connection.
//
// What it does: signs (SigV4, locally, no network call) a request for the
// "rds-db:connect" action; the resulting URL-like string is valid for 15
// minutes and is used as the PostgreSQL password. See the AWS docs "IAM
// database authentication for MariaDB, MySQL, and PostgreSQL".
//
// IAM action needed by the caller's role: rds-db:connect on
// arn:aws:rds-db:<region>:<account-id>:dbuser:<DbiResourceId>/<db user>.
// The DB user must exist with `GRANT rds_iam TO <user>`.
//
// Failure looks like: "PAM authentication failed for user ..." (user missing the
// rds_iam role, or role lacks rds-db:connect) or "no pg_hba.conf entry" (TLS off).
// Token expiry is not a problem: a token only has to be valid when the
// connection is OPENED.
type IAMAuth struct {
	Host     string
	Port     int
	User     string
	Region   string
	Provider aws.CredentialsProvider
}

// Credentials implements Credentials.
func (a IAMAuth) Credentials(ctx context.Context) (string, string, error) {
	endpoint := net.JoinHostPort(a.Host, strconv.Itoa(a.Port))
	tok, err := auth.BuildAuthToken(ctx, endpoint, a.Region, a.User, a.Provider)
	if err != nil {
		return "", "", fmt.Errorf("build rds auth token: %w", err)
	}
	return a.User, tok, nil
}

// SecretsAPI is the subset of *secretsmanager.Client used.
type SecretsAPI interface {
	GetSecretValue(ctx context.Context, in *secretsmanager.GetSecretValueInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

// SecretCredentials reads {"username","password"} JSON from Secrets Manager.
//
// Caching: calling GetSecretValue for every new connection would add latency and
// cost (and can be throttled). We cache for TTL (default 5 min) and also
// invalidate on authentication failure (see Pool.retryAuth), which is the case
// that matters: the secret was rotated and the old password no longer works.
type SecretCredentials struct {
	Client   SecretsAPI
	SecretID string
	TTL      time.Duration

	mu       sync.Mutex
	user     string
	password string
	fetched  time.Time
}

// Credentials implements Credentials.
func (s *SecretCredentials) Credentials(ctx context.Context) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ttl := s.TTL
	if ttl == 0 {
		ttl = 5 * time.Minute
	}
	if s.user != "" && time.Since(s.fetched) < ttl {
		return s.user, s.password, nil
	}
	// secretsmanager:GetSecretValue on the secret ARN (+ kms:Decrypt if the secret
	// uses a customer-managed key). Failure: AccessDenied (IAM), ResourceNotFound
	// (wrong DB_SECRET_ID), throttling (retried by the SDK).
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := s.Client.GetSecretValue(cctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(s.SecretID)})
	if err != nil {
		return "", "", fmt.Errorf("get secret: %w", err)
	}
	var v struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if out.SecretString == nil || json.Unmarshal([]byte(*out.SecretString), &v) != nil || v.Username == "" || v.Password == "" {
		// Do not include the value in the error: it is a secret.
		return "", "", errors.New("secret is not JSON with non-empty username and password")
	}
	s.user, s.password, s.fetched = v.Username, v.Password, time.Now()
	return s.user, s.password, nil
}

// Invalidate drops the cache so the next connection re-reads the secret.
func (s *SecretCredentials) Invalidate() {
	s.mu.Lock()
	s.user, s.password = "", ""
	s.mu.Unlock()
}
