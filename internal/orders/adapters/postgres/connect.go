package postgres

// This file: building the orders database pool from environment configuration.
// Shared by orders-api, orders-worker and orders-outbox-relay.
//
// Credentials come from AWS Secrets Manager (secret JSON {"username","password"},
// env DB_SECRET_ID), cached by platform/pgdb.SecretCredentials. If the secret is
// rotated while the service runs, the next new connection fails authentication
// (SQLSTATE 28P01); pgdb.Pool then drops the cache, re-reads the secret and
// retries ONCE. IAM: secretsmanager:GetSecretValue on the secret (+ kms:Decrypt
// if it uses a customer-managed key).

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
)

// DBConfig is the database part of the configuration.
type DBConfig struct {
	Host, Name, SecretID, RootCert string
	Port, MaxConns                 int
	StatementTimeout               time.Duration
}

// LoadDB reads DB_HOST, DB_PORT, DB_NAME, DB_SECRET_ID (+ optional tuning).
func LoadDB(l *config.Loader) DBConfig {
	return DBConfig{
		Host: l.Required("DB_HOST"), Port: l.Int("DB_PORT", 5432), Name: l.Required("DB_NAME"), SecretID: l.Required("DB_SECRET_ID"),
		MaxConns:         l.Int("DB_MAX_CONNS", 10),
		StatementTimeout: l.Duration("DB_STATEMENT_TIMEOUT", 5*time.Second),
		RootCert:         l.String("DB_SSL_ROOT_CERT", ""),
	}
}

// Connect creates the pool. app names the binary in pg_stat_activity.
func Connect(ctx context.Context, c DBConfig, awsCfg aws.Config, app string) (*pgdb.Pool, error) {
	creds := &pgdb.SecretCredentials{Client: secretsmanager.NewFromConfig(awsCfg), SecretID: c.SecretID}
	return pgdb.Connect(ctx, pgdb.Config{Host: c.Host, Port: c.Port, Name: c.Name, MaxConns: int32(c.MaxConns),
		StatementTimeout: c.StatementTimeout, RootCertPath: c.RootCert, AppName: app}, creds)
}
