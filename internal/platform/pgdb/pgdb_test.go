package pgdb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestEmbeddedBundleParsesAndTLSIsVerifyFull(t *testing.T) {
	cfg, err := tlsConfig("db.example.com", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "db.example.com" || cfg.InsecureSkipVerify || cfg.RootCAs == nil {
		t.Fatalf("tls config is not verify-full: %+v", cfg)
	}
	if _, err := tlsConfig("h", "/nonexistent/file.pem"); err == nil {
		t.Fatal("missing override file accepted")
	}
}

func TestIsAuthErr(t *testing.T) {
	if !isAuthErr(&pgconn.PgError{Code: "28P01"}) || isAuthErr(&pgconn.PgError{Code: "23505"}) || isAuthErr(errors.New("x")) {
		t.Fatal("isAuthErr misclassifies")
	}
}

type fakeSM struct {
	calls  atomic.Int32
	secret atomic.Value
}

func (f *fakeSM) GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	f.calls.Add(1)
	return &secretsmanager.GetSecretValueOutput{SecretString: aws.String(f.secret.Load().(string))}, nil
}

func TestSecretCredentialsCacheAndInvalidate(t *testing.T) {
	f := &fakeSM{}
	f.secret.Store(`{"username":"orders_app","password":"old"}`)
	s := &SecretCredentials{Client: f, SecretID: "id"}
	ctx := context.Background()

	_, p, _ := s.Credentials(ctx)
	_, _, _ = s.Credentials(ctx)
	if p != "old" || f.calls.Load() != 1 {
		t.Fatalf("not cached: pw=%s calls=%d", p, f.calls.Load())
	}
	f.secret.Store(`{"username":"orders_app","password":"new"}`) // rotation
	_, p, _ = s.Credentials(ctx)
	if p != "old" {
		t.Fatal("cache should still serve the old value until invalidated")
	}
	s.Invalidate()
	_, p, _ = s.Credentials(ctx)
	if p != "new" || f.calls.Load() != 2 {
		t.Fatalf("after invalidate: pw=%s calls=%d", p, f.calls.Load())
	}
}

func TestSecretCredentialsRejectsBadSecretWithoutLeakingIt(t *testing.T) {
	f := &fakeSM{}
	f.secret.Store(`{"username":"x","password":""}`)
	_, _, err := (&SecretCredentials{Client: f, SecretID: "id"}).Credentials(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

type fakeCreds struct{ invalidated atomic.Int32 }

func (f *fakeCreds) Credentials(context.Context) (string, string, error) { return "u", "p", nil }
func (f *fakeCreds) Invalidate()                                         { f.invalidated.Add(1) }

func TestRetryAuthRetriesExactlyOnce(t *testing.T) {
	fc := &fakeCreds{}
	p := &Pool{creds: fc}
	calls := 0
	_, err := retryAuth(p, func() (int, error) { calls++; return 0, &pgconn.PgError{Code: "28P01"} })
	if err == nil || calls != 2 || fc.invalidated.Load() != 1 {
		t.Fatalf("calls=%d invalidated=%d err=%v", calls, fc.invalidated.Load(), err)
	}
	calls = 0
	_, err = retryAuth(p, func() (int, error) {
		calls++
		if calls == 1 {
			return 0, &pgconn.PgError{Code: "28P01"}
		}
		return 7, nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("recovery failed: calls=%d err=%v", calls, err)
	}
	// Non-auth errors are never retried (a retried INSERT could double-write).
	calls = 0
	_, _ = retryAuth(p, func() (int, error) { calls++; return 0, errors.New("other") })
	if calls != 1 {
		t.Fatalf("non-auth error retried: %d", calls)
	}
}
