package redisx

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestBuildIAMTokenShape(t *testing.T) {
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "secretexample"}, nil
	})
	tok, err := BuildIAMToken(context.Background(), creds, "us-east-1", "my-cache", "cache_user", false, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"my-cache/?", "Action=connect", "User=cache_user", "X-Amz-Expires=900", "X-Amz-Signature=", "elasticache%2Faws4_request"} {
		if !strings.Contains(tok, want) {
			t.Errorf("token lacks %q: %s", want, tok)
		}
	}
	if strings.HasPrefix(tok, "http") {
		t.Errorf("scheme must be stripped: %s", tok)
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{Addr: "no-port"}); err == nil {
		t.Error("addr without port accepted")
	}
	if _, err := New(Config{Addr: "h:6379", IAMAuth: true}); err == nil {
		t.Error("IAM auth without user/cache accepted")
	}
	if _, err := New(Config{Addr: "h:6379", AuthFile: "/x"}); err != nil {
		t.Error(err)
	}
}
