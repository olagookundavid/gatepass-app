package main

import (
	"strings"
	"testing"

	"github.com/olagookundavid/gatepass-app/internal/platform/config"
)

func TestConfigListsEveryMissingVariable(t *testing.T) {
	l := config.FromMap(map[string]string{})
	loadServe(l)
	err := l.Err()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, v := range []string{"AWS_REGION", "DB_HOST", "DB_NAME", "DB_SECRET_ID", "RESERVATIONS_INTERNAL_URL", "PAYMENTS_QUEUE_URL", "COGNITO_ISSUER", "COGNITO_CLIENT_ID"} {
		if !strings.Contains(err.Error(), v) {
			t.Errorf("error does not mention %s:\n%v", v, err)
		}
	}
}
