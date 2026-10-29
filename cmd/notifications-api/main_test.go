package main

import (
	"strings"
	"testing"

	"github.com/olagookundavid/gatepass-app/internal/platform/config"
)

func TestConfigListsEveryMissingVariable(t *testing.T) {
	l := config.FromMap(map[string]string{})
	loadConfig(l)
	err := l.Err()
	for _, v := range []string{"AWS_REGION", "TABLE_NAME", "TICKETS_BUCKET", "COGNITO_ISSUER", "COGNITO_CLIENT_ID"} {
		if err == nil || !strings.Contains(err.Error(), v) {
			t.Errorf("%s not reported: %v", v, err)
		}
	}
}
