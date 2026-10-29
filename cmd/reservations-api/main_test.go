package main

import (
	"strings"
	"testing"

	"github.com/olagookundavid/gatepass-app/internal/platform/config"
)

func TestConfigRequiresOnlyWhatTheEnabledModesNeed(t *testing.T) {
	// consumer-only: no Cognito, Redis or Scheduler variables needed
	l := config.FromMap(map[string]string{"AWS_REGION": "us-east-1", "TABLE_NAME": "t", "API_ENABLED": "false", "CONSUMER_ENABLED": "true", "INVENTORY_QUEUE_URL": "https://sqs.example.com/q"})
	loadConfig(l)
	if err := l.Err(); err != nil {
		t.Fatalf("consumer-only config rejected: %v", err)
	}

	// everything off is a mistake
	l = config.FromMap(map[string]string{"AWS_REGION": "us-east-1", "TABLE_NAME": "t", "API_ENABLED": "false"})
	loadConfig(l)
	if l.Err() == nil {
		t.Fatal("API and consumer both disabled accepted")
	}

	// API mode lists everything that is missing at once
	l = config.FromMap(map[string]string{})
	loadConfig(l)
	err := l.Err()
	for _, v := range []string{"TABLE_NAME", "COGNITO_ISSUER", "SCHEDULER_GROUP_NAME", "SCHEDULER_ROLE_ARN", "EXPIRER_LAMBDA_ARN", "REDIS_ADDR"} {
		if err == nil || !strings.Contains(err.Error(), v) {
			t.Errorf("missing %s not reported: %v", v, err)
		}
	}
}
