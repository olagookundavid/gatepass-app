package main

import (
	"strings"
	"testing"

	"github.com/olagookundavid/gatepass-app/internal/platform/config"
)

func TestWorkerConfigValidation(t *testing.T) {
	base := map[string]string{"AWS_REGION": "us-east-1", "DB_HOST": "h", "DB_NAME": "orders", "DB_SECRET_ID": "s", "PAYMENTS_QUEUE_URL": "https://sqs.example.com/q"}
	l := config.FromMap(base)
	if c := loadConfig(l); l.Err() != nil || c.SuccessRate != 0.8 || c.Concurrency != 4 {
		t.Fatalf("defaults: %+v %v", c, l.Err())
	}
	base["PAYMENT_SUCCESS_RATE"] = "1.5"
	l = config.FromMap(base)
	loadConfig(l)
	if err := l.Err(); err == nil || !strings.Contains(err.Error(), "PAYMENT_SUCCESS_RATE") {
		t.Fatalf("rate out of range accepted: %v", err)
	}
}
