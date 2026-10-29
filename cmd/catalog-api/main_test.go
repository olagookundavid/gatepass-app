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
	for _, v := range []string{"AWS_REGION", "DB_HOST", "DB_NAME", "DB_USER", "REDIS_ADDR", "IMAGES_BUCKET", "CDN_BASE_URL", "EVENT_BUS_NAME", "COGNITO_ISSUER", "COGNITO_CLIENT_ID", "DB_IAM_AUTH"} {
		if !strings.Contains(err.Error(), v) {
			t.Errorf("error does not mention %s:\n%v", v, err)
		}
	}
}

func TestValidConfig(t *testing.T) {
	l := config.FromMap(map[string]string{
		"AWS_REGION": "us-east-1", "DB_HOST": "db.example.com", "DB_NAME": "catalog", "DB_USER": "catalog_app", "DB_IAM_AUTH": "true",
		"REDIS_ADDR": "cache.example.com:6379", "REDIS_IAM_AUTH": "true", "REDIS_USER": "catalog", "CACHE_NAME": "gatepass",
		"IMAGES_BUCKET": "b", "CDN_BASE_URL": "https://cdn.example.com", "EVENT_BUS_NAME": "gatepass-dev",
		"COGNITO_ISSUER": "https://cognito-idp.us-east-1.amazonaws.com/pool", "COGNITO_CLIENT_ID": "c",
	})
	c := loadServe(l)
	if err := l.Err(); err != nil {
		t.Fatal(err)
	}
	if c.Common.BasePath != "/catalog" || c.DB.Port != 5432 {
		t.Fatalf("%+v", c)
	}
}
