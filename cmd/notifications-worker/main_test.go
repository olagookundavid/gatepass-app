package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/olagookundavid/gatepass-app/internal/platform/config"
)

func TestConfigListsEveryMissingVariable(t *testing.T) {
	l := config.FromMap(map[string]string{})
	loadConfig(l)
	err := l.Err()
	for _, v := range []string{"TABLE_NAME", "TICKETS_BUCKET", "NOTIFICATIONS_QUEUE_URL", "COGNITO_USER_POOL_ID", "SES_FROM_ADDRESS", "TICKET_SIGNING_KEY_FILE"} {
		if err == nil || !strings.Contains(err.Error(), v) {
			t.Errorf("%s not reported: %v", v, err)
		}
	}
}

func TestSigningKeyFile(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "k")
	_ = os.WriteFile(good, []byte("0123456789abcdef0123456789abcdef\n"), 0o600)
	if k, err := readSigningKey(good); err != nil || len(k) != 32 {
		t.Fatalf("%v %d", err, len(k))
	}
	short := filepath.Join(dir, "s")
	_ = os.WriteFile(short, []byte("tooshort"), 0o600)
	if _, err := readSigningKey(short); err == nil {
		t.Fatal("short key accepted")
	}
	if _, err := readSigningKey(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("missing file accepted")
	}
}
