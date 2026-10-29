package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestCorrelationFieldsAndMasking(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "svc", "v1", "info")
	ctx, _ := WithReqInfo(context.Background(), "req-1")
	SetUserID(ctx, "user-9")
	log.InfoContext(ctx, "hello")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]string{"service": "svc", "version": "v1", "request_id": "req-1", "user_id": "user-9", "msg": "hello"} {
		if m[k] != want {
			t.Errorf("%s = %v, want %s", k, m[k], want)
		}
	}
	if got := MaskEmail("jane@example.com"); got != "j***@example.com" {
		t.Errorf("MaskEmail = %s", got)
	}
}
