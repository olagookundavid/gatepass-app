package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoaderReportsEveryProblemAtOnce(t *testing.T) {
	l := FromMap(map[string]string{"N": "abc", "B": "maybe", "D": "soon"})
	l.Required("MISSING_ONE")
	l.Required("MISSING_TWO")
	l.Int("N", 1)
	l.Bool("B", false)
	l.Duration("D", time.Second)
	err := l.Err()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"MISSING_ONE", "MISSING_TWO", "N must be an integer", "B must be true or false", "D must be a duration"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q:\n%v", want, err)
		}
	}
}

func TestDefaultsAndTrimming(t *testing.T) {
	l := FromMap(map[string]string{"X": "  7 ", "BASE_PATH": "/catalog/", "AWS_REGION": "us-east-1"})
	if got := l.Int("X", 1); got != 7 {
		t.Fatalf("Int = %d", got)
	}
	if got := l.String("UNSET", "dflt"); got != "dflt" {
		t.Fatalf("String = %q", got)
	}
	c := LoadCommon(l, "/other")
	if c.BasePath != "/catalog" || c.HTTPAddr != ":8080" || c.ShutdownDelay != 5*time.Second {
		t.Fatalf("common = %+v", c)
	}
	if err := l.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestAWSMaxAttemptsValidated(t *testing.T) {
	l := FromMap(map[string]string{"AWS_REGION": "us-east-1", "AWS_MAX_ATTEMPTS": "99"})
	LoadCommon(l, "")
	if l.Err() == nil {
		t.Fatal("expected AWS_MAX_ATTEMPTS range error")
	}
}
