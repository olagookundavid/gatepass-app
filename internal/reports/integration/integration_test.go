//go:build integration

// Integration tests for the nightly report adapters.
//
//	TEST_DATABASE_URL    PostgreSQL with password auth (the query and CSV are verified here)
//	TEST_REPORTS_BUCKET + AWS_REGION   S3 overwrite behaviour
//	REPORT_IT_RDS_HOST, REPORT_IT_RDS_PORT, REPORT_IT_RDS_DB, REPORT_IT_RDS_USER + AWS_REGION
//	                     a real RDS instance with IAM auth for the read-only user
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
	"github.com/olagookundavid/gatepass-app/internal/platform/testenv"
	rpg "github.com/olagookundavid/gatepass-app/internal/reports/adapters/postgres"
	rs3 "github.com/olagookundavid/gatepass-app/internal/reports/adapters/s3"
	"github.com/olagookundavid/gatepass-app/internal/reports/app"
	"github.com/olagookundavid/gatepass-app/internal/reports/domain"
	"github.com/olagookundavid/gatepass-app/migrations"
)

var ctx = context.Background()

type memStore struct{ puts map[string]string }

func (m *memStore) Put(_ context.Context, k string, b []byte) error {
	m.puts[k] = string(b)
	return nil
}

func seedPaid(t *testing.T, pool *pgdb.Pool, event, currency, status string, qty int, amount int64, paidAt time.Time) {
	t.Helper()
	id := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO orders (id,user_id,hold_id,event_id,ticket_type_id,quantity,amount_cents,currency,status,created_at,updated_at)
		VALUES ($1,'u',$2,$3,$4,$5,$6,$7,$8,$9,$9)`, id, uuid.NewString(), event, uuid.NewString(), qty, amount, currency, status, paidAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO order_status_history (order_id,status,at) VALUES ($1,'pending_payment',$2),($1,'paid',$2)`, id, paidAt); err != nil {
		t.Fatal(err)
	}
}

func TestReportQueryWindowAndCSV(t *testing.T) {
	env := testenv.Need(t, "TEST_DATABASE_URL")
	pool := testenv.FreshSchema(t, env["TEST_DATABASE_URL"], migrations.Orders())
	now := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	yStart := time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC)
	e1, e2 := "11111111-1111-7111-8111-111111111111", "22222222-2222-7222-8222-222222222222"

	seedPaid(t, pool, e1, "USD", "paid", 2, 2000, yStart)                                  // first instant of the day: included
	seedPaid(t, pool, e1, "USD", "cancelled", 1, 1000, yStart.Add(10*time.Hour))           // paid then cancelled: counted, not "still paid"
	seedPaid(t, pool, e2, "USD", "paid", 4, 8000, yStart.Add(23*time.Hour+59*time.Minute)) // included
	seedPaid(t, pool, e1, "USD", "paid", 8, 9999, yStart.Add(-time.Second))                // day before: excluded
	seedPaid(t, pool, e1, "USD", "paid", 8, 9999, yStart.Add(24*time.Hour))                // exactly midnight after: excluded (half-open)

	st := &memStore{puts: map[string]string{}}
	svc := &app.Service{Source: &rpg.Source{DB: pool}, Store: st, Now: func() time.Time { return now }}
	for i := 0; i < 2; i++ {
		out, err := svc.Run(ctx, json.RawMessage(`{}`))
		if err != nil || out.Key != "reports/2026/02/28.csv" || out.Rows != 2 {
			t.Fatalf("%+v %v", out, err)
		}
	}
	want := strings.Join([]string{
		"date,event_id,currency,orders_paid,tickets,gross_cents,orders_still_paid",
		"2026-02-28," + e1 + ",USD,2,3,3000,1",
		"2026-02-28," + e2 + ",USD,1,4,8000,1",
	}, "\n") + "\n"
	if st.puts["reports/2026/02/28.csv"] != want {
		t.Fatalf("csv:\n%s\nwant:\n%s", st.puts["reports/2026/02/28.csv"], want)
	}
}

func TestS3OverwritesTheSameKey(t *testing.T) {
	env := testenv.Need(t, "TEST_REPORTS_BUCKET", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	client := awss3.NewFromConfig(cfg)
	st := &rs3.Store{Client: client, Bucket: env["TEST_REPORTS_BUCKET"]}
	key := "reports/it-" + testenv.Prefix() + "/day.csv"
	t.Cleanup(func() {
		_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(env["TEST_REPORTS_BUCKET"]), Key: aws.String(key)})
	})
	for _, body := range []string{"first\n", "second\n"} {
		if err := st.Put(ctx, key, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(env["TEST_REPORTS_BUCKET"]), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	buf := make([]byte, 16)
	n, _ := out.Body.Read(buf)
	if string(buf[:n]) != "second\n" {
		t.Fatalf("object holds %q, want the second write", buf[:n])
	}
}

func TestReadOnlyUserOverRDSIAMAuth(t *testing.T) {
	env := testenv.Need(t, "REPORT_IT_RDS_HOST", "REPORT_IT_RDS_PORT", "REPORT_IT_RDS_DB", "REPORT_IT_RDS_USER", "AWS_REGION")
	cfg, err := awscfg.Load(ctx, env["AWS_REGION"])
	if err != nil {
		t.Fatal(err)
	}
	var port int
	if _, err := fmt.Sscanf(env["REPORT_IT_RDS_PORT"], "%d", &port); err != nil {
		t.Fatal(err)
	}
	host := env["REPORT_IT_RDS_HOST"]
	pool, err := pgdb.Connect(ctx, pgdb.Config{Host: host, Port: port, Name: env["REPORT_IT_RDS_DB"], MaxConns: 1},
		pgdb.IAMAuth{Host: host, Port: port, User: env["REPORT_IT_RDS_USER"], Region: env["AWS_REGION"], Provider: cfg.Credentials})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := (&rpg.Source{DB: pool}).SalesBetween(ctx, domain.DayWindow(time.Now().UTC().Add(-48*time.Hour))); err != nil {
		t.Fatalf("report query as the read-only user failed: %v", err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM orders WHERE false"); err == nil {
		t.Fatal("the report user can write to orders")
	}
}
