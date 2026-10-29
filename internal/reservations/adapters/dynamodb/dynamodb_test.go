package dynamodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

// scripted returns the queued errors from TransactWriteItems, then success.
type scripted struct {
	API
	errs   []error
	calls  int
	tokens []string
}

func (s *scripted) TransactWriteItems(_ context.Context, in *ddb.TransactWriteItemsInput, _ ...func(*ddb.Options)) (*ddb.TransactWriteItemsOutput, error) {
	s.calls++
	s.tokens = append(s.tokens, aws.ToString(in.ClientRequestToken))
	if len(s.errs) > 0 {
		e := s.errs[0]
		s.errs = s.errs[1:]
		return nil, e
	}
	return &ddb.TransactWriteItemsOutput{}, nil
}

func cancelled(codes ...string) error {
	rs := make([]types.CancellationReason, len(codes))
	for i, c := range codes {
		rs[i] = types.CancellationReason{Code: aws.String(c)}
	}
	return &types.TransactionCanceledException{CancellationReasons: rs}
}

var hold = domain.Hold{ID: "h1", UserID: "u", EventID: "e", TicketTypeID: "t", Quantity: 1, Status: domain.StatusActive,
	CreatedAt: time.Unix(1_700_000_000, 0).UTC(), ExpiresAt: time.Unix(1_700_000_600, 0).UTC()}

func create(s *Store) error {
	return s.CreateHold(context.Background(), hold, "key", ports.IdempotencyRecord{RequestHash: "x", Hold: hold}, time.Hour)
}

func TestTransactionConflictIsRetriedWithSameToken(t *testing.T) {
	c := &scripted{errs: []error{cancelled("TransactionConflict", "None", "None"), cancelled("TransactionConflict", "None", "None")}}
	if err := create(&Store{Client: c, Table: "t"}); err != nil {
		t.Fatalf("conflicts must be retried: %v", err)
	}
	if c.calls != 3 || c.tokens[0] != "h1" || c.tokens[2] != "h1" {
		t.Fatalf("calls=%d tokens=%v", c.calls, c.tokens)
	}
}

func TestPersistentContentionSurfacesAsErrContention(t *testing.T) {
	errs := make([]error, 20)
	for i := range errs {
		errs[i] = cancelled("TransactionConflict", "None", "None")
	}
	c := &scripted{errs: errs}
	err := create(&Store{Client: c, Table: "t"})
	if !errors.Is(err, domain.ErrContention) || c.calls != 8 {
		t.Fatalf("err=%v calls=%d (retries must be bounded)", err, c.calls)
	}
}

func TestConditionFailuresAreMappedNotRetried(t *testing.T) {
	cases := []struct {
		reasons []string
		want    error
	}{
		{[]string{"ConditionalCheckFailed", "None", "None"}, domain.ErrSoldOut},
		{[]string{"None", "None", "ConditionalCheckFailed"}, domain.ErrIdempotencyInUse},
	}
	for _, tc := range cases {
		c := &scripted{errs: []error{cancelled(tc.reasons...)}}
		if err := create(&Store{Client: c, Table: "t"}); !errors.Is(err, tc.want) || c.calls != 1 {
			t.Errorf("%v: err=%v calls=%d", tc.reasons, err, c.calls)
		}
	}
}

func TestItemRoundTrip(t *testing.T) {
	h := hold
	h.OrderID = "o1"
	got := HoldFromItem(holdToItem(h, h.ExpiresAt))
	if got != h {
		t.Fatalf("round trip lost data:\n got %+v\nwant %+v", got, h)
	}
	it := holdToItem(h, h.ExpiresAt)
	if gstr(it, "GSI1PK") != "USER#u" || gstr(it, "PK") != "HOLD#h1" || gnum(it, "ttl") != h.ExpiresAt.Unix() {
		t.Fatalf("keys wrong: %v", it)
	}
}
