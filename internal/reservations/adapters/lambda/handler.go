// Package lambda (reservations adapter) is the logic of the hold-expirer Lambda.
//
// HEXAGONAL BOUNDARY: adapter, "driving" side (an AWS event instead of HTTP).
// cmd/lambda-hold-expirer/main.go only wires it and calls lambda.Start.
//
// ONE handler, TWO input shapes, detected from the payload:
//
//  1. EventBridge Scheduler (primary path). The one-time schedule created by
//     reservations-api invokes the function with exactly {"holdId":"..."} at the
//     hold's expiry time. We release the tickets if the hold is still active.
//
//  2. DynamoDB Streams (safety net). If the Scheduler path failed, the hold row is
//     eventually deleted by DynamoDB TTL ("typically within a few days"). The
//     stream then carries a REMOVE record whose userIdentity says the deletion was
//     done by the TTL service (type "Service", principalId
//     "dynamodb.amazonaws.com"). Only for those records, and only if the deleted
//     hold was still `active`, we return its tickets. Records for other
//     items (idempotency keys, processed-event markers, inventory) and user-made
//     deletes are ignored.
//     The event source mapping must enable ReportBatchItemFailures so we can tell
//     Lambda which records to retry (BatchItemFailures) instead of failing the batch.
//
// Both paths are safe to run for the same hold, any number of times, in any
// order: the release itself is a conditional DynamoDB transaction (see
// adapters/dynamodb ReleaseHold / ReleaseDeletedHold).
//
// IAM (function execution role): dynamodb:GetItem, UpdateItem, PutItem,
// TransactWriteItems(+ the item actions) on the table; for the stream path also
// dynamodb:DescribeStream, GetRecords, GetShardIterator, ListStreams on the
// stream ARN; CloudWatch Logs.
//
// The business metric holds_expired_released_total is emitted as a CloudWatch
// Embedded Metric Format (EMF) log line, which CloudWatch turns into a metric
// without any API call. UNVERIFIED until seen in a real account.
package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	rddb "github.com/olagookundavid/gatepass-app/internal/reservations/adapters/dynamodb"
	"github.com/olagookundavid/gatepass-app/internal/reservations/app"
	"github.com/olagookundavid/gatepass-app/internal/reservations/domain"
)

// Handler holds the use case.
type Handler struct {
	Svc *app.Service
	// Emit writes the EMF metric line; defaults to slog. Tests replace it.
	Emit func(ctx context.Context, name string, value int)
}

// SchedulerInput is the payload created by reservations-api.
type SchedulerInput struct {
	HoldID string `json:"holdId"`
}

// SchedulerOutput is returned to the invoker (visible in logs only).
type SchedulerOutput struct {
	Released bool `json:"released"`
}

// Handle detects the payload shape and dispatches.
func (h *Handler) Handle(ctx context.Context, raw json.RawMessage) (any, error) {
	var probe struct {
		HoldID  string            `json:"holdId"`
		Records []json.RawMessage `json:"Records"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("payload is not JSON: %w", err)
	}
	switch {
	case probe.HoldID != "":
		return h.handleScheduler(ctx, probe.HoldID)
	case len(probe.Records) > 0:
		var ev events.DynamoDBEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil, fmt.Errorf("payload is not a DynamoDB stream event: %w", err)
		}
		return h.handleStream(ctx, ev), nil
	default:
		return nil, errors.New("unrecognised payload: expected {\"holdId\":...} or a DynamoDB stream event")
	}
}

func (h *Handler) handleScheduler(ctx context.Context, holdID string) (SchedulerOutput, error) {
	// Returning an error makes EventBridge Scheduler retry per the schedule's retry
	// policy (used when the function fires a moment before expiresAt).
	released, err := h.Svc.ExpireHold(ctx, holdID)
	if err != nil {
		slog.ErrorContext(ctx, "expire hold failed", "hold_id", holdID, "error", err.Error())
		return SchedulerOutput{}, err
	}
	if released {
		h.emit(ctx, 1)
	}
	slog.InfoContext(ctx, "scheduler invocation done", "hold_id", holdID, "released", released)
	return SchedulerOutput{Released: released}, nil
}

func (h *Handler) handleStream(ctx context.Context, ev events.DynamoDBEvent) events.DynamoDBEventResponse {
	resp := events.DynamoDBEventResponse{BatchItemFailures: []events.DynamoDBBatchItemFailure{}}
	for _, rec := range ev.Records {
		hold, ok := ttlDeletedActiveHold(rec)
		if !ok {
			continue
		}
		released, err := h.Svc.ExpireDeletedHold(ctx, hold)
		if err != nil {
			slog.ErrorContext(ctx, "stream release failed; will be retried", "hold_id", hold.ID, "error", err.Error())
			// For DynamoDB streams the item identifier is the record's sequence number.
			resp.BatchItemFailures = append(resp.BatchItemFailures,
				events.DynamoDBBatchItemFailure{ItemIdentifier: rec.Change.SequenceNumber})
			continue
		}
		if released {
			h.emit(ctx, 1)
		}
	}
	return resp
}

// ttlDeletedActiveHold reports whether rec is a TTL deletion of an active hold,
// and returns that hold.
func ttlDeletedActiveHold(rec events.DynamoDBEventRecord) (domain.Hold, bool) {
	if rec.EventName != "REMOVE" {
		return domain.Hold{}, false
	}
	ui := rec.UserIdentity
	if ui == nil || ui.Type != "Service" || ui.PrincipalID != "dynamodb.amazonaws.com" {
		return domain.Hold{}, false // a human or application deleted it, not TTL
	}
	img := convert(rec.Change.OldImage)
	if !strings.HasPrefix(attrS(img, "PK"), "HOLD#") {
		return domain.Hold{}, false
	}
	h := rddb.HoldFromItem(img)
	if h.ID == "" || h.Status != domain.StatusActive {
		return domain.Hold{}, false
	}
	return h, true
}

func attrS(it map[string]types.AttributeValue, k string) string {
	if v, ok := it[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

// convert maps the Lambda event representation of an item to the SDK types the
// adapter already knows how to read. Only strings and numbers are needed.
func convert(in map[string]events.DynamoDBAttributeValue) map[string]types.AttributeValue {
	out := make(map[string]types.AttributeValue, len(in))
	for k, v := range in {
		switch v.DataType() {
		case events.DataTypeString:
			out[k] = &types.AttributeValueMemberS{Value: v.String()}
		case events.DataTypeNumber:
			out[k] = &types.AttributeValueMemberN{Value: v.Number()}
		}
	}
	return out
}

func (h *Handler) emit(ctx context.Context, n int) {
	if h.Emit != nil {
		h.Emit(ctx, "holds_expired_released_total", n)
		return
	}
	EmitEMF(n)
}

// EmitEMF prints one CloudWatch Embedded Metric Format line to stdout (the Lambda
// log). CloudWatch extracts the metric "holds_expired_released_total" in
// namespace "Gatepass" with dimension Service=hold-expirer.
func EmitEMF(n int) {
	line, _ := json.Marshal(map[string]any{
		"_aws": map[string]any{
			"Timestamp": time.Now().UnixMilli(),
			"CloudWatchMetrics": []map[string]any{{
				"Namespace":  "Gatepass",
				"Dimensions": [][]string{{"Service"}},
				"Metrics":    []map[string]string{{"Name": "holds_expired_released_total", "Unit": "Count"}},
			}},
		},
		"Service":                      "hold-expirer",
		"holds_expired_released_total": n,
	})
	fmt.Println(string(line))
}
