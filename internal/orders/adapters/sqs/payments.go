// Package sqs (orders adapter) sends payment requests to the payments queue and
// turns received ones into worker calls.
//
// HEXAGONAL BOUNDARY: adapter (driven side: Enqueue; driving side: Handler).
//
// Queue: standard SQS queue `orders-payments` with a dead-letter queue
// (maxReceiveCount 5). Messages are JSON {orderId, amountCents, attempt[, poison]}
// with the W3C trace context in the message attribute "traceparent".
//
// IAM: sqs:SendMessage on the queue (orders-api and the relay's reconciler);
// sqs:ReceiveMessage, DeleteMessage, ChangeMessageVisibility (orders-worker).
// Failure looks like: AccessDenied (role), QueueDoesNotExist (wrong URL),
// KMS.AccessDeniedException if the queue uses a customer-managed KMS key.
package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/olagookundavid/gatepass-app/internal/orders/app"
	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/sqsconsumer"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// Sender is the subset of *sqs.Client used to send.
type Sender interface {
	SendMessage(ctx context.Context, in *awssqs.SendMessageInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

// Queue implements ports.Payments.
type Queue struct {
	Client   Sender
	QueueURL string
}

var _ ports.Payments = (*Queue)(nil)

// Enqueue sends the request.
func (q *Queue) Enqueue(ctx context.Context, r domain.PaymentRequest) error {
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	attrs := map[string]types.MessageAttributeValue{}
	if tp := telemetry.TraceparentFromContext(ctx); tp != "" {
		attrs["traceparent"] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(tp)}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// sqs:SendMessage. Standard queue: the same request may be delivered twice
	// (and our own retries may enqueue twice); the worker is idempotent.
	_, err = q.Client.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: aws.String(q.QueueURL), MessageBody: aws.String(string(body)), MessageAttributes: attrs})
	if err != nil {
		return fmt.Errorf("send payment request: %w", err)
	}
	return nil
}

// Handler adapts the payment worker to the shared consumer loop.
func Handler(w *app.PaymentWorker) sqsconsumer.Handler {
	return func(ctx context.Context, m sqsconsumer.Message) error {
		var req domain.PaymentRequest
		if err := json.Unmarshal([]byte(m.Body), &req); err != nil {
			// Unparseable: it will never succeed. Error -> redelivery -> DLQ.
			return fmt.Errorf("unparseable payment message %s: %w", m.ID, err)
		}
		return w.Process(ctx, req)
	}
}
