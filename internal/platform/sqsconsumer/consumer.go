// Package sqsconsumer is the shared Amazon SQS consumer loop used by
// reservations (inventory queue), orders-worker (payments queue) and
// notifications-worker.
//
// Delivery model you must understand to read this file: SQS standard queues are
// AT-LEAST-ONCE and unordered. A message is hidden from other consumers for the
// "visibility timeout" after it is received; if it is not DELETED before that
// time, it becomes visible again and is delivered again. After the queue's
// maxReceiveCount failed receives, SQS moves it to the dead-letter queue (DLQ).
// So:
//
//   - the handler must be IDEMPOTENT (it can see the same message twice);
//   - we delete only after the handler succeeded;
//   - on failure we do NOT delete and do NOT hurry: SQS redelivers after the
//     visibility timeout, and eventually the DLQ catches poison messages.
//
// Loop behaviour (all required by the Gatepass spec):
//   - long polling, WaitTimeSeconds=20 (fewer empty receives = lower cost, lower latency)
//   - batch up to 10 messages per receive, but never more than free worker slots
//   - a fixed-size worker pool (back-pressure: we do not pull what we cannot process)
//   - while a handler runs, a heartbeat extends the visibility timeout so slow
//     handlers are not redelivered mid-flight
//   - W3C trace context is read from the message attribute "traceparent"
//   - on shutdown: stop polling, let in-flight handlers finish
//
// IAM actions: sqs:ReceiveMessage, sqs:DeleteMessage, sqs:ChangeMessageVisibility
// on the queue ARN (+ kms:Decrypt if the queue uses a customer-managed key).
package sqsconsumer

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// API is the subset of *sqs.Client the loop uses. Declaring it here lets unit
// tests substitute a small in-memory fake of OUR interface (not a mock of the SDK).
type API interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// Message is what handlers see.
type Message struct {
	ID           string
	Body         string
	Attributes   map[string]string // SQS message attributes (string values only)
	ReceiveCount int               // ApproximateReceiveCount: 1 on first delivery
}

// Handler processes one message. Return nil = success (message is deleted).
// Return an error = failure (message stays; SQS redelivers, then DLQ).
// To permanently discard a message you decided is hopeless but harmless, log it
// and return nil.
type Handler func(ctx context.Context, m Message) error

// Config configures a Consumer.
type Config struct {
	Name     string
	QueueURL string
	Workers  int           // default 4
	Wait     time.Duration // long poll, default 20s (SQS max)
	// Visibility is the visibility timeout we set while processing and keep
	// extending. Default 60s. It should be >= the handler's normal duration.
	Visibility time.Duration
	Client     API
	Handler    Handler
}

// Consumer implements lifecycle.Component.
type Consumer struct{ cfg Config }

// New validates cfg and applies defaults.
func New(cfg Config) (*Consumer, error) {
	if cfg.Client == nil || cfg.Handler == nil || cfg.QueueURL == "" {
		return nil, errors.New("sqsconsumer: Client, Handler and QueueURL are required")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.Wait <= 0 || cfg.Wait > 20*time.Second {
		cfg.Wait = 20 * time.Second
	}
	if cfg.Visibility <= 0 {
		cfg.Visibility = 60 * time.Second
	}
	return &Consumer{cfg: cfg}, nil
}

// Name implements lifecycle.Component.
func (c *Consumer) Name() string { return "sqs-consumer-" + c.cfg.Name }

var (
	mReceived = telemetry.Counter("sqs_messages_received_total", "SQS messages received by consumers")
	mFailed   = telemetry.Counter("sqs_messages_failed_total", "SQS messages whose handler returned an error")
)

// Run polls until ctx is cancelled, then waits for in-flight handlers.
func (c *Consumer) Run(ctx context.Context) error {
	sem := make(chan struct{}, c.cfg.Workers)
	var wg sync.WaitGroup
	defer wg.Wait() // graceful drain: lifecycle bounds this with its drain timeout

	backoff := time.Second
	for ctx.Err() == nil {
		// Only ask for as many messages as we have free workers.
		free := cap(sem) - len(sem)
		if free == 0 {
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
			}
			continue
		}
		max := min(free, 10)

		// sqs:ReceiveMessage - long poll. Failure modes: AccessDenied (IAM or
		// queue policy), QueueDoesNotExist (wrong URL), throttling/5xx (the SDK
		// retries; we also back off), context cancelled on shutdown.
		out, err := c.cfg.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(c.cfg.QueueURL),
			MaxNumberOfMessages:         int32(max),
			WaitTimeSeconds:             int32(c.cfg.Wait / time.Second),
			VisibilityTimeout:           int32(c.cfg.Visibility / time.Second),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
			MessageAttributeNames:       []string{"All"},
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.ErrorContext(ctx, "sqs receive failed", "queue", c.cfg.Name, "error", err.Error(), "retry_in", backoff.String())
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
			}
			backoff = min(backoff*2, 30*time.Second) // bounded exponential backoff
			continue
		}
		backoff = time.Second

		for _, m := range out.Messages {
			sem <- struct{}{} // cannot block: we asked for at most `free` messages
			wg.Add(1)
			mReceived.Add(ctx, 1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				c.process(ctx, m)
			}()
		}
	}
	return nil
}

func (c *Consumer) process(parent context.Context, raw types.Message) {
	// In-flight work must not be cut short by shutdown: detach from cancellation.
	// (lifecycle's drain timeout is the upper bound.)
	ctx := context.WithoutCancel(parent)

	msg := Message{ID: aws.ToString(raw.MessageId), Body: aws.ToString(raw.Body), Attributes: map[string]string{}}
	for k, v := range raw.MessageAttributes {
		if v.StringValue != nil {
			msg.Attributes[k] = *v.StringValue
		}
	}
	if n, err := strconv.Atoi(raw.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]); err == nil {
		msg.ReceiveCount = n
	}
	// Give log lines a request_id (the SQS message id) and a settable user_id, the
	// same correlation fields HTTP requests have.
	ctx, _ = logging.WithReqInfo(ctx, msg.ID)
	// Continue the producer's trace.
	ctx = telemetry.ContextWithTraceparent(ctx, msg.Attributes["traceparent"])
	ctx, span := telemetry.Tracer().Start(ctx, "sqs.process "+c.cfg.Name)
	defer span.End()

	// Heartbeat: keep the message invisible while the handler runs. Without it a
	// handler slower than the visibility timeout would run TWICE concurrently.
	hbCtx, stopHB := context.WithCancel(ctx)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		c.heartbeat(hbCtx, raw.ReceiptHandle)
	}()

	err := c.cfg.Handler(ctx, msg)
	stopHB()
	<-hbDone

	if err != nil {
		// Do NOT delete. SQS will redeliver after the visibility timeout; after
		// maxReceiveCount receives it goes to the DLQ.
		mFailed.Add(ctx, 1)
		slog.ErrorContext(ctx, "message handler failed; leaving message for redelivery",
			"queue", c.cfg.Name, "message_id", msg.ID, "receive_count", msg.ReceiveCount, "error", err.Error())
		return
	}

	// sqs:DeleteMessage - acknowledge. If this call fails the message will be
	// redelivered even though we processed it, which is exactly why handlers
	// must be idempotent.
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, derr := c.cfg.Client.DeleteMessage(dctx, &sqs.DeleteMessageInput{
		QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: raw.ReceiptHandle,
	}); derr != nil {
		slog.ErrorContext(ctx, "delete after success failed; message will be redelivered",
			"queue", c.cfg.Name, "message_id", msg.ID, "error", derr.Error())
	}
}

// heartbeat extends visibility every Visibility/2 until ctx is cancelled.
func (c *Consumer) heartbeat(ctx context.Context, receipt *string) {
	t := time.NewTicker(c.cfg.Visibility / 2)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// sqs:ChangeMessageVisibility - resets the timer to Visibility from
			// now. Fails with ReceiptHandleIsInvalid if the message was already
			// redelivered to someone else; then there is nothing more to do.
			vctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := c.cfg.Client.ChangeMessageVisibility(vctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: receipt,
				VisibilityTimeout: int32(c.cfg.Visibility / time.Second),
			})
			cancel()
			if err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "extend visibility failed", "queue", c.cfg.Name, "error", err.Error())
			}
		}
	}
}
