package sqsconsumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// fakeSQS is an in-memory implementation of OUR API interface.
type fakeSQS struct {
	mu       sync.Mutex
	pending  []types.Message
	deleted  []string
	extended int
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	n := min(len(f.pending), int(in.MaxNumberOfMessages))
	out := f.pending[:n]
	f.pending = f.pending[n:]
	f.mu.Unlock()
	if len(out) == 0 {
		select { // emulate long polling
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &sqs.ReceiveMessageOutput{Messages: out}, nil
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	f.extended++
	f.mu.Unlock()
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func msg(id, body string) types.Message {
	return types.Message{MessageId: aws.String(id), Body: aws.String(body), ReceiptHandle: aws.String("rh-" + id),
		Attributes: map[string]string{"ApproximateReceiveCount": "1"}}
}

func TestDeletesOnlyAfterSuccess(t *testing.T) {
	f := &fakeSQS{pending: []types.Message{msg("ok", "good"), msg("bad", "fail")}}
	c, err := New(Config{Name: "t", QueueURL: "https://sqs.example.com/q", Client: f, Workers: 2,
		Handler: func(_ context.Context, m Message) error {
			if m.Body == "fail" {
				return errors.New("boom")
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deleted) != 1 || f.deleted[0] != "rh-ok" {
		t.Fatalf("deleted = %v; only the successful message may be deleted", f.deleted)
	}
}

func TestHeartbeatExtendsVisibilityForSlowHandlers(t *testing.T) {
	f := &fakeSQS{pending: []types.Message{msg("slow", "x")}}
	c, _ := New(Config{Name: "t", QueueURL: "q", Client: f, Visibility: 40 * time.Millisecond,
		Handler: func(context.Context, Message) error { time.Sleep(150 * time.Millisecond); return nil }})
	// Visibility is truncated to whole seconds when sent to SQS, but the ticker
	// uses the raw duration, so the heartbeat fires several times here.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.extended < 2 {
		t.Fatalf("visibility extended %d times, want >= 2", f.extended)
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error")
	}
}
