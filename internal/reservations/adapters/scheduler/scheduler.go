// Package scheduler (reservations adapter) implements ports.Scheduler with
// Amazon EventBridge Scheduler one-time schedules.
//
// HEXAGONAL BOUNDARY: adapter.
//
// Why a schedule per hold and not DynamoDB TTL: TTL deletes items "typically
// within a few days" of the timestamp, so it cannot release tickets 10 minutes
// after a hold. EventBridge Scheduler can invoke a target at an exact time:
// `at(2026-06-01T12:10:00)`. The schedule invokes the hold-expirer Lambda with
// {"holdId": "..."} and, with ActionAfterCompletion=DELETE, removes itself.
//
// IAM for the caller (reservations-api):
//
//	scheduler:CreateSchedule  on arn:aws:scheduler:<region>:<account-id>:schedule/<SCHEDULER_GROUP_NAME>/hold-*
//	scheduler:DeleteSchedule  on the same resource
//	iam:PassRole              on SCHEDULER_ROLE_ARN, with condition
//	                          iam:PassedToService = scheduler.amazonaws.com
//
// iam:PassRole is required because creating a schedule hands a role to the
// Scheduler service ("the caller must be allowed to pass the execution role");
// without it CreateSchedule fails with AccessDeniedException mentioning iam:PassRole.
// The passed role (SCHEDULER_ROLE_ARN) must trust scheduler.amazonaws.com and
// allow lambda:InvokeFunction on EXPIRER_LAMBDA_ARN.
//
// Failure shapes: ConflictException (name exists: we treat it as success, the
// name is derived from the hold id), ValidationException (the `at()` time is in
// the past), ServiceQuotaExceededException, ThrottlingException,
// ResourceNotFoundException on delete (already ran or never existed: ignored).
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsscheduler "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"

	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

// API is the subset of *scheduler.Client used.
type API interface {
	CreateSchedule(ctx context.Context, in *awsscheduler.CreateScheduleInput, opts ...func(*awsscheduler.Options)) (*awsscheduler.CreateScheduleOutput, error)
	DeleteSchedule(ctx context.Context, in *awsscheduler.DeleteScheduleInput, opts ...func(*awsscheduler.Options)) (*awsscheduler.DeleteScheduleOutput, error)
}

// Scheduler implements ports.Scheduler.
type Scheduler struct {
	Client    API
	Group     string // SCHEDULER_GROUP_NAME
	RoleARN   string // SCHEDULER_ROLE_ARN: role Scheduler assumes to invoke the target
	TargetARN string // EXPIRER_LAMBDA_ARN
}

var _ ports.Scheduler = (*Scheduler)(nil)

// Name is the deterministic schedule name for a hold.
func Name(holdID string) string { return "hold-" + holdID }

// ScheduleExpiry creates the one-time schedule.
func (s *Scheduler) ScheduleExpiry(ctx context.Context, holdID string, at time.Time) error {
	input, err := json.Marshal(map[string]string{"holdId": holdID})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, awscfg.DefaultCallTimeout)
	defer cancel()
	name := Name(holdID)
	_, err = s.Client.CreateSchedule(ctx, &awsscheduler.CreateScheduleInput{
		Name:                       aws.String(name),
		GroupName:                  aws.String(s.Group),
		ScheduleExpression:         aws.String("at(" + at.UTC().Format("2006-01-02T15:04:05") + ")"),
		ScheduleExpressionTimezone: aws.String("UTC"),
		FlexibleTimeWindow:         &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
		ActionAfterCompletion:      types.ActionAfterCompletionDelete,
		ClientToken:                aws.String(name), // makes SDK retries of this call idempotent
		Target: &types.Target{
			Arn:     aws.String(s.TargetARN),
			RoleArn: aws.String(s.RoleARN),
			Input:   aws.String(string(input)),
			// If the Lambda invocation fails Scheduler retries with backoff; the
			// Lambda is idempotent so retries are safe.
			RetryPolicy: &types.RetryPolicy{MaximumRetryAttempts: aws.Int32(5), MaximumEventAgeInSeconds: aws.Int32(3600)},
		},
	})
	var conflict *types.ConflictException
	if errors.As(err, &conflict) {
		return nil // already created by an earlier attempt of the same request
	}
	if err != nil {
		awscfg.LogThrottle(ctx, "scheduler.CreateSchedule", err)
		return fmt.Errorf("create schedule %s: %w", name, err)
	}
	return nil
}

// CancelExpiry deletes the schedule; "not found" is success.
func (s *Scheduler) CancelExpiry(ctx context.Context, holdID string) error {
	ctx, cancel := context.WithTimeout(ctx, awscfg.DefaultCallTimeout)
	defer cancel()
	_, err := s.Client.DeleteSchedule(ctx, &awsscheduler.DeleteScheduleInput{Name: aws.String(Name(holdID)), GroupName: aws.String(s.Group)})
	var nf *types.ResourceNotFoundException
	if errors.As(err, &nf) {
		return nil
	}
	return err
}
