// Package awscfg is the AWS client factory: one place that builds the SDK
// configuration every service uses.
//
// Rules enforced here:
//   - Credentials come ONLY from the SDK default chain. In EKS that resolves to
//     IRSA / EKS Pod Identity (a role per Kubernetes service account); in a
//     Lambda to the function's execution role. There is no code path for static
//     access keys, and none must ever be added.
//   - The region comes from AWS_REGION (validated by config.LoadCommon).
//   - The standard retryer with a bounded attempt count (AWS_MAX_ATTEMPTS, default 3)
//     handles throttling and 5xx with exponential backoff and jitter. "Unbounded
//     retries" turn a small AWS outage into a retry storm.
//   - Every SDK call is traced (otelaws middleware) so AWS latency shows up as
//     child spans of the request that caused it.
//
// IAM: none by itself; every adapter documents the actions it needs.
package awscfg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/smithy-go"
	// The otelaws module is on the approved list but upstream marks it deprecated
	// (the SDK has native smithy tracing now). Kept deliberately; see HANDOFF.md.
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws" //nolint:staticcheck // SA1019, see comment above
)

// DefaultCallTimeout is the deadline adapters apply to a single AWS call.
const DefaultCallTimeout = 5 * time.Second

// Load builds the shared aws.Config.
func Load(ctx context.Context, region string) (aws.Config, error) {
	if region == "" {
		return aws.Config{}, errors.New("awscfg: region is empty (AWS_REGION)")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		// AWS_MAX_ATTEMPTS, if set, overrides this default inside the SDK.
		awsconfig.WithRetryMaxAttempts(3),
		awsconfig.WithRetryMode(aws.RetryModeStandard),
	)
	if err != nil {
		return aws.Config{}, fmt.Errorf("load aws config: %w", err)
	}
	otelaws.AppendMiddlewares(&cfg.APIOptions)
	return cfg, nil
}

// IsThrottle reports whether err is an AWS throttling error (after the SDK's
// own retries were exhausted).
func IsThrottle(err error) bool {
	if err == nil {
		return false
	}
	_, throttled := retry.DefaultThrottleErrorCodes[ErrorCode(err)]
	return throttled
}

// ErrorCode returns the AWS error code ("ConditionalCheckFailedException",
// "ThrottlingException", ...) or "".
func ErrorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// LogThrottle logs a warning if err is a throttle, so throttling is visible.
func LogThrottle(ctx context.Context, op string, err error) {
	if IsThrottle(err) {
		slog.WarnContext(ctx, "aws call throttled", "operation", op, "code", ErrorCode(err))
	}
}
