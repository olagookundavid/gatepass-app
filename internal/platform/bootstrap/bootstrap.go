// Package bootstrap holds the few lines every main() starts with: logger,
// telemetry, AWS config. Keeping them here means each cmd/*/main.go reads as
// "wire the adapters to the use cases", nothing else.
//
// AWS services touched: none directly (builds the SDK config used by adapters).
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/olagookundavid/gatepass-app/internal/platform/awscfg"
	"github.com/olagookundavid/gatepass-app/internal/platform/buildinfo"
	"github.com/olagookundavid/gatepass-app/internal/platform/config"
	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// Env is what Init produces.
type Env struct {
	AWS aws.Config
	// FlushTelemetry must be run as a closer during graceful shutdown.
	FlushTelemetry func(context.Context) error
}

// Init installs the default logger, starts telemetry and loads AWS config.
func Init(ctx context.Context, service string, c config.Common) (*Env, error) {
	slog.SetDefault(logging.New(os.Stdout, service, buildinfo.Version, c.LogLevel))
	flush, err := telemetry.Setup(ctx, service, buildinfo.Version)
	if err != nil {
		return nil, fmt.Errorf("telemetry: %w", err)
	}
	awsCfg, err := awscfg.Load(ctx, c.AWSRegion)
	if err != nil {
		return nil, err
	}
	slog.Info("starting", "service", service, "version", buildinfo.Version, "build_time", buildinfo.BuildTime)
	return &Env{AWS: awsCfg, FlushTelemetry: flush}, nil
}

// Fatal logs the error and exits 1. Used for startup failures only. Config
// errors are printed to stderr as plain text as well, because the logger may
// not exist yet.
func Fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	slog.Error("fatal", "error", err.Error())
	os.Exit(1)
}
