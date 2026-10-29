// Skeleton: health-only server, replaced when the catalog service is wired.
package main

import (
	"log/slog"
	"os"

	"github.com/olagookundavid/gatepass-app/internal/platform/buildinfo"
	"github.com/olagookundavid/gatepass-app/internal/platform/httpserver"
	"github.com/olagookundavid/gatepass-app/internal/platform/lifecycle"
	"github.com/olagookundavid/gatepass-app/internal/platform/logging"
)

func main() {
	slog.SetDefault(logging.New(os.Stdout, "catalog-api", buildinfo.Version, "info"))
	h := httpserver.NewHealth("catalog-api")
	srv := httpserver.New(httpserver.Options{Service: "catalog-api", Addr: ":8080", Health: h})
	if err := lifecycle.Run(lifecycle.Options{SetNotReady: h.SetShuttingDown},
		httpserver.Component{Name_: "public", Srv: srv}); err != nil {
		slog.Error("exit", "error", err.Error())
		os.Exit(1)
	}
}
