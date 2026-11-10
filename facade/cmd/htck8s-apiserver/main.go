// Command htck8s-apiserver serves a kubectl-compatible Kubernetes API backed
// by an HTCondor access point.
//
//	htck8s-apiserver --collector=<host:port> --spool-root=/var/lib/htck8s
//	kubectl --server=http://localhost:8080 get pods
//
// There is no authentication and no TLS. The process acts as whatever pool
// identity it already has, so anything it can be asked to do over HTTP, it
// does as that identity: bind it to localhost and put a real front end in
// front of it before it is anywhere a stranger can reach.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/bbockelm/htcondor-k8s/facade/internal/apiserver"
	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/source"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "htck8s-apiserver:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen       = flag.String("listen", "127.0.0.1:8080", "address to serve the Kubernetes API on")
		collector    = flag.String("collector", "", "collector address used to locate the schedd")
		scheddName   = flag.String("schedd", "", "schedd name; empty means the pool's only schedd")
		scheddAddr   = flag.String("schedd-address", "", "schedd sinful string, bypassing the collector")
		spoolRoot    = flag.String("spool-root", "", "directory under which each object gets a job spool directory; required for `kubectl logs`")
		sourceKind   = flag.String("source", "poll", "where change events come from: poll or htcondordb")
		pollInterval = flag.Duration("poll-interval", 2*time.Second, "how often the poll source re-reads the queue")
		dbAddress    = flag.String("htcondordb-address", "", "htcondordb sinful string or address file, for --source=htcondordb")
		dbTCP        = flag.String("htcondordb-tcp", "", "unauthenticated host:port for an htcondordb server; for testing only")

		maxCPU    = flag.Int64("max-cpu", 0, "largest CPU request admitted; 0 means no local bound")
		maxMemory = flag.String("max-memory", "", "largest memory request admitted, as a Kubernetes quantity")
		maxDisk   = flag.String("max-disk", "", "largest ephemeral-storage request admitted, as a Kubernetes quantity")
		gpuNames  = flag.String("gpu-resources", "", "comma-separated extended resource names that map to request_gpus")
		nodeAttrs = flag.String("node-selector-attrs", "", "comma-separated key=MachineAttribute pairs that make a nodeSelector key admissible")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	caps, err := capabilities(*maxCPU, *maxMemory, *maxDisk, *gpuNames, *nodeAttrs)
	if err != nil {
		return err
	}

	sched, err := connect(ctx, *collector, *scheddName, *scheddAddr)
	if err != nil {
		return err
	}

	// The generation must be larger than any this process used before, and
	// with no persistent state the clock is the only thing that qualifies.
	// A restart therefore invalidates every open watch, which is correct
	// but not free; see docs/FACADE.md.
	st := store.New(uint64(time.Now().UnixMilli()))

	src, err := eventSource(*sourceKind, sched, *pollInterval, *dbAddress, *dbTCP, log)
	if err != nil {
		return err
	}
	go func() {
		if err := src.Run(ctx, st); err != nil && ctx.Err() == nil {
			log.Error("the event source stopped", "source", src.Name(), "err", err)
		}
	}()

	srv := apiserver.New(&apiserver.Server{
		Backend:    sched,
		Store:      st,
		Caps:       caps,
		SpoolRoot:  *spoolRoot,
		ScheddName: sched.Name(),
		Log:        log,
	})

	httpServer := &http.Server{
		Addr:    *listen,
		Handler: srv,
		// No write timeout: a watch is a long-lived stream and a
		// deadline here would cut every informer off mid-flight.
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
	}()

	log.Info("serving the Kubernetes API facade",
		"listen", *listen, "schedd", sched.Name(), "source", src.Name(), "spool_root", *spoolRoot)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func connect(ctx context.Context, collector, name, addr string) (*backend.Schedd, error) {
	if addr != "" {
		return backend.NewSchedd(newDirectSchedd(name, addr)), nil
	}
	if collector == "" {
		return nil, fmt.Errorf("one of --collector or --schedd-address is required")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return backend.Locate(dialCtx, collector, name)
}

func eventSource(kind string, sched *backend.Schedd, interval time.Duration, dbAddr, dbTCP string, log *slog.Logger) (source.JobEventSource, error) {
	switch kind {
	case "poll":
		return &source.Poll{Backend: sched, Interval: interval, Log: log}, nil
	case "htcondordb":
		switch {
		case dbTCP != "":
			return &source.HTCondorDB{Dial: source.StreamDialer(dbTCP), Log: log}, nil
		case dbAddr != "":
			return &source.HTCondorDB{Dial: source.CedarDialer(dbAddr), Log: log}, nil
		default:
			return nil, fmt.Errorf("--source=htcondordb needs --htcondordb-address")
		}
	default:
		return nil, fmt.Errorf("unknown --source %q; use poll or htcondordb", kind)
	}
}

func capabilities(maxCPU int64, maxMem, maxDisk, gpus, nodeAttrs string) (translate.Capabilities, error) {
	caps := translate.DefaultCapabilities()
	caps.MaxCPU = maxCPU
	if maxMem != "" {
		q, err := resource.ParseQuantity(maxMem)
		if err != nil {
			return caps, fmt.Errorf("--max-memory: %w", err)
		}
		caps.MaxMemoryBytes = q.Value()
	}
	if maxDisk != "" {
		q, err := resource.ParseQuantity(maxDisk)
		if err != nil {
			return caps, fmt.Errorf("--max-disk: %w", err)
		}
		caps.MaxDiskBytes = q.Value()
	}
	for _, name := range splitNonEmpty(gpus) {
		caps.GPUResourceNames = append(caps.GPUResourceNames, name)
	}
	for _, pair := range splitNonEmpty(nodeAttrs) {
		key, attr, ok := strings.Cut(pair, "=")
		if !ok {
			return caps, fmt.Errorf("--node-selector-attrs: %q is not key=MachineAttribute", pair)
		}
		caps.NodeSelectorAttrs[key] = attr
	}
	return caps, nil
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
