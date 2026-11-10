package source

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/htcondordb/watchfeed"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
)

// Watch event kinds, as htcondordb's storage layer numbers them. watchfeed
// passes them through as a bare uint8, so they are repeated here rather than
// pulled in from classad/db, which would drag the whole storage engine into a
// process that only ever reads a socket.
const (
	kindUpsert uint8 = 0
	kindDelete uint8 = 1
	// kindReset means "discard what you have"; an authoritative snapshot
	// follows and ends at kindSynced.
	kindReset uint8 = 2
	// kindSynced ends a catch-up and carries the durable resume cursor.
	// Catch-up events themselves carry no cursor, so this is the only
	// event worth persisting a position from during a rebuild.
	kindSynced uint8 = 3
	// kindResync means the server gave up on this stream (the per-watcher
	// buffer overflowed, or the table was truncated). It carries no cursor
	// and the stream ends immediately after it.
	kindResync uint8 = 4
)

// ScheddJobsTable is where htcondordb's schedd mirror puts live job ads, keyed
// "cluster.proc" -- the same key the facade uses.
const ScheddJobsTable = "jobs"

// HTCondorDB follows htcondordb's change feed over the schedd's mirrored job
// queue.
//
// The feed gives the facade sub-second change notification without polling,
// but it deliberately does *not* give it a resourceVersion. htcondordb's watch
// cursor is a 16-wide vector clock with a per-process random epoch: it is
// opaque, it is not totally ordered, and every restart of the htcondordb
// daemon invalidates it. Handing that to a Kubernetes client as a
// resourceVersion would break the one property clients actually rely on, and
// would turn a routine daemon restart into a fleet-wide 410 storm. So the
// cursor stays private to this source, used only to resume its own stream, and
// the store above it keeps the monotonic counter clients see.
//
// The mapping from feed events to store operations is where the honesty lives:
//
//   - The first reset/replay/synced sequence becomes a Replace. The store is
//     empty, so the diff is exactly the right set of ADDED events.
//   - Any *later* reset becomes a Resync into a new generation. A reset means
//     the server could not replay what happened since our cursor; the snapshot
//     that follows is authoritative about the present but says nothing about
//     the transitions in between, and a watcher resuming across that gap would
//     be told it had seen everything when it had not.
//   - A resync ends the session; this source reconnects from its last cursor
//     without telling the store anything, because nothing is known to be lost
//     until the reconnect says so.
type HTCondorDB struct {
	// Dial opens a connection to an htcondordb server. CedarDialer is the
	// production one; StreamDialer exists so a test can run against an
	// in-process server over plain TCP.
	Dial func(ctx context.Context) (*dbrpc.Client, error)

	// Table defaults to ScheddJobsTable.
	Table string

	// Backoff is the delay between reconnection attempts.
	Backoff time.Duration

	Log *slog.Logger

	cursor     []byte
	everSynced bool
}

func (h *HTCondorDB) Name() string { return "htcondordb-watch" }

func (h *HTCondorDB) Run(ctx context.Context, sink Sink) error {
	backoff := h.Backoff
	if backoff <= 0 {
		backoff = 2 * time.Second
	}
	table := h.Table
	if table == "" {
		table = ScheddJobsTable
	}
	for ctx.Err() == nil {
		if err := h.session(ctx, sink, table); err != nil && ctx.Err() == nil {
			h.logf("htcondordb watch session ended; reconnecting", "err", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
	}
	return ctx.Err()
}

// session runs one connection's worth of the feed and returns when it ends.
func (h *HTCondorDB) session(ctx context.Context, sink Sink, table string) error {
	client, err := h.Dial(ctx)
	if err != nil {
		return fmt.Errorf("connecting to htcondordb: %w", err)
	}
	defer func() { _ = client.Close() }()

	events, stop, wire, err := watchfeed.Watch(ctx, client, table, h.cursor)
	if err != nil {
		// A cursor the server will not take is not fatal: drop it and
		// the next attempt starts from a full snapshot.
		h.cursor = nil
		return fmt.Errorf("starting a watch on %q: %w", table, err)
	}
	defer stop()
	h.logf("watching htcondordb", "table", table, "wire_feed", wire, "resuming", len(h.cursor) > 0)

	var shadow map[string]backend.JobRecord
	for ev := range events {
		if ev.Err != nil {
			// A row that would not decode is one row, not the stream.
			// Dropping it loses a Pod until the next full snapshot,
			// which is better than ending every client's watch.
			h.logf("skipping an undecodable row", "key", ev.Key, "err", ev.Err)
			continue
		}
		switch ev.Kind {
		case kindReset:
			shadow = map[string]backend.JobRecord{}

		case kindSynced:
			if shadow != nil {
				h.install(sink, shadow)
				shadow = nil
			}
			h.everSynced = true
			h.cursor = ev.Cursor

		case kindResync:
			return fmt.Errorf("server asked for a resync on %q", table)

		case kindUpsert:
			rec := backend.FromClassAd(ev.Ad)
			if !rec.Usable() {
				// Not one of ours. During a rebuild it must still be
				// absent from the shadow, which it is.
				if shadow == nil {
					sink.Delete(keyOf(ev.Key, rec))
				}
				break
			}
			if shadow != nil {
				shadow[rec.Key()] = rec
			} else {
				sink.Upsert(rec)
			}
			h.advance(ev.Cursor)

		case kindDelete:
			if shadow != nil {
				delete(shadow, ev.Key)
			} else {
				sink.Delete(ev.Key)
			}
			h.advance(ev.Cursor)
		}
	}
	return nil
}

// install publishes a rebuilt snapshot. The first one lands in the current
// generation; every later one starts a new generation, because a reset after
// we were already live means the server could not tell us what we missed.
func (h *HTCondorDB) install(sink Sink, shadow map[string]backend.JobRecord) {
	recs := make([]backend.JobRecord, 0, len(shadow))
	for _, r := range shadow {
		recs = append(recs, r)
	}
	if h.everSynced {
		h.logf("htcondordb reset after a live stream; starting a new resourceVersion generation", "rows", len(recs))
		sink.Resync(sink.Generation()+1, recs)
		return
	}
	sink.Replace(recs)
}

func (h *HTCondorDB) advance(cursor []byte) {
	if len(cursor) > 0 {
		h.cursor = cursor
	}
}

func (h *HTCondorDB) logf(msg string, args ...any) {
	if h.Log != nil {
		h.Log.Info(msg, args...)
	}
}

// keyOf prefers the record's own cluster.proc over the table key, which may be
// zero-padded ("01.-1") in what the schedd wrote.
func keyOf(tableKey string, rec backend.JobRecord) string {
	if rec.Ad.ClusterID > 0 {
		return rec.Key()
	}
	return tableKey
}

// dbSessionCommand is htcondordb's CEDAR command number. It is a constant in
// htcondordb's own command package, which is internal to that module's
// daemons; repeating the number is the only way to reach the server from here.
const dbSessionCommand = 74000

// CedarDialer connects to a production htcondordb the way its own clients do:
// an authenticated CEDAR session negotiated from the local HTCondor config.
//
// addr may be a CEDAR sinful string, or the path of the address file the
// daemon publishes (normally $(LOG)/.htcondordb_address).
func CedarDialer(addr string) func(context.Context) (*dbrpc.Client, error) {
	return func(ctx context.Context) (*dbrpc.Client, error) {
		resolved, err := resolveDBAddress(addr)
		if err != nil {
			return nil, err
		}
		cfg, err := config.New()
		if err != nil {
			return nil, fmt.Errorf("reading HTCondor configuration: %w", err)
		}
		sec, err := htcondor.GetSecurityConfig(cfg, dbSessionCommand, "CLIENT")
		if err != nil {
			return nil, fmt.Errorf("building a security configuration: %w", err)
		}
		sec.Command = dbSessionCommand
		if sec.Authentication == security.SecurityOptional {
			// htcondordb always authenticates; leaving this optional
			// makes the negotiation skip it and the server hang up.
			sec.Authentication = security.SecurityPreferred
		}
		dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		cl, err := cedarclient.ConnectAndAuthenticate(dialCtx, resolved, sec)
		if err != nil {
			return nil, fmt.Errorf("authenticating to htcondordb at %s: %w", resolved, err)
		}
		return dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream())), nil
	}
}

// StreamDialer connects to an unauthenticated dbrpc server over TCP. Nothing
// in production speaks this, but it is what an in-process test server offers,
// and having the source exercised end to end without CEDAR is worth the
// twenty lines.
func StreamDialer(addr string) func(context.Context) (*dbrpc.Client, error) {
	return func(ctx context.Context) (*dbrpc.Client, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		return dbrpc.NewClient(dbrpc.NewStreamConn(conn)), nil
	}
}

// resolveDBAddress accepts either an address or the file the daemon writes it
// to, because an operator has one of the two and should not have to care which
// the flag wanted.
func resolveDBAddress(addr string) (string, error) {
	if addr == "" {
		return "", fmt.Errorf("no htcondordb address given")
	}
	if strings.HasPrefix(addr, "<") {
		return addr, nil
	}
	if info, err := os.Stat(addr); err == nil && !info.IsDir() {
		raw, err := os.ReadFile(addr)
		if err != nil {
			return "", fmt.Errorf("reading the htcondordb address file %s: %w", addr, err)
		}
		line := strings.TrimSpace(strings.SplitN(string(raw), "\n", 2)[0])
		if line == "" {
			return "", fmt.Errorf("the htcondordb address file %s is empty", addr)
		}
		return line, nil
	}
	return addr, nil
}
