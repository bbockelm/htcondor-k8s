package source

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondor-k8s/facade/internal/backend"
	"github.com/bbockelm/htcondor-k8s/facade/internal/store"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// The htcondordb source is exercised against a real classad/db server rather
// than a stub feed. The whole point of this source is that it understands what
// the storage layer actually emits -- reset, the replay that follows it, and
// the synced marker that ends it -- and a stub would only prove that the
// translation matches whatever the stub's author believed.
//
// The server here speaks plain framed TCP; a production htcondordb speaks
// CEDAR. That difference is confined to the Dial function.
func dbServer(t *testing.T) (addr string, client *dbrpc.Client) {
	t.Helper()
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatalf("opening a catalog: %v", err)
	}
	srv := dbrpc.NewServerCatalog(cat)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	var wg sync.WaitGroup
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = srv.ServeConnOpts(dbrpc.NewStreamConn(conn), dbrpc.ServeOptions{Privileged: true})
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		srv.Close()
		_ = cat.Close()
	})

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	client = dbrpc.NewClient(dbrpc.NewStreamConn(conn))
	t.Cleanup(func() { _ = client.Close() })
	if err := client.CreateTable(context.Background(), ScheddJobsTable); err != nil {
		t.Fatalf("creating the %s table: %v", ScheddJobsTable, err)
	}
	return ln.Addr().String(), client
}

// putJob writes one job ad the way htcondordb's schedd mirror would: keyed
// "cluster.proc", carrying the facade's identity attributes.
func putJob(t *testing.T, c *dbrpc.Client, cluster, proc, status int, name string) {
	t.Helper()
	key := fmt.Sprintf("%d.%d", cluster, proc)
	// The mirror stores ads in HTCondor's old (line-oriented) format,
	// which is what job_queue.log itself holds.
	ad := fmt.Sprintf("ClusterId = %d\nProcId = %d\nJobStatus = %d\nQDate = 1700000000\nOwner = \"alice\"\n%s = \"%s\"\n%s = \"default\"\n",
		cluster, proc, status, translate.AttrPodName, name, translate.AttrPodNamespace)
	tx, err := c.BeginTable(context.Background(), ScheddJobsTable)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.NewClassAd(context.Background(), key, ad); err != nil {
		t.Fatalf("writing %s: %v", key, err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func deleteJob(t *testing.T, c *dbrpc.Client, key string) {
	t.Helper()
	tx, err := c.BeginTable(context.Background(), ScheddJobsTable)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.DestroyClassAd(context.Background(), key); err != nil {
		t.Fatalf("destroying %s: %v", key, err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// waitFor polls a predicate instead of sleeping for a fixed interval, so the
// test is as fast as the feed is and does not become flaky on a loaded box.
func waitFor(t *testing.T, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestHTCondorDBSourceReplaysThenStreams(t *testing.T) {
	addr, c := dbServer(t)
	// A row that exists before the watch starts must arrive in the initial
	// replay, not be missed because the stream only reports changes.
	putJob(t, c, 1, 0, translate.JobStatusIdle, "before")

	st := store.New(100)
	src := &HTCondorDB{Dial: StreamDialer(addr), Backoff: 50 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = src.Run(ctx, st) }()

	waitFor(t, "the pre-existing row to be replayed", func() bool {
		_, _, ok := st.Item("1.0")
		return ok
	})

	w, err := st.Watch(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	putJob(t, c, 2, 0, translate.JobStatusIdle, "after")
	select {
	case ev := <-w.Events():
		if ev.Type != store.Added || ev.Record.Ad.Identity.Name != "after" {
			t.Fatalf("got %s for %q", ev.Type, ev.Record.Ad.Identity.Name)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a live upsert never reached the store")
	}

	// An update to an existing row is a MODIFIED, and the record the store
	// holds is the new one.
	putJob(t, c, 2, 0, translate.JobStatusRunning, "after")
	waitFor(t, "the status change", func() bool {
		rec, _, ok := st.Item("2.0")
		return ok && rec.Ad.JobStatus == translate.JobStatusRunning
	})

	deleteJob(t, c, "2.0")
	waitFor(t, "the deletion", func() bool {
		_, _, ok := st.Item("2.0")
		return !ok
	})
}

func TestHTCondorDBSourceIgnoresJobsThatAreNotOurs(t *testing.T) {
	addr, c := dbServer(t)
	st := store.New(100)
	src := &HTCondorDB{Dial: StreamDialer(addr), Backoff: 50 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = src.Run(ctx, st) }()

	// An ordinary condor_submit has no K8s attributes and must never show
	// up as a Pod; a facade that reported every job in the queue would be
	// claiming to manage work it cannot describe.
	tx, err := c.BeginTable(context.Background(), ScheddJobsTable)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.NewClassAd(context.Background(), "9.0",
		"ClusterId = 9\nProcId = 0\nJobStatus = 1\nOwner = \"bob\"\n"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	putJob(t, c, 10, 0, translate.JobStatusIdle, "ours")

	waitFor(t, "our job to arrive", func() bool {
		_, _, ok := st.Item("10.0")
		return ok
	})
	if _, _, ok := st.Item("9.0"); ok {
		t.Error("a job with no K8sPodName was surfaced as a Pod")
	}
	recs, _ := st.Snapshot(nil)
	if len(recs) != 1 {
		t.Fatalf("the store holds %d records, want 1: %+v", len(recs), recs)
	}
	var _ backend.JobRecord = recs[0]
}
