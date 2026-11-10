package apiserver

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

// tableAccept is the header kubectl sends for every `get`.
const tableAccept = "application/json;as=Table;v=v1;g=meta.k8s.io,application/json"

func getTable(t *testing.T, url string) *metav1.Table {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", tableAccept)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d", url, resp.StatusCode)
	}
	var table metav1.Table
	if err := json.NewDecoder(resp.Body).Decode(&table); err != nil {
		t.Fatalf("decoding the table: %v", err)
	}
	return &table
}

// Without server-side printing kubectl falls back to a printer that knows
// only NAME and AGE, so `kubectl get pods` cannot tell you whether anything
// is running. The Table is what makes the output look like Kubernetes.
func TestListPodsAsATable(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.CoreV1().Pods("research").Create(ctx, demoPod("alpha", "research"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.Fake.SetStatus("1.0", translate.JobStatusRunning)
	f.sync()

	table := getTable(t, f.HTTP.URL+"/api/v1/namespaces/research/pods")
	if table.Kind != "Table" || table.APIVersion != "meta.k8s.io/v1" {
		t.Fatalf("got %s/%s", table.APIVersion, table.Kind)
	}
	var names []string
	for _, c := range table.ColumnDefinitions {
		names = append(names, c.Name)
	}
	for i, want := range []string{"Name", "Ready", "Status", "Restarts", "Age"} {
		if i >= len(names) || names[i] != want {
			t.Fatalf("column %d is %v, want %s (kubectl prints columns in order)", i, names, want)
		}
	}
	if len(table.Rows) != 1 {
		t.Fatalf("got %d rows", len(table.Rows))
	}
	cells := table.Rows[0].Cells
	if cells[0] != "alpha" || cells[1] != "1/1" || cells[2] != "Running" {
		t.Errorf("row reads %v", cells[:3])
	}
	// The row must carry the object too, or `kubectl get -o yaml` through
	// the table path returns a row with no object in it.
	if len(table.Rows[0].Object.Raw) == 0 {
		t.Error("the row carries no object")
	}
}

func TestListJobsAsATable(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.BatchV1().Jobs("research").Create(ctx, demoJob("sweep", "research", 2), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.Fake.SetStatus("1.0", translate.JobStatusCompleted)
	f.sync()

	table := getTable(t, f.HTTP.URL+"/apis/batch/v1/namespaces/research/jobs")
	if len(table.Rows) != 1 {
		t.Fatalf("got %d rows", len(table.Rows))
	}
	cells := table.Rows[0].Cells
	if cells[0] != "sweep" || cells[1] != "1/2" {
		t.Errorf("row reads %v, want sweep with 1/2 completions", cells[:2])
	}
}

// A client that asked for a Table on the list asks for one on the watch too.
// Sending it plain objects leaves `kubectl get -w` printing the first screen
// and then nothing at all.
func TestWatchEmitsTablesWhenAsked(t *testing.T) {
	f := newFixture(t)
	cs := clientFor(t, f)
	ctx := context.Background()
	if _, err := cs.CoreV1().Pods("research").Create(ctx, demoPod("alpha", "research"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	f.sync()

	req, err := http.NewRequest(http.MethodGet, f.HTTP.URL+"/api/v1/namespaces/research/pods?watch=true", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", tableAccept)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	var event metav1.WatchEvent
	if err := json.NewDecoder(resp.Body).Decode(&event); err != nil {
		t.Fatalf("decoding the first watch event: %v", err)
	}
	if event.Type != "ADDED" {
		t.Fatalf("got %s", event.Type)
	}
	var table metav1.Table
	if err := json.Unmarshal(event.Object.Raw, &table); err != nil {
		t.Fatalf("the watch event does not carry a Table: %v", err)
	}
	if table.Kind != "Table" || len(table.Rows) != 1 || table.Rows[0].Cells[0] != "alpha" {
		t.Fatalf("the event's table is %+v", table)
	}
}
