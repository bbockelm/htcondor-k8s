// Command manager runs the HTCJob operator: the mutating webhook that claims
// batch/v1 Jobs, and the two controllers that turn claimed Jobs into HTCondor
// clusters and carry their status back.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/htcondor-k8s/operator/api/v1alpha1"
	"github.com/bbockelm/htcondor-k8s/operator/internal/condor"
	"github.com/bbockelm/htcondor-k8s/operator/internal/controller"
	htcwebhook "github.com/bbockelm/htcondor-k8s/operator/internal/webhook"
	"github.com/bbockelm/htcondor-k8s/shared/translate"
)

var scheme = runtime.NewScheme()

func init() {
	must(clientgoscheme.AddToScheme(scheme))
	must(v1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr    string
		probeAddr      string
		webhookPort    int
		certDir        string
		leaderElect    bool
		scheddName     string
		scheddAddr     string
		spoolRoot      string
		pollInterval   time.Duration
		mirrorStatus   bool
		useManagedBy   bool
		suspendBelt    bool
		maxCPU         int64
		maxMemoryBytes int64
		gpuResources   string
		nodeSelectors  string
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "metrics endpoint; 0 disables it")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe endpoint")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "webhook server port")
	flag.StringVar(&certDir, "webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "directory holding tls.crt and tls.key")
	flag.BoolVar(&leaderElect, "leader-elect", false, "run leader election")
	flag.StringVar(&scheddName, "schedd-name", "", "schedd name")
	flag.StringVar(&scheddAddr, "schedd-address", "", "schedd sinful string or host:port")
	flag.StringVar(&spoolRoot, "spool-root", "", "directory shared with the access point for per-job initialdir")
	flag.DurationVar(&pollInterval, "poll-interval", 15*time.Second, "how often a live HTCJob is re-read from the queue")
	flag.BoolVar(&useManagedBy, "use-managed-by", true,
		"set .spec.managedBy on claimed Jobs so the built-in Job controller skips them "+
			"(needs Kubernetes 1.32 or newer; see api/v1alpha1.ManagedByName)")
	flag.BoolVar(&suspendBelt, "suspend-fallback", false,
		"also force .spec.suspend on claimed Jobs, for a cluster that might not honor managedBy. "+
			"Implied when -use-managed-by is false")
	flag.BoolVar(&mirrorStatus, "mirror-job-status", true,
		"copy HTCJob status onto the owning batch/v1 Job. Safe under -use-managed-by, which "+
			"makes this operator the only writer; without it the built-in Job controller "+
			"contests and wins")
	flag.Int64Var(&maxCPU, "pool-max-cpu", 0, "largest CPU count a single job may request; 0 for no local bound")
	flag.Int64Var(&maxMemoryBytes, "pool-max-memory-bytes", 0, "largest memory request; 0 for no local bound")
	flag.StringVar(&gpuResources, "pool-gpu-resources", "",
		"comma-separated extended resource names that map onto request_gpus")
	flag.StringVar(&nodeSelectors, "pool-node-selectors", "",
		"comma-separated key=MachineAdAttribute pairs that nodeSelector keys may use")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	caps := translate.DefaultCapabilities()
	caps.MaxCPU = maxCPU
	caps.MaxMemoryBytes = maxMemoryBytes
	caps.GPUResourceNames = splitList(gpuResources)
	for _, pair := range splitList(nodeSelectors) {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			setupLog.Error(fmt.Errorf("bad -pool-node-selectors entry %q", pair), "expected key=Attribute")
			os.Exit(1)
		}
		caps.NodeSelectorAttrs[k] = v
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "htcjob-operator.htcondor.org",
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    webhookPort,
			CertDir: certDir,
		}),
	})
	if err != nil {
		setupLog.Error(err, "starting manager")
		os.Exit(1)
	}

	mgr.GetWebhookServer().Register(htcwebhook.Path, &admission.Webhook{
		Handler: &htcwebhook.JobClaimer{
			UseManagedBy:    useManagedBy,
			SuspendFallback: suspendBelt || !useManagedBy,
			Caps:            caps,
			Decoder:         admission.NewDecoder(scheme),
			Log:             ctrl.Log.WithName("webhook").WithName("job"),
		},
	})

	if err := (&controller.JobReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("htcjob-operator"),
		Caps:         caps,
		MirrorStatus: mirrorStatus,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up the Job controller")
		os.Exit(1)
	}

	// The schedd is resolved once at startup. A schedd that moves gets a new
	// sinful string, which this will not notice -- restarting the operator
	// is the recovery, and the submission idempotency means no job is run
	// twice because of it.
	if scheddAddr == "" {
		setupLog.Error(fmt.Errorf("-schedd-address is required"), "no access point configured")
		os.Exit(1)
	}
	queue := condor.NewQueue(htcondor.NewSchedd(scheddName, scheddAddr))

	if err := (&controller.HTCJobReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("htcjob-operator"),
		Queue:        queue,
		Caps:         caps,
		SpoolRoot:    spoolRoot,
		PollInterval: pollInterval,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up the HTCJob controller")
		os.Exit(1)
	}

	must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	must(mgr.AddReadyzCheck("readyz", healthz.Ping))

	// Registering batchv1 explicitly keeps the import honest: the webhook
	// decodes Jobs and the controller watches them.
	_ = batchv1.AddToScheme

	setupLog.Info("starting", "schedd", scheddAddr, "webhookPort", webhookPort)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
