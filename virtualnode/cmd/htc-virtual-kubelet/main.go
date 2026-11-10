// Command htc-virtual-kubelet runs the Pod-interception approach: a mutating
// webhook that points a translatable Job's pods at this scheduler, a virtual
// node to bind those pods to, a controller that turns each one into an
// HTCondor job, and the kubelet HTTP API that makes `kubectl logs` work.
//
// What it deliberately does not do is touch the Job. The built-in Job
// controller runs untouched and derives job.status from the pod statuses this
// process writes, which is the whole reason this approach exists.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/htcondor-k8s/shared/translate"
	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/condor"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/kubelet"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/node"
	"github.com/bbockelm/htcondor-k8s/virtualnode/internal/podctl"
	htcwebhook "github.com/bbockelm/htcondor-k8s/virtualnode/internal/webhook"
)

var scheme = runtime.NewScheme()

func init() {
	must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr   string
		probeAddr     string
		webhookPort   int
		certDir       string
		leaderElect   bool
		nodeName      string
		kubeletAddr   string
		kubeletPort   int
		kubeletCert   string
		kubeletKey    string
		kubeletCA     string
		nodeOS        string
		nodeArch      string
		scheddName    string
		scheddAddr    string
		spoolRoot     string
		pollInterval  time.Duration
		reapInterval  time.Duration
		reapGrace     time.Duration
		maxCPU        int64
		maxMemBytes   int64
		gpuResources  string
		nodeSelectors string
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "metrics endpoint; 0 disables it")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe endpoint")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "webhook server port")
	flag.StringVar(&certDir, "webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "directory holding tls.crt and tls.key")
	flag.BoolVar(&leaderElect, "leader-elect", false, "run leader election")
	flag.StringVar(&nodeName, "node-name", api.NodeName, "name of the Node object to register")
	flag.StringVar(&kubeletAddr, "kubelet-address", "", "address the API server dials for logs; published in the Node's status")
	flag.IntVar(&kubeletPort, "kubelet-port", 10250, "port the kubelet API listens on")
	flag.StringVar(&kubeletCert, "kubelet-tls-cert", "", "serving certificate; a self-signed one is generated when empty")
	flag.StringVar(&kubeletKey, "kubelet-tls-key", "", "serving key")
	flag.StringVar(&kubeletCA, "kubelet-client-ca", "", "require a client certificate signed by this CA")
	flag.StringVar(&nodeOS, "node-os", "", "OS reported by the virtual node; this is a claim about the execute nodes, not about this process")
	flag.StringVar(&nodeArch, "node-arch", "", "architecture reported by the virtual node")
	flag.StringVar(&scheddName, "schedd-name", "", "schedd name")
	flag.StringVar(&scheddAddr, "schedd-address", "", "schedd sinful string or host:port")
	flag.StringVar(&spoolRoot, "spool-root", "", "directory shared with the access point for per-pod initialdir")
	flag.DurationVar(&pollInterval, "poll-interval", 15*time.Second, "how often a live job is re-read from the queue")
	flag.DurationVar(&reapInterval, "reap-interval", 5*time.Minute, "how often to sweep for jobs whose Pod is gone")
	flag.DurationVar(&reapGrace, "reap-grace", 10*time.Minute, "how old a job must be before the sweep may remove it")
	flag.Int64Var(&maxCPU, "pool-max-cpu", 0, "largest CPU count a single pod may request; 0 for no local bound")
	flag.Int64Var(&maxMemBytes, "pool-max-memory-bytes", 0, "largest memory request; 0 for no local bound")
	flag.StringVar(&gpuResources, "pool-gpu-resources", "", "comma-separated extended resource names that map onto request_gpus")
	flag.StringVar(&nodeSelectors, "pool-node-selectors", "", "comma-separated key=MachineAdAttribute pairs that nodeSelector keys may use")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	if scheddAddr == "" {
		setupLog.Error(fmt.Errorf("-schedd-address is required"), "no access point configured")
		os.Exit(1)
	}
	// The address the API server dials is not discoverable from in here: it
	// is whatever route the control plane has back to this process, which
	// in a cluster is the Service or Pod IP. Guessing it wrong fails only
	// at `kubectl logs` time, long after startup, so it is required.
	if kubeletAddr == "" {
		setupLog.Error(fmt.Errorf("-kubelet-address is required"),
			"the API server reads this from the Node's status to serve logs; it cannot be discovered")
		os.Exit(1)
	}

	caps := translate.DefaultCapabilities()
	caps.MaxCPU = maxCPU
	caps.MaxMemoryBytes = maxMemBytes
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
		LeaderElectionID:       "htc-virtual-kubelet.htcondor.org",
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
		Handler: &htcwebhook.JobStamper{
			Caps:    caps,
			Decoder: admission.NewDecoder(scheme),
			Log:     ctrl.Log.WithName("webhook").WithName("job"),
		},
	})

	queue := condor.NewQueue(htcondor.NewSchedd(scheddName, scheddAddr))

	if err := (&podctl.PodReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("htc-virtual-kubelet"),
		Queue:        queue,
		Caps:         caps,
		NodeName:     nodeName,
		ScheddName:   scheddName,
		SpoolRoot:    spoolRoot,
		PollInterval: pollInterval,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up the pod controller")
		os.Exit(1)
	}

	if err := (&podctl.Reaper{
		Client:   mgr.GetClient(),
		Queue:    queue,
		NodeName: nodeName,
		Interval: reapInterval,
		Grace:    reapGrace,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up the orphan sweep")
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "building a typed client for the node lifecycle")
		os.Exit(1)
	}

	nodeCtl, err := node.New(node.Config{
		Client:   clientset,
		NodeName: nodeName,
		Address:  kubeletAddr,
		Port:     int32(kubeletPort),
		OS:       nodeOS,
		Arch:     nodeArch,
		Log:      ctrl.Log.WithName("node"),
	})
	if err != nil {
		setupLog.Error(err, "building the virtual node")
		os.Exit(1)
	}

	logServer, err := kubelet.New(kubelet.Config{
		Pods:         kubelet.PodsFromClient(mgr.GetClient()),
		Peek:         queue,
		Address:      kubeletAddr,
		Port:         int32(kubeletPort),
		CertFile:     kubeletCert,
		KeyFile:      kubeletKey,
		ClientCAFile: kubeletCA,
		Log:          ctrl.Log.WithName("kubelet"),
	})
	if err != nil {
		setupLog.Error(err, "building the kubelet API")
		os.Exit(1)
	}

	// The node and the log server are NOT leader-elected, unlike the two
	// controllers above. Every replica has to answer logs for the pods it
	// can reach, and the Node object has to stay fresh whether or not this
	// replica holds the lease -- a standby that stopped heartbeating would
	// take the node NotReady and, five minutes later, have the taint
	// manager delete every bound pod.
	must(mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		if err := nodeCtl.Register(ctx); err != nil {
			return fmt.Errorf("registering the virtual node: %w", err)
		}
		return nodeCtl.Run(ctx)
	})))
	must(mgr.Add(manager.RunnableFunc(logServer.Run)))

	must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	must(mgr.AddReadyzCheck("readyz", healthz.Ping))

	setupLog.Info("starting",
		"schedd", scheddAddr, "node", nodeName,
		"kubelet", fmt.Sprintf("%s:%d", kubeletAddr, kubeletPort),
		"webhookPort", webhookPort)
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
