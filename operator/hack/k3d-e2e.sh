#!/usr/bin/env bash
#
# Bring up a k3d cluster, install the CRD and the webhook, run the operator
# against it, and check the two things envtest structurally cannot:
#
#   1. The built-in Job controller is present. A claimed Job is suspended, so
#      it must create zero Pods -- and if -mirror-job-status is on, the Job's
#      .status is contested by kube-controller-manager. That fight is only
#      observable here.
#   2. The garbage collector is present, so deleting a Job really does delete
#      the HTCJob it owns, whose finalizer really does remove the cluster.
#
# It skips, rather than fails, when Docker is not running: the value of the
# suite is in envtest, and a developer without a container runtime should not
# be blocked.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "${HERE}/.." && pwd)"
CLUSTER="${K3D_CLUSTER:-htcondor-k8s-e2e}"

skip() { echo "SKIP: $*" >&2; exit 0; }

command -v docker >/dev/null 2>&1 || skip "docker is not installed"
docker info >/dev/null 2>&1 || skip "the Docker daemon is not running"
command -v k3d >/dev/null 2>&1 || skip "k3d is not installed (brew install k3d)"
command -v kubectl >/dev/null 2>&1 || skip "kubectl is not installed"

cleanup() {
	if [[ "${K3D_KEEP:-}" == "1" ]]; then
		echo "keeping cluster ${CLUSTER} (K3D_KEEP=1)"
		return
	fi
	k3d cluster delete "${CLUSTER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> creating k3d cluster ${CLUSTER}"
k3d cluster create "${CLUSTER}" --wait --timeout 180s
kubectl config use-context "k3d-${CLUSTER}"

echo "==> installing the CRD"
kubectl apply -f "${OPERATOR_DIR}/config/crd/"
kubectl wait --for=condition=Established --timeout=60s crd/htcjobs.htcondor.org

# The webhook must be reachable from the API server inside the cluster, and
# must present a certificate the API server trusts. Running the operator in
# the cluster is the realistic shape; building and importing an image here
# would make this script a build system, so it is deliberately left to the
# caller.
if [[ -z "${OPERATOR_IMAGE:-}" ]]; then
	cat >&2 <<'EOT'
SKIP: set OPERATOR_IMAGE to a built operator image to run the webhook and
      controller halves of this test. Without it only the CRD half runs,
      which envtest already covers.
EOT
	echo "==> CRD-only check: a Job-shaped field must be refused"
	if kubectl apply --validate=strict -f - <<'EOT' 2>/dev/null
apiVersion: htcondor.org/v1alpha1
kind: HTCJob
metadata: { name: should-fail, namespace: default }
spec:
  template:
    spec:
      container: { name: w, image: busybox, command: ["/bin/true"] }
      tolerations: [{ key: gpu }]
EOT
	then
		echo "FAIL: the API server accepted a toleration on an HTCJob" >&2
		exit 1
	fi
	echo "PASS: the CRD refused an unsupported field"
	exit 0
fi

echo "==> deploying the operator from ${OPERATOR_IMAGE}"
k3d image import "${OPERATOR_IMAGE}" -c "${CLUSTER}"
kubectl apply -f "${OPERATOR_DIR}/config/manager/namespace.yaml"
kubectl apply -f "${OPERATOR_DIR}/config/rbac/"
kubectl apply -f "${OPERATOR_DIR}/config/manager/"
kubectl -n htcondor-system set image deployment/htcondor-operator "manager=${OPERATOR_IMAGE}"
kubectl -n htcondor-system rollout status deployment/htcondor-operator --timeout=180s
kubectl apply -f "${OPERATOR_DIR}/config/webhook/"

echo "==> a claimed Job must create no Pods"
kubectl apply -f "${OPERATOR_DIR}/config/samples/job.yaml"
for _ in $(seq 1 60); do
	claimed=$(kubectl get job claimable -o jsonpath='{.metadata.annotations.htcondor\.org/claimed}' 2>/dev/null || true)
	[[ -n "${claimed}" ]] && break
	sleep 1
done
[[ -n "${claimed:-}" ]] || { echo "FAIL: the webhook did not claim the Job" >&2; exit 1; }

sleep 10 # let the built-in Job controller have its chance to make Pods
pods=$(kubectl get pods -l batch.kubernetes.io/job-name=claimable --no-headers 2>/dev/null | wc -l | tr -d ' ')
[[ "${pods}" == "0" ]] || { echo "FAIL: a claimed Job produced ${pods} Pods" >&2; exit 1; }
echo "PASS: no Pods were created for the claimed Job"

echo "==> deleting the Job must cascade to the HTCJob"
kubectl delete job claimable --wait=true
for _ in $(seq 1 60); do
	kubectl get htcjob claimable >/dev/null 2>&1 || { echo "PASS: the HTCJob was garbage collected"; exit 0; }
	sleep 1
done
echo "FAIL: the HTCJob outlived its owning Job" >&2
exit 1
