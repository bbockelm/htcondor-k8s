# htcondor-k8s: two experiments in running Kubernetes workloads on HTCondor.
#
# Three modules, deliberately separate so the facade and the operator do not
# inherit each other's dependency trees:
#   shared/    translation + admission contract (no HTCondor client code)
#   facade/    kubectl-compatible API server in front of a schedd
#   operator/  HTCJob CRD, mutating webhook and sync controller

MODULES := shared facade operator virtualnode
GOENV   := GOWORK=off GOFLAGS=-mod=mod GOPRIVATE=github.com/bbockelm,github.com/PelicanPlatform

.PHONY: all build test test-integration vet tidy fmt clean

all: build test

build:
	@for m in $(MODULES); do echo "== build $$m"; (cd $$m && $(GOENV) go build ./...) || exit 1; done

test:
	@for m in $(MODULES); do echo "== test $$m"; (cd $$m && $(GOENV) go test ./...) || exit 1; done

# Integration tests need a real HTCondor (facade) or envtest binaries
# (operator); both skip themselves when their dependency is absent.
test-integration:
	@for m in $(MODULES); do echo "== integration $$m"; (cd $$m && $(GOENV) go test -tags=integration -timeout 20m ./...) || exit 1; done

vet:
	@for m in $(MODULES); do (cd $$m && $(GOENV) go vet ./...) || exit 1; done

tidy:
	@for m in $(MODULES); do (cd $$m && $(GOENV) go mod tidy) || exit 1; done

fmt:
	@for m in $(MODULES); do (cd $$m && gofmt -w .); done

clean:
	rm -rf bin */bin
