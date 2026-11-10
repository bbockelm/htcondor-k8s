package k3s_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
)

// A self-signed kubelet certificate does not work here, and finding that out
// is itself a result worth keeping.
//
// kube-apiserver verifies the kubelet's serving certificate whenever it was
// started with --kubelet-certificate-authority, and **k3s sets that by
// default** (so do most managed distributions). The failure is a 502 Bad
// Gateway from the proxy, which reads like the log server is down rather than
// like a certificate was rejected -- there is nothing in the message about
// TLS at all.
//
// So a real deployment of this virtual kubelet needs a serving certificate
// the cluster's API server trusts. In production that means a
// CertificateSigningRequest with signerName kubernetes.io/kubelet-serving,
// which the cluster signs with the same CA. Here, where the whole cluster is
// a container we started, borrowing the CA directly is the equivalent and
// much shorter.
func issueServingCert(ctx context.Context, container, dir string, hosts ...string) (certFile, keyFile string, err error) {
	caCertPEM, err := catFromContainer(ctx, container, "/var/lib/rancher/k3s/server/tls/server-ca.crt")
	if err != nil {
		return "", "", err
	}
	caKeyPEM, err := catFromContainer(ctx, container, "/var/lib/rancher/k3s/server/tls/server-ca.key")
	if err != nil {
		return "", "", err
	}

	caCert, err := parseCert(caCertPEM)
	if err != nil {
		return "", "", fmt.Errorf("parsing the cluster CA: %w", err)
	}
	caKey, err := parseKey(caKeyPEM)
	if err != nil {
		return "", "", fmt.Errorf("parsing the cluster CA key: %w", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", "", err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		// The API server authorizes the kubelet it dials by the
		// system:node identity, the same as a real one.
		Subject:     pkix.Name{CommonName: "system:node:" + api.NodeName, Organization: []string{"system:nodes"}},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    hosts,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return "", "", fmt.Errorf("signing the serving certificate: %w", err)
	}

	certFile = filepath.Join(dir, "kubelet.crt")
	keyFile = filepath.Join(dir, "kubelet.key")
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", "", err
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return "", "", err
	}
	return certFile, keyFile, nil
}

func catFromContainer(ctx context.Context, container, path string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, "docker", "exec", container, "cat", path).Output()
	if err != nil {
		return nil, fmt.Errorf("reading %s from the cluster: %w", path, err)
	}
	return out, nil
}

func parseCert(data []byte) (*x509.Certificate, error) {
	blk, _ := pem.Decode(data)
	if blk == nil {
		return nil, fmt.Errorf("no PEM block")
	}
	return x509.ParseCertificate(blk.Bytes)
}

func parseKey(data []byte) (any, error) {
	blk, _ := pem.Decode(data)
	if blk == nil {
		return nil, fmt.Errorf("no PEM block")
	}
	if k, err := x509.ParseECPrivateKey(blk.Bytes); err == nil {
		return k, nil
	}
	if k, err := x509.ParsePKCS8PrivateKey(blk.Bytes); err == nil {
		return k, nil
	}
	return x509.ParsePKCS1PrivateKey(blk.Bytes)
}
