package kubelet

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/bbockelm/htcondor-k8s/virtualnode/api"
)

// SelfSignedCert mints a serving certificate for the kubelet API.
//
// This exists because of an asymmetry worth stating plainly: the API server
// proxies `kubectl logs` to the address in the Node's status over TLS, but it
// does not verify the certificate it is offered unless it was started with
// --kubelet-certificate-authority. In a default cluster, therefore, a
// certificate nobody has ever seen works -- and so would anybody else's, if
// they could get their address into a Node object.
//
// A real deployment should pass CertFile/KeyFile a certificate issued by the
// cluster CA (a CertificateSigningRequest with the kubelet-serving signer is
// the mechanism), and the cluster should set --kubelet-certificate-authority
// so the API server actually checks it. Until both halves are done, the
// transport is encrypted but unauthenticated in the direction that matters.
func SelfSignedCert(hosts ...string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet: generating key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet: generating serial: %w", err)
	}

	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "system:node:" + api.NodeName},
		NotBefore:    time.Now().Add(-time.Hour),
		// One year. The process is expected to be restarted long before
		// then, and a certificate that expires while a cluster is
		// running produces a failure (`kubectl logs` stops working)
		// whose cause is nowhere near its symptom.
		NotAfter:              time.Now().AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	tmpl.DNSNames = append(tmpl.DNSNames, "localhost", api.NodeName)
	if h, err := os.Hostname(); err == nil && h != "" {
		tmpl.DNSNames = append(tmpl.DNSNames, h)
	}
	tmpl.IPAddresses = append(tmpl.IPAddresses, net.ParseIP("127.0.0.1"), net.ParseIP("::1"))
	for _, h := range hosts {
		if h == "" {
			continue
		}
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
			continue
		}
		tmpl.DNSNames = append(tmpl.DNSNames, h)
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet: creating certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("kubelet: marshalling key: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}
