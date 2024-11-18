package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
)

func ParseCAFile(certfile string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	pem, err := os.ReadFile(certfile)
	if err != nil {
		return nil, fmt.Errorf("reading %q: %w", certfile, err)
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("PEM parsing error")
	}
	return pool, nil
}

func ClientAuthClient(serverName string, rootCA *x509.CertPool, clientCert tls.Certificate) (*tls.Config, error) {
	if serverName == "" {
		panic(serverName)
	}
	if rootCA == nil {
		panic(rootCA)
	}
	if clientCert.Certificate == nil || clientCert.PrivateKey == nil {
		panic(clientCert)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      rootCA,
		ServerName:   serverName,
		KeyLogWriter: keylogFromEnv(),
	}
	return tlsConfig, nil
}

func keylogFromEnv() io.Writer {
	var keyLog io.Writer = nil
	if outfile := os.Getenv("ZREPL_KEYLOG_FILE"); outfile != "" {
		fmt.Fprintf(os.Stderr, "writing to key log %s\n", outfile)
		var err error
		keyLog, err = os.OpenFile(outfile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			panic(err)
		}
	}
	return keyLog
}
