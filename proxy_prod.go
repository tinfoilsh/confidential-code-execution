//go:build !dev

package main

import (
	"net/http"

	"github.com/tinfoilsh/verifier/client"
)

// Attested TLS client pinned to the enclave's measurement
func executorHTTPClient(domain, repo string) (*http.Client, error) {
	sc := client.NewSecureClient(domain, repo)
	return sc.HTTPClient()
}
