package main

// Minimal coverage for the orchestrator-side snapshot wiring:
//
//   - decryptSnapshotTar round-trips against the same nonce||ct||tag
//     layout the executor's snapshot.go produces.
//   - GetOrAssign with a resume DEK: fetches the bundle from a fake
//     controlplane, decrypts, and POSTs the plaintext tar to the
//     fake container's /restore.
//   - evictAndSnapshot: pulls the bundle from the fake container's
//     /snapshot and PUTs it to the fake controlplane.
//   - Per-execSessionId serialization: two concurrent GetOrAssigns for
//     the same session land on the same container.
//
// These don't try to be exhaustive — just enough that a refactor
// breaking the wire format or the lock pattern fails fast.

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// gcmSealStdLayout is the same nonce(12) || ct||tag construction the
// executor uses, so we can build a "snapshot" that the orchestrator
// must accept.
func gcmSealStdLayout(t *testing.T, key, plaintext []byte) []byte {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	out := append([]byte{}, nonce...)
	return gcm.Seal(out, nonce, plaintext, nil)
}

func TestDecryptSnapshotTarRoundTrip(t *testing.T) {
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatal(err)
	}
	plain := []byte("hello tar contents — pretend this is a real tar")
	ct := gcmSealStdLayout(t, dek, plain)

	got, err := decryptSnapshotTar(dek, ct)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round trip mismatch:\nwant %q\n got %q", plain, got)
	}

	// Wrong key → AEAD failure, not a panic.
	bad := make([]byte, 32)
	if _, err := decryptSnapshotTar(bad, ct); err == nil {
		t.Fatalf("expected AEAD failure with wrong key, got nil")
	}
}

func TestDecodeBase64Lenient(t *testing.T) {
	want := []byte{0x01, 0x02, 0x03, 0x04, 0xfe, 0xff}
	for name, enc := range map[string]*base64.Encoding{
		"std":     base64.StdEncoding,
		"raw-std": base64.RawStdEncoding,
		"url":     base64.URLEncoding,
		"raw-url": base64.RawURLEncoding,
	} {
		got, err := decodeBase64Lenient(enc.EncodeToString(want))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: round trip mismatch", name)
		}
	}
	if _, err := decodeBase64Lenient("$$$not-base64$$$"); err == nil {
		t.Fatalf("expected error on garbage input")
	}
}

// fakeContainerServer stands in for a code-execution-environment
// container. It exposes /restore (asserts on the tar bytes it sees)
// and /snapshot (returns a canned bundle).
type fakeContainerServer struct {
	*httptest.Server
	mu             sync.Mutex
	gotRestoreTar  []byte // last tar plaintext POSTed to /restore
	gotSnapshotPub string // last pubkey POSTed to /snapshot
	snapshotResp   snapshotBundle
}

func newFakeContainer(snapshotResp snapshotBundle) *fakeContainerServer {
	f := &fakeContainerServer{snapshotResp: snapshotResp}
	mux := http.NewServeMux()
	mux.HandleFunc("/restore", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Tar string `json:"tar"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		raw, err := base64.StdEncoding.DecodeString(body.Tar)
		if err != nil {
			http.Error(w, "bad b64", 400)
			return
		}
		f.mu.Lock()
		f.gotRestoreTar = raw
		f.mu.Unlock()
		w.WriteHeader(200)
		fmt.Fprintln(w, `{"status":"ok"}`)
	})
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		var body snapshotRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		f.mu.Lock()
		f.gotSnapshotPub = body.Pubkey
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(f.snapshotResp)
	})
	f.Server = httptest.NewServer(mux)
	return f
}

// snapshotRequest is duplicated here to avoid importing the executor
// package; same shape as the executor's. We only need the Pubkey field.
type snapshotRequest struct {
	Pubkey string `json:"pubkey"`
}

// fakeContainer returns a *Container whose proxy URL points at the
// httptest server. The proxy + restore + snapshot helpers all build
// "https://" + Domain + path, so we override Domain and inject an
// httpClient with a custom dialer that strips https.
func fakeContainer(f *fakeContainerServer) *Container {
	u, _ := url.Parse(f.URL)
	// Build an http.Client whose transport rewrites https://<Domain>/ to
	// the test server. The simplest way is a transport with a Dial that
	// overrides scheme; even simpler is to use the test server's URL
	// directly as Domain and replace the proxy's "https://" construction
	// — but the manager hardcodes https. So: custom Transport with
	// DialContext that swallows the host + uses TLS-less net.Dial.
	//
	// Easiest path: just lie about Domain (use "x") and use a Transport
	// that always reroutes to the test server.
	transport := &http.Transport{
		DialTLS:             nil,
		DisableKeepAlives:   true,
		MaxIdleConnsPerHost: 1,
	}
	// Override the dial so https://x/path actually reaches the test
	// server. We do this by giving the http.Client a custom RoundTripper.
	rt := rewritingTransport{target: u}
	c := &Container{
		ID:         "fake",
		Name:       "fake-container",
		Domain:     "fake.invalid",
		Status:     "ready",
		httpClient: &http.Client{Transport: rt, Timeout: 5 * time.Second},
	}
	_ = transport // unused; rewritingTransport handles everything
	return c
}

// rewritingTransport rewrites any incoming request to hit the test
// server URL, preserving path/method/body/headers. Lets us point the
// orchestrator's "https://<container.Domain>/restore" at httptest.
type rewritingTransport struct {
	target *url.URL
}

func (rt rewritingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r2 := r.Clone(r.Context())
	r2.URL.Scheme = rt.target.Scheme
	r2.URL.Host = rt.target.Host
	r2.Host = rt.target.Host
	return http.DefaultTransport.RoundTrip(r2)
}

// fakeControlplane stands in for api.tinfoil.sh. Records PUT bodies and
// serves a canned snapshot bundle on GET. It also accepts the warm-pool
// management endpoints (POST /api/containers, GET /api/containers/{id})
// so the manager can be exercised end-to-end without real infra.
type fakeControlplane struct {
	*httptest.Server
	mu          sync.Mutex
	getBundle   *snapshotBundle // returned for GET /api/storage/exec-snapshot/*
	putBundles  map[string]snapshotBundle
}

func newFakeControlplane(getBundle *snapshotBundle) *fakeControlplane {
	cp := &fakeControlplane{getBundle: getBundle, putBundles: map[string]snapshotBundle{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/storage/exec-snapshot/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/storage/exec-snapshot/")
		switch r.Method {
		case "GET":
			cp.mu.Lock()
			b := cp.getBundle
			cp.mu.Unlock()
			if b == nil {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(b)
		case "PUT":
			data, _ := io.ReadAll(r.Body)
			var b snapshotBundle
			if err := json.Unmarshal(data, &b); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			cp.mu.Lock()
			cp.putBundles[id] = b
			cp.mu.Unlock()
			w.WriteHeader(204)
		default:
			w.WriteHeader(405)
		}
	})
	cp.Server = httptest.NewServer(mux)
	return cp
}

func TestRestoreOnAssign(t *testing.T) {
	// Build a "snapshot" the way the executor would: encrypt some plaintext
	// tar bytes with a DEK, then base64 it. Stick it in a fake
	// controlplane bundle.
	plainTar := []byte("PRETEND-TAR-BYTES")
	dek := make([]byte, 32)
	rand.Read(dek)
	ct := gcmSealStdLayout(t, dek, plainTar)
	bundle := &snapshotBundle{
		Ciphertext: base64.StdEncoding.EncodeToString(ct),
		WrappedDEK: "wrapped-not-used-in-this-test",
	}

	cp := newFakeControlplane(bundle)
	defer cp.Close()
	prev := apiBase
	apiBase = cp.URL
	defer func() { apiBase = prev }()

	fc := newFakeContainer(snapshotBundle{})
	defer fc.Close()
	c := fakeContainer(fc)

	m := NewManager(ManagerConfig{AdminAPIKey: "x", PoolSize: 1, MaxContainers: 4, IdleTimeout: time.Hour})
	// Pre-seed the warm pool with our fake container so GetOrAssign
	// pulls it and runs restore.
	m.warmPool = []*Container{c}

	m.RegisterSession("sess-1", "cGstYjY0", base64.StdEncoding.EncodeToString(dek))

	got, errMsg := m.GetOrAssign("sess-1", nil)
	if got == nil {
		t.Fatalf("GetOrAssign failed: %s", errMsg)
	}

	fc.mu.Lock()
	gotTar := fc.gotRestoreTar
	fc.mu.Unlock()
	if !bytes.Equal(gotTar, plainTar) {
		t.Fatalf("restore did not deliver plaintext tar to container.\nwant %q\n got %q", plainTar, gotTar)
	}
	if got.Pubkey != "cGstYjY0" {
		t.Fatalf("pubkey not cached on container: %q", got.Pubkey)
	}
	// Resume DEK should have been consumed.
	m.mu.Lock()
	a := m.attrs["sess-1"]
	m.mu.Unlock()
	if a == nil || a.ResumeDEK != "" {
		t.Fatalf("ResumeDEK should be cleared after use, got %+v", a)
	}
}

func TestEvictAndSnapshot(t *testing.T) {
	// Fake container that returns a canned snapshot bundle.
	fc := newFakeContainer(snapshotBundle{
		Ciphertext: "Y2lwaGVydGV4dA==",
		WrappedDEK: "d3JhcHBlZA==",
	})
	defer fc.Close()
	c := fakeContainer(fc)
	c.Pubkey = "user-pub-b64url"

	cp := newFakeControlplane(nil)
	defer cp.Close()
	prev := apiBase
	apiBase = cp.URL
	defer func() { apiBase = prev }()

	m := NewManager(ManagerConfig{AdminAPIKey: "x"})
	// Override deleteContainer side effect path: we don't have a real
	// container to delete; the fake controlplane just 405s on
	// /api/containers/* which is fine — the call returns and we move on.
	m.evictAndSnapshot("sess-evict", c)

	cp.mu.Lock()
	got, ok := cp.putBundles["sess-evict"]
	cp.mu.Unlock()
	if !ok {
		t.Fatalf("controlplane never received PUT bundle")
	}
	if got.Ciphertext != "Y2lwaGVydGV4dA==" || got.WrappedDEK != "d3JhcHBlZA==" {
		t.Fatalf("PUT bundle mismatch: %+v", got)
	}

	fc.mu.Lock()
	gotPub := fc.gotSnapshotPub
	fc.mu.Unlock()
	if gotPub != "user-pub-b64url" {
		t.Fatalf("container /snapshot got wrong pubkey: %q", gotPub)
	}
}

func TestPerSessionSerialization(t *testing.T) {
	// No resume DEK → restore is skipped, GetOrAssign just pulls from
	// warm pool. Two goroutines racing for the same session should both
	// see the same container.
	cp := newFakeControlplane(nil)
	defer cp.Close()
	prev := apiBase
	apiBase = cp.URL
	defer func() { apiBase = prev }()

	fc := newFakeContainer(snapshotBundle{})
	defer fc.Close()
	c1 := fakeContainer(fc)
	c1.Name = "first"
	c2 := fakeContainer(fc)
	c2.Name = "second"

	m := NewManager(ManagerConfig{AdminAPIKey: "x", PoolSize: 2, MaxContainers: 8, IdleTimeout: time.Hour})
	m.warmPool = []*Container{c1, c2}

	var wg sync.WaitGroup
	results := make([]*Container, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got, _ := m.GetOrAssign("shared-session", nil)
			results[idx] = got
		}(i)
	}
	wg.Wait()

	if results[0] == nil || results[1] == nil {
		t.Fatalf("one or both GetOrAssigns failed: %+v %+v", results[0], results[1])
	}
	if results[0] != results[1] {
		t.Fatalf("expected both racers to get same container, got %s and %s",
			results[0].Name, results[1].Name)
	}
	// Warm pool should still have one container left (the second one).
	m.mu.Lock()
	left := len(m.warmPool)
	m.mu.Unlock()
	if left != 1 {
		t.Fatalf("expected exactly one container left in warm pool, got %d", left)
	}
}
