package main

// Coverage for the orchestrator-side snapshot wiring against tinfoil-buckets:
//
//   - decodeBase64Lenient round-trips std/url/raw variants.
//   - GetOrAssign with an exec key: GETs the plaintext tar from a fake
//     buckets server (which echoes back what was previously PUT under
//     that key) and POSTs it to the fake container's /restore.
//   - evictAndSnapshot: pulls a plaintext tar from the fake container's
//     /snapshot and PUTs it to the fake buckets server.
//   - Per-accessToken serialization: two concurrent GetOrAssigns for
//     the same session land on the same container.

import (
	"bytes"
	"context"
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

func TestURLBase64ToStd(t *testing.T) {
	want := []byte{0xfa, 0xfb, 0xfc, 0xfd}
	urlForm := base64.RawURLEncoding.EncodeToString(want)
	std, err := urlBase64ToStd(urlForm)
	if err != nil {
		t.Fatalf("urlBase64ToStd: %v", err)
	}
	got, err := base64.StdEncoding.DecodeString(std)
	if err != nil {
		t.Fatalf("std-decode: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round trip mismatch: %x vs %x", got, want)
	}
}

// fakeContainerServer stands in for a code-execution-environment
// container. /restore records what plaintext tar arrives; /snapshot
// streams a canned plaintext tar with the success trailer; /health
// returns whatever healthStatus is set to.
type fakeContainerServer struct {
	*httptest.Server
	mu             sync.Mutex
	gotRestoreTar  []byte
	snapshotTarB64 string
	// snapshotOmitTrailer mirrors a truncated stream: handler sends
	// the JSON body but never sets X-Snapshot-Status=ok.
	snapshotOmitTrailer bool
	// healthStatus is the status code returned by /health. 0 means 200.
	healthStatus int
}

func newFakeContainer(snapshotTar []byte) *fakeContainerServer {
	f := &fakeContainerServer{
		snapshotTarB64: base64.StdEncoding.EncodeToString(snapshotTar),
	}
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
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Trailer", snapshotTrailer)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"tar": f.snapshotTarB64})
		f.mu.Lock()
		omit := f.snapshotOmitTrailer
		f.mu.Unlock()
		if !omit {
			w.Header().Set(snapshotTrailer, "ok")
		}
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		s := f.healthStatus
		f.mu.Unlock()
		if s == 0 {
			s = http.StatusOK
		}
		w.WriteHeader(s)
	})
	f.Server = httptest.NewServer(mux)
	return f
}

// fakeContainer returns a *Container whose proxy URL points at the
// httptest server.
func fakeContainer(f *fakeContainerServer) *Container {
	u, _ := url.Parse(f.URL)
	rt := rewritingTransport{target: u}
	return &Container{
		ID:         "fake",
		Name:       "fake-container",
		Domain:     "fake.invalid",
		Status:     "ready",
		httpClient: &http.Client{Transport: rt, Timeout: 5 * time.Second},
	}
}

// rewritingTransport reroutes any request to the test server, regardless
// of the URL it was built with. Lets us point "https://<container.Domain>"
// at httptest without TLS.
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

// fakeBuckets stands in for buckets.tinfoil.sh. Stores plaintext blobs
// per (lookupKey, key) pair — we don't actually encrypt; we just verify
// PUT/GET symmetry under the same key. Wrong-key GETs return 403,
// matching real buckets behavior.
type fakeBuckets struct {
	*httptest.Server
	mu                   sync.Mutex
	putAttempts          int
	putFailuresRemaining int
	// stored maps lookupKey → (key, plaintext). Single-key per item is
	// fine for these tests.
	stored map[string]storedBlob
}

type storedBlob struct {
	key       string // base64 std
	plaintext []byte
}

func newFakeBuckets() *fakeBuckets {
	b := &fakeBuckets{stored: map[string]storedBlob{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/items/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/items/")
		switch r.Method {
		case "GET":
			key := r.Header.Get("X-Encryption-Key")
			b.mu.Lock()
			blob, ok := b.stored[id]
			b.mu.Unlock()
			if !ok {
				http.Error(w, "lookup_key not found", 404)
				return
			}
			if blob.key != key {
				http.Error(w, "wrong key", 403)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"value": base64.StdEncoding.EncodeToString(blob.plaintext),
			})
		case "PUT":
			data, _ := io.ReadAll(r.Body)
			var body struct {
				Value          string   `json:"value"`
				EncryptionKeys []string `json:"encryption_keys"`
			}
			if err := json.Unmarshal(data, &body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			b.mu.Lock()
			b.putAttempts++
			if b.putFailuresRemaining > 0 {
				b.putFailuresRemaining--
				b.mu.Unlock()
				http.Error(w, "simulated transient failure", 500)
				return
			}
			pt, err := base64.StdEncoding.DecodeString(body.Value)
			if err != nil {
				b.mu.Unlock()
				http.Error(w, "bad b64", 400)
				return
			}
			if len(body.EncryptionKeys) == 0 {
				b.mu.Unlock()
				http.Error(w, "no key", 400)
				return
			}
			b.stored[id] = storedBlob{key: body.EncryptionKeys[0], plaintext: pt}
			b.mu.Unlock()
			w.WriteHeader(200)
			fmt.Fprintln(w, `{}`)
		default:
			w.WriteHeader(405)
		}
	})
	b.Server = httptest.NewServer(mux)
	return b
}

func TestRestoreOnAssign(t *testing.T) {
	plainTar := []byte("PRETEND-TAR-BYTES")

	bk := newFakeBuckets()
	defer bk.Close()
	// Pre-load the bucket with a snapshot for sess-1, encrypted under
	// the user's key. (fake bucket stores std-base64 of the key.)
	keyRaw := bytes.Repeat([]byte{0xab}, 32)
	keyStd := base64.StdEncoding.EncodeToString(keyRaw)
	keyURL := base64.RawURLEncoding.EncodeToString(keyRaw)
	bk.mu.Lock()
	bk.stored["sess-1"] = storedBlob{key: keyStd, plaintext: plainTar}
	bk.mu.Unlock()

	fc := newFakeContainer(nil)
	defer fc.Close()
	c := fakeContainer(fc)

	m := NewManager(ManagerConfig{AdminAPIKey: "x", BucketsBase: bk.URL, PoolSize: 1, MaxContainers: 4, MaxConcurrentSnapshots: 4, IdleTimeout: time.Hour})
	m.warmPool = []*Container{c}

	// Webapp sends url-safe base64; orchestrator should normalize.
	ctx := WithCodeExecutionEncryptionKey(context.Background(), keyURL)
	ctx = WithBearer(ctx, "tk_test")

	got, errMsg := m.GetOrAssign(ctx, "sess-1", nil)
	if got == nil {
		t.Fatalf("GetOrAssign failed: %s", errMsg)
	}

	fc.mu.Lock()
	gotTar := fc.gotRestoreTar
	fc.mu.Unlock()
	if !bytes.Equal(gotTar, plainTar) {
		t.Fatalf("restore did not deliver plaintext tar to container.\nwant %q\n got %q", plainTar, gotTar)
	}
	if got.CodeExecutionEncryptionKey != keyURL {
		t.Fatalf("code execution encryption key not cached on container: %q", got.CodeExecutionEncryptionKey)
	}
}

func TestRestoreOnAssignNoSnapshot(t *testing.T) {
	// No snapshot in buckets → 404 → fresh container, /restore not called.
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeContainer(nil)
	defer fc.Close()
	c := fakeContainer(fc)

	m := NewManager(ManagerConfig{AdminAPIKey: "x", BucketsBase: bk.URL, PoolSize: 1, MaxContainers: 4, MaxConcurrentSnapshots: 4, IdleTimeout: time.Hour})
	m.warmPool = []*Container{c}

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, 32))
	ctx := WithCodeExecutionEncryptionKey(context.Background(), keyURL)
	ctx = WithBearer(ctx, "tk_test")

	if got, errMsg := m.GetOrAssign(ctx, "sess-fresh", nil); got == nil {
		t.Fatalf("GetOrAssign failed: %s", errMsg)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.gotRestoreTar != nil {
		t.Fatalf("expected no restore call for missing snapshot, got tar of %d bytes", len(fc.gotRestoreTar))
	}
}

func TestEvictAndSnapshot(t *testing.T) {
	plainTar := []byte("workspace-tar-bytes")

	fc := newFakeContainer(plainTar)
	defer fc.Close()
	c := fakeContainer(fc)

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x11}, 32))
	keyStd, _ := urlBase64ToStd(keyURL)
	c.CodeExecutionEncryptionKey = keyURL
	c.Bearer = "tk_test"

	bk := newFakeBuckets()
	defer bk.Close()
	m := NewManager(ManagerConfig{AdminAPIKey: "x", BucketsBase: bk.URL, MaxConcurrentSnapshots: 4})
	m.evictAndSnapshot(context.Background(), "sess-evict", c)

	bk.mu.Lock()
	stored, ok := bk.stored["sess-evict"]
	bk.mu.Unlock()
	if !ok {
		t.Fatalf("buckets never received PUT")
	}
	if !bytes.Equal(stored.plaintext, plainTar) {
		t.Fatalf("PUT plaintext mismatch: got %q want %q", stored.plaintext, plainTar)
	}
	if stored.key != keyStd {
		t.Fatalf("PUT key not normalized to std-base64: got %q want %q", stored.key, keyStd)
	}
}

func TestEvictAndSnapshotPutRetry(t *testing.T) {
	// First PUT fails 500; the bounded retry succeeds.
	fc := newFakeContainer([]byte("tar-bytes"))
	defer fc.Close()
	c := fakeContainer(fc)
	c.CodeExecutionEncryptionKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x22}, 32))
	c.Bearer = "tk_test"

	bk := newFakeBuckets()
	bk.putFailuresRemaining = 1
	defer bk.Close()
	prevDelay := snapshotPutRetryDelay
	snapshotPutRetryDelay = 0
	defer func() { snapshotPutRetryDelay = prevDelay }()

	m := NewManager(ManagerConfig{AdminAPIKey: "x", BucketsBase: bk.URL, MaxConcurrentSnapshots: 4})
	m.evictAndSnapshot(context.Background(), "sess-retry", c)

	bk.mu.Lock()
	attempts := bk.putAttempts
	_, stored := bk.stored["sess-retry"]
	bk.mu.Unlock()
	if attempts != 2 {
		t.Fatalf("expected exactly 2 PUT attempts (1 fail + 1 retry), got %d", attempts)
	}
	if !stored {
		t.Fatalf("blob not stored after retry")
	}
}

func TestFinishSnapshotsActiveSessions(t *testing.T) {
	// Shutdown must take the same snapshot-then-delete path as idle eviction
	// for any session with a cached key+bearer. Anything that doesn't get
	// snapshotted on shutdown is workspace state lost on the next deploy.
	plainTar := []byte("session-tar-bytes")
	fc := newFakeContainer(plainTar)
	defer fc.Close()
	c := fakeContainer(fc)
	c.CodeExecutionEncryptionKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x33}, 32))
	c.Bearer = "tk_test"

	bk := newFakeBuckets()
	defer bk.Close()
	m := NewManager(ManagerConfig{AdminAPIKey: "x", BucketsBase: bk.URL, MaxConcurrentSnapshots: 4, ShutdownDeadline: 5 * time.Second})
	m.sessions["sess-shutdown"] = c

	m.Finish()

	bk.mu.Lock()
	stored, ok := bk.stored["sess-shutdown"]
	bk.mu.Unlock()
	if !ok {
		t.Fatalf("expected snapshot PUT on shutdown")
	}
	if !bytes.Equal(stored.plaintext, plainTar) {
		t.Fatalf("snapshot plaintext mismatch")
	}

	m.mu.Lock()
	left := len(m.sessions)
	m.mu.Unlock()
	if left != 0 {
		t.Fatalf("expected sessions drained, got %d", left)
	}
}

func TestFetchSnapshotRejectsTruncatedStream(t *testing.T) {
	// /snapshot returns 200 + body but no X-Snapshot-Status trailer —
	// mirrors the executor hitting a walk error mid-tar. The orchestrator
	// must refuse the result so we don't PUT a half-baked snapshot.
	fc := newFakeContainer([]byte("partial-tar"))
	fc.mu.Lock()
	fc.snapshotOmitTrailer = true
	fc.mu.Unlock()
	defer fc.Close()
	c := fakeContainer(fc)

	m := NewManager(ManagerConfig{AdminAPIKey: "x"})
	_, err := m.fetchSnapshotFromContainer(context.Background(), c, "sess-truncated")
	if err == nil {
		t.Fatalf("expected error on missing snapshot trailer, got nil")
	}
	if !strings.Contains(err.Error(), "snapshot stream incomplete") {
		t.Fatalf("expected stream-incomplete error, got: %v", err)
	}
}

func TestGetOrAssignDiscardsUnhealthyWarmContainer(t *testing.T) {
	// First warm container 503s on /health; orchestrator must drop it,
	// pop the next one, and assign that.
	bk := newFakeBuckets()
	defer bk.Close()
	bad := newFakeContainer(nil)
	bad.mu.Lock()
	bad.healthStatus = http.StatusServiceUnavailable
	bad.mu.Unlock()
	defer bad.Close()
	good := newFakeContainer(nil)
	defer good.Close()

	cBad := fakeContainer(bad)
	cBad.Name = "bad"
	cGood := fakeContainer(good)
	cGood.Name = "good"

	m := NewManager(ManagerConfig{AdminAPIKey: "x", BucketsBase: bk.URL, PoolSize: 2, MaxContainers: 4, IdleTimeout: time.Hour})
	m.warmPool = []*Container{cBad, cGood}

	got, errMsg := m.GetOrAssign(context.Background(), "sess-health", nil)
	if got == nil {
		t.Fatalf("GetOrAssign failed: %s", errMsg)
	}
	if got != cGood {
		t.Fatalf("expected to assign healthy container %q, got %q", cGood.Name, got.Name)
	}
	m.mu.Lock()
	left := len(m.warmPool)
	m.mu.Unlock()
	if left != 0 {
		t.Fatalf("expected warm pool drained (1 discarded + 1 assigned), got %d left", left)
	}
}

func TestPerSessionSerialization(t *testing.T) {
	// No exec key → restore is skipped, GetOrAssign just pulls from
	// warm pool. Two goroutines racing for the same session should both
	// see the same container.
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeContainer(nil)
	defer fc.Close()
	c1 := fakeContainer(fc)
	c1.Name = "first"
	c2 := fakeContainer(fc)
	c2.Name = "second"

	m := NewManager(ManagerConfig{AdminAPIKey: "x", BucketsBase: bk.URL, PoolSize: 2, MaxContainers: 8, IdleTimeout: time.Hour})
	m.warmPool = []*Container{c1, c2}

	var wg sync.WaitGroup
	results := make([]*Container, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got, _ := m.GetOrAssign(context.Background(), "shared-session", nil)
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
	m.mu.Lock()
	left := len(m.warmPool)
	m.mu.Unlock()
	if left != 1 {
		t.Fatalf("expected exactly one container left in warm pool, got %d", left)
	}
}
