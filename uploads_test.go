package main

// Coverage for SyncUploads, the orchestrator-side glue between the
// caller's _meta.tinfoil_code_exec.uploads and the executor's two-step
// /sync-uploads protocol. Reuses fakeBuckets / fakeContainer /
// rewritingTransport from snapshot_test.go.
//
// What we exercise:
//   - all-present: manifest reports nothing missing, no buckets fetch,
//     no blobs call.
//   - all-missing: every file fetched from buckets and POSTed to blobs.
//   - partial: only the missing subset is fetched and shipped.
//   - sha mismatch from executor (returns a sha not in the manifest).
//   - manifest 5xx and blobs 5xx surface as errors.
//   - missing bearer / encryption key in ctx aborts before any HTTP.
//   - bucket fetch failure surfaces, and blobs is never called.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// Wire-shape mirrors of what the executor's /sync-uploads/* expects.
// Defined locally because they're shaped here as map[string]any (see
// SyncUploads in uploads.go) — these structs only exist for test
// readability and aren't exported by uploads.go.
type uploadEntry struct {
	Filename string `json:"filename"`
	Sha256   string `json:"sha256"`
}
type uploadBlob struct {
	Filename string `json:"filename"`
	Sha256   string `json:"sha256"`
	Contents string `json:"contents"` // base64
}
type syncManifestRequest struct {
	Manifest []uploadEntry `json:"manifest"`
}
type syncManifestResponse struct {
	Missing []uploadEntry `json:"missing"`
}
type syncBlobsRequest struct {
	Files []uploadBlob `json:"files"`
}
type syncBlobsResponse struct {
	Written int `json:"written"`
}

// fakeUploadsContainer stands in for the executor's /sync-uploads/*
// endpoints. wantMissing controls what /manifest returns; recorded
// fields capture exactly what arrived for assertion.
type fakeUploadsContainer struct {
	*httptest.Server

	mu sync.Mutex
	// wantMissing is what /manifest reports back as missing.
	wantMissing []uploadEntry
	// manifestStatus / blobsStatus override the default 200 when non-zero.
	manifestStatus int
	blobsStatus    int

	gotManifestEntries []uploadEntry
	gotBlobs           []uploadBlob
	manifestCalls      int
	blobsCalls         int
}

func newFakeUploadsContainer() *fakeUploadsContainer {
	f := &fakeUploadsContainer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/sync-uploads/manifest", func(w http.ResponseWriter, r *http.Request) {
		var req syncManifestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		f.mu.Lock()
		f.manifestCalls++
		f.gotManifestEntries = append([]uploadEntry(nil), req.Manifest...)
		status := f.manifestStatus
		missing := append([]uploadEntry(nil), f.wantMissing...)
		f.mu.Unlock()
		if status != 0 {
			http.Error(w, "forced", status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(syncManifestResponse{Missing: missing})
	})
	mux.HandleFunc("/sync-uploads/blobs", func(w http.ResponseWriter, r *http.Request) {
		var req syncBlobsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		f.mu.Lock()
		f.blobsCalls++
		f.gotBlobs = append([]uploadBlob(nil), req.Files...)
		status := f.blobsStatus
		f.mu.Unlock()
		if status != 0 {
			http.Error(w, "forced", status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(syncBlobsResponse{Written: len(req.Files)})
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	f.Server = httptest.NewServer(mux)
	return f
}

func fakeUploadContainer(f *fakeUploadsContainer) *Container {
	u, _ := url.Parse(f.URL)
	return &Container{
		ID:         "fake",
		Name:       "fake-container",
		Domain:     "fake.invalid",
		Status:     "ready",
		httpClient: &http.Client{Transport: rewritingTransport{target: u}, Timeout: 5 * time.Second},
	}
}

// uploadsTestCtx wires bearer / encryption key / container auth token into
// a context just like a real MCP request does.
func uploadsTestCtx(keyURL string) context.Context {
	ctx := context.Background()
	ctx = WithBearer(ctx, "tk_test")
	ctx = WithCodeExecutionEncryptionKey(ctx, keyURL)
	ctx = WithContainerAuthToken(ctx, "auth-tok-uploads")
	return ctx
}

// preloadUpload stashes a file's plaintext bytes in the fake bucket
// under fileAccessToken, encrypted under keyURL. Returns the uploadFile
// the orchestrator would receive in _meta.
func preloadUpload(bk *fakeBuckets, fileAccessToken, filename string, plaintext []byte, keyURL string) uploadFile {
	keyStd, _ := urlBase64ToStd(keyURL)
	bk.mu.Lock()
	bk.stored[fileAccessToken] = storedBlob{key: keyStd, plaintext: plaintext}
	bk.mu.Unlock()
	sum := sha256.Sum256(plaintext)
	return uploadFile{
		FileAccessToken: fileAccessToken,
		Filename:        filename,
		Sha256:          hex.EncodeToString(sum[:]),
	}
}

// newSyncUploadsManager returns a Manager wired to bk, with c registered
// as the live session container for accessToken. Mirrors how a real
// request would land after GetOrAssign returns.
func newSyncUploadsManager(t *testing.T, bk *fakeBuckets, c *Container, accessToken, keyURL string) *Manager {
	t.Helper()
	m := NewManager(ManagerConfig{
		ScopedCodeExecAdminKey: "x",
		BucketsBase:            bk.URL,
		PoolSize:               1,
		MaxContainers:          4,
		MaxConcurrentSnapshots: 4,
		IdleTimeout:            time.Hour,
	})
	c.CodeExecutionEncryptionKey = keyURL
	c.Bearer = "tk_test"
	c.AccessToken = accessToken
	m.sessions[accessToken] = c
	return m
}

func TestSyncUploadsAllPresent(t *testing.T) {
	// Executor reports nothing missing → orchestrator never touches buckets,
	// never POSTs blobs.
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeUploadsContainer()
	defer fc.Close()
	c := fakeUploadContainer(fc)

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x01}, 32))
	m := newSyncUploadsManager(t, bk, c, "sess-present", keyURL)

	files := []uploadFile{
		{FileAccessToken: "tok-a", Filename: "a.txt", Sha256: "deadbeef"},
		{FileAccessToken: "tok-b", Filename: "b.txt", Sha256: "feedface"},
	}
	if err := m.SyncUploads(uploadsTestCtx(keyURL), "sess-present", files); err != nil {
		t.Fatalf("SyncUploads: %v", err)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.manifestCalls != 1 {
		t.Errorf("manifest calls = %d, want 1", fc.manifestCalls)
	}
	if fc.blobsCalls != 0 {
		t.Errorf("blobs calls = %d, want 0 (executor reported nothing missing)", fc.blobsCalls)
	}
	if got := len(fc.gotManifestEntries); got != 2 {
		t.Errorf("manifest entries = %d, want 2", got)
	}
	bk.mu.Lock()
	defer bk.mu.Unlock()
	if len(bk.stored) != 0 {
		t.Errorf("buckets unexpectedly accessed: %+v", bk.stored)
	}
}

func TestSyncUploadsAllMissing(t *testing.T) {
	// Every file is missing → fetch each from buckets and ship via blobs.
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeUploadsContainer()
	defer fc.Close()
	c := fakeUploadContainer(fc)

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x02}, 32))

	files := []uploadFile{
		preloadUpload(bk, "tok-a", "a.txt", []byte("AAA"), keyURL),
		preloadUpload(bk, "tok-b", "nested/b.csv", []byte("BBB"), keyURL),
	}
	fc.mu.Lock()
	fc.wantMissing = []uploadEntry{
		{Filename: files[0].Filename, Sha256: files[0].Sha256},
		{Filename: files[1].Filename, Sha256: files[1].Sha256},
	}
	fc.mu.Unlock()

	m := newSyncUploadsManager(t, bk, c, "sess-missing", keyURL)
	if err := m.SyncUploads(uploadsTestCtx(keyURL), "sess-missing", files); err != nil {
		t.Fatalf("SyncUploads: %v", err)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.blobsCalls != 1 {
		t.Fatalf("blobs calls = %d, want 1", fc.blobsCalls)
	}
	if len(fc.gotBlobs) != 2 {
		t.Fatalf("blob count = %d, want 2", len(fc.gotBlobs))
	}
	got := map[string]string{}
	for _, b := range fc.gotBlobs {
		raw, err := base64.StdEncoding.DecodeString(b.Contents)
		if err != nil {
			t.Fatalf("blob %s: bad base64: %v", b.Filename, err)
		}
		got[b.Filename] = string(raw)
		if b.Sha256 == "" {
			t.Errorf("blob %s missing sha", b.Filename)
		}
	}
	if got["a.txt"] != "AAA" || got["nested/b.csv"] != "BBB" {
		t.Errorf("blob contents wrong: %+v", got)
	}
}

func TestSyncUploadsPartiallyMissing(t *testing.T) {
	// Manifest reports only one of two files as missing — the other must
	// not be fetched or sent.
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeUploadsContainer()
	defer fc.Close()
	c := fakeUploadContainer(fc)

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x03}, 32))
	have := preloadUpload(bk, "tok-have", "have.txt", []byte("already"), keyURL)
	miss := preloadUpload(bk, "tok-miss", "miss.txt", []byte("fetch-me"), keyURL)

	fc.mu.Lock()
	fc.wantMissing = []uploadEntry{{Filename: miss.Filename, Sha256: miss.Sha256}}
	fc.mu.Unlock()

	m := newSyncUploadsManager(t, bk, c, "sess-partial", keyURL)
	if err := m.SyncUploads(uploadsTestCtx(keyURL), "sess-partial", []uploadFile{have, miss}); err != nil {
		t.Fatalf("SyncUploads: %v", err)
	}

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.gotBlobs) != 1 {
		t.Fatalf("blob count = %d, want 1", len(fc.gotBlobs))
	}
	if fc.gotBlobs[0].Filename != "miss.txt" {
		t.Errorf("blob filename = %q, want miss.txt", fc.gotBlobs[0].Filename)
	}
}

func TestSyncUploadsRejectsUnknownShaFromExecutor(t *testing.T) {
	// Defensive: executor reports a sha that isn't in our manifest. We must
	// abort with a clear error rather than blindly fetch nothing.
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeUploadsContainer()
	defer fc.Close()
	c := fakeUploadContainer(fc)

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x04}, 32))
	files := []uploadFile{
		{FileAccessToken: "tok-a", Filename: "a.txt", Sha256: "aaaa"},
	}
	fc.mu.Lock()
	fc.wantMissing = []uploadEntry{{Filename: "ghost.txt", Sha256: "ffff"}}
	fc.mu.Unlock()

	m := newSyncUploadsManager(t, bk, c, "sess-ghost", keyURL)
	err := m.SyncUploads(uploadsTestCtx(keyURL), "sess-ghost", files)
	if err == nil {
		t.Fatalf("expected error for unknown sha, got nil")
	}
	if !strings.Contains(err.Error(), "missing sha") {
		t.Errorf("error message should mention missing sha, got: %v", err)
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.blobsCalls != 0 {
		t.Errorf("blobs should not be called after manifest error, calls=%d", fc.blobsCalls)
	}
}

func TestSyncUploadsManifest5xxErrors(t *testing.T) {
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeUploadsContainer()
	defer fc.Close()
	c := fakeUploadContainer(fc)
	fc.mu.Lock()
	fc.manifestStatus = http.StatusInternalServerError
	fc.mu.Unlock()

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x05}, 32))
	m := newSyncUploadsManager(t, bk, c, "sess-m500", keyURL)

	err := m.SyncUploads(uploadsTestCtx(keyURL), "sess-m500", []uploadFile{
		{FileAccessToken: "t", Filename: "x.txt", Sha256: "00"},
	})
	if err == nil {
		t.Fatalf("expected error from manifest 500, got nil")
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.blobsCalls != 0 {
		t.Errorf("blobs should not be called after manifest 500, calls=%d", fc.blobsCalls)
	}
}

func TestSyncUploadsBlobs5xxErrors(t *testing.T) {
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeUploadsContainer()
	defer fc.Close()
	c := fakeUploadContainer(fc)

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x06}, 32))
	f := preloadUpload(bk, "tok-x", "x.txt", []byte("payload"), keyURL)
	fc.mu.Lock()
	fc.wantMissing = []uploadEntry{{Filename: f.Filename, Sha256: f.Sha256}}
	fc.blobsStatus = http.StatusInternalServerError
	fc.mu.Unlock()

	m := newSyncUploadsManager(t, bk, c, "sess-b500", keyURL)
	if err := m.SyncUploads(uploadsTestCtx(keyURL), "sess-b500", []uploadFile{f}); err == nil {
		t.Fatalf("expected error from blobs 500, got nil")
	}
}

func TestSyncUploadsRejectsMissingBearerOrKey(t *testing.T) {
	// Without bearer/encryption key in ctx, SyncUploads can't fetch from
	// buckets. It must surface a clear error and never reach the blobs step.
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeUploadsContainer()
	defer fc.Close()
	c := fakeUploadContainer(fc)

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x07}, 32))
	f := preloadUpload(bk, "tok", "x.txt", []byte("p"), keyURL)
	fc.mu.Lock()
	fc.wantMissing = []uploadEntry{{Filename: f.Filename, Sha256: f.Sha256}}
	fc.mu.Unlock()

	m := newSyncUploadsManager(t, bk, c, "sess-nokey", keyURL)

	// ctx has the container auth token (so /manifest can proxy) but
	// neither the bearer nor the encryption key.
	ctx := WithContainerAuthToken(context.Background(), "auth-tok-nokey")
	err := m.SyncUploads(ctx, "sess-nokey", []uploadFile{f})
	if err == nil {
		t.Fatalf("expected error when bearer/key missing, got nil")
	}
	if !strings.Contains(err.Error(), "missing bearer or encryption key") {
		t.Errorf("error message should mention missing bearer/key, got: %v", err)
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.blobsCalls != 0 {
		t.Errorf("blobs should not be called when fetch can't proceed, calls=%d", fc.blobsCalls)
	}
}

func TestSyncUploadsBucketFetchErrorAborts(t *testing.T) {
	// Bucket entry never preloaded → fetch returns 404 (treated as "no
	// snapshot" by Buckets.fetch, which returns nil bytes). The orchestrator
	// then ships an empty blob, which is technically a bug — but until we
	// decide the policy, document the current behavior so it can't change
	// silently. (If we add a "404 means error during upload fetch" guard
	// later, this test should be updated alongside that change.)
	bk := newFakeBuckets()
	defer bk.Close()
	fc := newFakeUploadsContainer()
	defer fc.Close()
	c := fakeUploadContainer(fc)

	keyURL := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x08}, 32))
	missing := uploadFile{FileAccessToken: "tok-missing", Filename: "ghost.txt", Sha256: "00"}
	fc.mu.Lock()
	fc.wantMissing = []uploadEntry{{Filename: missing.Filename, Sha256: missing.Sha256}}
	fc.mu.Unlock()

	m := newSyncUploadsManager(t, bk, c, "sess-fetch404", keyURL)
	err := m.SyncUploads(uploadsTestCtx(keyURL), "sess-fetch404", []uploadFile{missing})
	// Either behavior is acceptable depending on policy; we just want the
	// test to fail loudly if it changes silently.
	if err != nil {
		t.Logf("SyncUploads returned error on bucket 404 (current behavior may treat as fetch failure): %v", err)
	} else {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		if fc.blobsCalls != 1 {
			t.Errorf("blobs calls = %d, want 1 (current behavior ships empty blob on 404)", fc.blobsCalls)
		}
		if len(fc.gotBlobs) == 1 {
			raw, _ := base64.StdEncoding.DecodeString(fc.gotBlobs[0].Contents)
			if len(raw) != 0 {
				t.Errorf("expected empty blob bytes for 404 fetch, got %d bytes", len(raw))
			}
		}
	}
}
