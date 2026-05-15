package main

// /user-uploads sync between buckets and the session container.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// uploadFile is one entry from _meta.tinfoil_code_exec.uploads.
//   - FileAccessToken: bucket access_token
//   - Filename:        file-name
//   - Sha256:          expected hex sha256 of the file's plaintext bytes
type uploadFile struct {
	FileAccessToken string `json:"fileAccessToken"`
	Filename        string `json:"filename"`
	Sha256          string `json:"sha256"`
}

// fetchFile retrieves one uploaded file's plaintext bytes from buckets.
// Same wire shape as fetch (the snapshot helper); the difference is
// purely how the caller interprets the bytes — file vs tar.
func (b *Buckets) fetchFile(ctx context.Context, bearer, fileAccessToken, encryptionKeyB64 string) ([]byte, error) {
	return b.fetch(ctx, bearer, fileAccessToken, encryptionKeyB64)
}

func (m *Manager) SyncUploads(ctx context.Context, accessToken string, files []uploadFile) error {
	c, errMsg := m.GetOrAssign(ctx, accessToken, nil)
	if c == nil {
		return fmt.Errorf("%s", errMsg)
	}

	// Step 1: tell the executor what /user-uploads should contain. It
	// drops anything stale and returns the shas it doesn't have.
	manifestEntries := make([]map[string]string, len(files))
	for i, f := range files {
		manifestEntries[i] = map[string]string{
			"filename": f.Filename,
			"sha256":   f.Sha256,
		}
	}
	manifestBody, _ := json.Marshal(map[string]any{"manifest": manifestEntries})
	status, raw, err := m.proxy(ctx, c, "/sync-uploads/manifest", manifestBody)
	if err != nil {
		return fmt.Errorf("sync-uploads manifest: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("sync-uploads manifest returned %d: %s", status, string(raw))
	}
	var missingResp struct {
		Missing []struct {
			Filename string `json:"filename"`
			Sha256   string `json:"sha256"`
		} `json:"missing"`
	}
	if err := json.Unmarshal(raw, &missingResp); err != nil {
		return fmt.Errorf("decode missing: %w", err)
	}
	if len(missingResp.Missing) == 0 {
		return nil
	}

	// Map missing shas back to their bucket fileAccessToken.
	bySha := make(map[string]uploadFile, len(files))
	for _, f := range files {
		bySha[f.Sha256] = f
	}

	bearer := sessionBearer(ctx)
	encryptionKey := sessionCodeExecutionEncryptionKey(ctx)
	if bearer == "" || encryptionKey == "" {
		return fmt.Errorf("missing bearer or encryption key for upload fetch")
	}

	// Step 2a: fetch missing bytes from buckets in parallel. One goroutine
	// per file; the bucket client retries transient failures internally.
	type fetchResult struct {
		idx  int
		data []byte
		err  error
	}
	results := make([]fetchResult, len(missingResp.Missing))
	var wg sync.WaitGroup
	for i, miss := range missingResp.Missing {
		f, ok := bySha[miss.Sha256]
		if !ok {
			return fmt.Errorf("executor reported missing sha %s not in manifest", miss.Sha256)
		}
		wg.Add(1)
		go func(i int, f uploadFile) {
			defer wg.Done()
			data, err := m.buckets.fetchFile(ctx, bearer, f.FileAccessToken, encryptionKey)
			results[i] = fetchResult{idx: i, data: data, err: err}
		}(i, f)
	}
	wg.Wait()

	// Surface the first fetch error if any.
	for i, r := range results {
		if r.err != nil {
			return fmt.Errorf("fetch %s: %w", missingResp.Missing[i].Filename, r.err)
		}
	}

	// Step 2b: ship the bytes to the executor.
	blobs := make([]map[string]any, len(results))
	for i, r := range results {
		blobs[i] = map[string]any{
			"filename": missingResp.Missing[i].Filename,
			"sha256":   missingResp.Missing[i].Sha256,
			"contents": base64.StdEncoding.EncodeToString(r.data),
		}
	}
	blobsBody, _ := json.Marshal(map[string]any{"files": blobs})
	status, raw, err = m.proxy(ctx, c, "/sync-uploads/blobs", blobsBody)
	if err != nil {
		return fmt.Errorf("sync-uploads blobs: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("sync-uploads blobs returned %d: %s", status, string(raw))
	}
	return nil
}
