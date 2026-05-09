//go:build !dev

package main

import (
	"context"
	"fmt"
	"net/http"
)

// AuthorizeSession verifies the bearer is a valid api_key.
//
//   - nil               on a valid api_key (200)
//   - ErrAuthRequired   when bearer is empty or controlplane refuses
//   - other err         on transport / unexpected upstream status
func (m *Manager) AuthorizeSession(ctx context.Context, bearer string) error {
	if bearer == "" {
		return ErrAuthRequired
	}
	if m.authCacheCheck(bearer) {
		return nil
	}
	status, err := m.cp.validateKey(ctx, bearer)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusOK:
		m.authCacheStore(bearer)
		return nil
	// common controlplane rejections — 401, 402, 403, 429
	case http.StatusUnauthorized,
		http.StatusPaymentRequired,
		http.StatusForbidden,
		http.StatusTooManyRequests:
		return ErrAuthRequired
	default:
		return fmt.Errorf("controlplane validate-key: status %d", status)
	}
}
