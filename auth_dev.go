//go:build dev

package main

import "context"

// AuthorizeSession is a no-op in dev builds. Never ship a binary built
// with -tags dev.
func (m *Manager) AuthorizeSession(ctx context.Context, bearer string) error {
	return nil
}
