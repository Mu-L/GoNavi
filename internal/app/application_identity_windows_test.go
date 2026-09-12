//go:build windows

package app

import "testing"

func TestInitializeWindowsApplicationIdentityResolvesShellAPI(t *testing.T) {
	if err := InitializeWindowsApplicationIdentity(); err != nil {
		t.Fatalf("initialize Windows application identity: %v", err)
	}
}
