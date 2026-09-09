//go:build windows

package app

import (
	"errors"
	"testing"
)

func TestConfigureWindowsApplicationUserModelIDSetsSharedIdentity(t *testing.T) {
	original := windowsSetCurrentProcessAppUserModelID
	t.Cleanup(func() {
		windowsSetCurrentProcessAppUserModelID = original
	})

	var got string
	windowsSetCurrentProcessAppUserModelID = func(applicationUserModelID string) error {
		got = applicationUserModelID
		return nil
	}

	if err := ConfigureWindowsApplicationUserModelID(); err != nil {
		t.Fatalf("configure Windows application user model ID: %v", err)
	}
	if got != windowsApplicationUserModelID {
		t.Fatalf("application user model ID = %q, want %q", got, windowsApplicationUserModelID)
	}
}

func TestConfigureWindowsApplicationUserModelIDReturnsShellFailure(t *testing.T) {
	original := windowsSetCurrentProcessAppUserModelID
	t.Cleanup(func() {
		windowsSetCurrentProcessAppUserModelID = original
	})
	windowsSetCurrentProcessAppUserModelID = func(string) error {
		return errors.New("shell unavailable")
	}

	if err := ConfigureWindowsApplicationUserModelID(); err == nil {
		t.Fatal("configure Windows application user model ID succeeded despite Shell failure")
	}
}
