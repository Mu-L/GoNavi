//go:build !windows && (!darwin || !cgo)

package app

import "testing"

func TestSetApplicationIconPNGIsExplicitlyUnsupportedOutsideNativePlatforms(t *testing.T) {
	if err := setApplicationIconPNG([]byte{0x89}, t.TempDir()); err == nil {
		t.Fatal("expected unsupported-platform error")
	}
}
