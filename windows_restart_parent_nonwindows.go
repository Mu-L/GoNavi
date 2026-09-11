//go:build !windows

package main

func waitForWindowsRestartParent(_ []string) error { return nil }
