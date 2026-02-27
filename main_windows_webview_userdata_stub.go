//go:build !windows

package main

func resolveWindowsWebviewUserDataPath() string {
	return ""
}
