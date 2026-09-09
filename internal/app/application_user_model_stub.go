//go:build !windows

package app

// ConfigureWindowsApplicationUserModelID is a no-op outside Windows.
func ConfigureWindowsApplicationUserModelID() error {
	return nil
}
