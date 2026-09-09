//go:build windows

package app

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	windowsApplicationUserModelShell32     = windows.NewLazySystemDLL("shell32.dll")
	windowsSetCurrentProcessAppUserModelID = func(applicationUserModelID string) error {
		encoded, err := windows.UTF16PtrFromString(applicationUserModelID)
		if err != nil {
			return fmt.Errorf("encode Windows application user model ID: %w", err)
		}
		result, _, _ := windowsApplicationUserModelShell32.NewProc("SetCurrentProcessExplicitAppUserModelID").Call(
			uintptr(unsafe.Pointer(encoded)),
		)
		if windowsHRESULTFailed(result) {
			return fmt.Errorf("set current process Windows application user model ID: HRESULT %#x", uint32(result))
		}
		return nil
	}
)

// ConfigureWindowsApplicationUserModelID fixes the process identity before
// Wails creates its first window. Every shortcut and window then belongs to the
// same taskbar group from the start of the process.
func ConfigureWindowsApplicationUserModelID() error {
	return windowsSetCurrentProcessAppUserModelID(windowsApplicationUserModelID)
}
