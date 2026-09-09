//go:build windows

package app

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	windowsImageIcon       = 1
	windowsLoadFromFile    = 0x0010
	windowsSetIconMessage  = 0x0080
	windowsIconSmall       = 0
	windowsIconBig         = 1
	windowsSmallIconPixels = 16
	windowsLargeIconPixels = 32
)

var (
	windowsApplicationIconUser32      = windows.NewLazySystemDLL("user32.dll")
	windowsApplicationIconLoadImage   = windowsApplicationIconUser32.NewProc("LoadImageW")
	windowsApplicationIconSendMessage = windowsApplicationIconUser32.NewProc("SendMessageW")
	windowsApplicationIconDestroy     = windowsApplicationIconUser32.NewProc("DestroyIcon")
	windowsApplicationIconHandleMu    sync.Mutex
	windowsApplicationIconSmallHandle uintptr
	windowsApplicationIconLargeHandle uintptr
)

func setApplicationIconPNG(pngBytes []byte, configDir string) error {
	if len(pngBytes) == 0 {
		return errors.New("application icon PNG is empty")
	}
	if strings.TrimSpace(configDir) == "" {
		configDir = resolveAppConfigDir()
	}
	iconPath, err := persistWindowsApplicationIcon(pngBytes, configDir)
	if err != nil {
		return err
	}
	if err := setCurrentWindowsApplicationIcon(iconPath); err != nil {
		return err
	}
	return updateCurrentWindowsApplicationShortcuts(iconPath)
}

func setCurrentWindowsApplicationIcon(iconPath string) error {
	small, err := loadWindowsApplicationIcon(iconPath, windowsSmallIconPixels)
	if err != nil {
		return err
	}
	large, err := loadWindowsApplicationIcon(iconPath, windowsLargeIconPixels)
	if err != nil {
		destroyWindowsApplicationIcon(small)
		return err
	}

	currentPID := windows.GetCurrentProcessId()
	updatedWindows := 0
	callback := windows.NewCallback(func(hwnd uintptr, _ uintptr) uintptr {
		var ownerPID uint32
		if _, ownerErr := windows.GetWindowThreadProcessId(windows.HWND(hwnd), &ownerPID); ownerErr == nil && ownerPID == currentPID {
			windowsApplicationIconSendMessage.Call(hwnd, windowsSetIconMessage, windowsIconSmall, small)
			windowsApplicationIconSendMessage.Call(hwnd, windowsSetIconMessage, windowsIconBig, large)
			updatedWindows++
		}
		return 1
	})
	if err := windows.EnumWindows(callback, nil); err != nil {
		destroyWindowsApplicationIcon(small)
		destroyWindowsApplicationIcon(large)
		return fmt.Errorf("enumerate Windows application windows: %w", err)
	}
	if updatedWindows == 0 {
		destroyWindowsApplicationIcon(small)
		destroyWindowsApplicationIcon(large)
		return errors.New("no Windows application window was found")
	}

	windowsApplicationIconHandleMu.Lock()
	previousSmall := windowsApplicationIconSmallHandle
	previousLarge := windowsApplicationIconLargeHandle
	windowsApplicationIconSmallHandle = small
	windowsApplicationIconLargeHandle = large
	windowsApplicationIconHandleMu.Unlock()
	destroyWindowsApplicationIcon(previousSmall)
	destroyWindowsApplicationIcon(previousLarge)
	return nil
}

func loadWindowsApplicationIcon(iconPath string, size int) (uintptr, error) {
	path, err := windows.UTF16PtrFromString(iconPath)
	if err != nil {
		return 0, fmt.Errorf("encode Windows application icon path: %w", err)
	}
	handle, _, callErr := windowsApplicationIconLoadImage.Call(
		0,
		uintptr(unsafe.Pointer(path)),
		windowsImageIcon,
		uintptr(size),
		uintptr(size),
		windowsLoadFromFile,
	)
	if handle == 0 {
		return 0, fmt.Errorf("load %dx%d Windows application icon: %w", size, size, callErr)
	}
	return handle, nil
}

func destroyWindowsApplicationIcon(handle uintptr) {
	if handle != 0 {
		windowsApplicationIconDestroy.Call(handle)
	}
}

func updateCurrentWindowsApplicationShortcuts(iconPath string) error {
	executablePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve Windows application executable: %w", err)
	}
	scriptDir := filepath.Dir(iconPath)
	temporary, err := os.CreateTemp(scriptDir, ".gonavi-brand-shortcuts-*.ps1")
	if err != nil {
		return fmt.Errorf("create Windows shortcut update script: %w", err)
	}
	scriptPath := temporary.Name()
	defer os.Remove(scriptPath)
	script := windowsShortcutRepairPowerShellScript + `

$ErrorActionPreference = 'Stop'
[void](Set-GoNaviShortcutBrandIcon -TargetPath $env:GONAVI_BRAND_TARGET -IconPath $env:GONAVI_BRAND_ICON)
`
	if _, err := temporary.WriteString(strings.ReplaceAll(script, "\n", "\r\n")); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write Windows shortcut update script: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close Windows shortcut update script: %w", err)
	}

	cmd := exec.Command(
		"powershell.exe",
		"-NoProfile",
		"-NonInteractive",
		"-ExecutionPolicy",
		windowsUpdatePowerShellExecutionPolicy,
		"-File",
		scriptPath,
	)
	cmd.Dir = scriptDir
	cmd.Env = append(cmd.Environ(),
		"GONAVI_BRAND_TARGET="+executablePath,
		"GONAVI_BRAND_ICON="+iconPath,
	)
	configureWindowsUpdateCommand(cmd)
	if output, err := cmd.CombinedOutput(); err != nil {
		detail := strings.TrimSpace(string(output))
		if detail != "" {
			return fmt.Errorf("update Windows application shortcuts: %w: %s", err, detail)
		}
		return fmt.Errorf("update Windows application shortcuts: %w", err)
	}
	return nil
}
