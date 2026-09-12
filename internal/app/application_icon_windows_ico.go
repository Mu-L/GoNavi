package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/image/draw"
)

const (
	windowsApplicationIconDirectoryName = "application-icons"
	windowsApplicationIconStateFileName = "active-icon.txt"
)

var windowsApplicationIconSizes = []int{16, 24, 32, 48, 64, 128, 256}

func buildWindowsApplicationIconICO(pngBytes []byte) ([]byte, error) {
	source, err := png.Decode(bytes.NewReader(pngBytes))
	if err != nil {
		return nil, fmt.Errorf("decode application icon PNG: %w", err)
	}

	frames := make([][]byte, 0, len(windowsApplicationIconSizes))
	for _, size := range windowsApplicationIconSizes {
		target := image.NewNRGBA(image.Rect(0, 0, size, size))
		draw.CatmullRom.Scale(target, target.Bounds(), source, source.Bounds(), draw.Over, nil)
		var encoded bytes.Buffer
		if err := png.Encode(&encoded, target); err != nil {
			return nil, fmt.Errorf("encode %dx%d application icon frame: %w", size, size, err)
		}
		frames = append(frames, encoded.Bytes())
	}

	const headerSize = 6
	const directoryEntrySize = 16
	payloadOffset := headerSize + (directoryEntrySize * len(frames))
	var ico bytes.Buffer
	_ = binary.Write(&ico, binary.LittleEndian, uint16(0))
	_ = binary.Write(&ico, binary.LittleEndian, uint16(1))
	_ = binary.Write(&ico, binary.LittleEndian, uint16(len(frames)))
	for index, frame := range frames {
		size := windowsApplicationIconSizes[index]
		encodedSize := byte(size)
		if size == 256 {
			encodedSize = 0
		}
		ico.WriteByte(encodedSize)
		ico.WriteByte(encodedSize)
		ico.WriteByte(0)
		ico.WriteByte(0)
		_ = binary.Write(&ico, binary.LittleEndian, uint16(1))
		_ = binary.Write(&ico, binary.LittleEndian, uint16(32))
		_ = binary.Write(&ico, binary.LittleEndian, uint32(len(frame)))
		_ = binary.Write(&ico, binary.LittleEndian, uint32(payloadOffset))
		payloadOffset += len(frame)
	}
	for _, frame := range frames {
		_, _ = ico.Write(frame)
	}
	return ico.Bytes(), nil
}

func windowsApplicationIconCandidatePath(pngBytes []byte, configDir string) string {
	hash := sha256.Sum256(pngBytes)
	return filepath.Join(strings.TrimSpace(configDir), windowsApplicationIconDirectoryName, fmt.Sprintf("gonavi-brand-%x.ico", hash[:12]))
}

func persistWindowsApplicationIcon(pngBytes []byte, configDir string) (string, error) {
	configDir = strings.TrimSpace(configDir)
	if configDir == "" {
		return "", errors.New("application config directory is empty")
	}
	ico, err := buildWindowsApplicationIconICO(pngBytes)
	if err != nil {
		return "", err
	}
	iconDir := filepath.Join(configDir, windowsApplicationIconDirectoryName)
	if err := os.MkdirAll(iconDir, 0o755); err != nil {
		return "", fmt.Errorf("create Windows application icon directory: %w", err)
	}
	iconPath := windowsApplicationIconCandidatePath(pngBytes, configDir)
	if existing, err := os.ReadFile(iconPath); err == nil && bytes.Equal(existing, ico) {
		return iconPath, nil
	}
	temporary, err := os.CreateTemp(iconDir, ".gonavi-brand-*.ico.tmp")
	if err != nil {
		return "", fmt.Errorf("create temporary Windows application icon: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(ico); err != nil {
		_ = temporary.Close()
		return "", fmt.Errorf("write Windows application icon: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close Windows application icon: %w", err)
	}
	if err := os.Remove(iconPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("replace Windows application icon: %w", err)
	}
	if err := os.Rename(temporaryPath, iconPath); err != nil {
		return "", fmt.Errorf("commit Windows application icon: %w", err)
	}
	return iconPath, nil
}

func activatePersistedWindowsApplicationIcon(iconPath, configDir string) error {
	configDir = strings.TrimSpace(configDir)
	if configDir == "" {
		return errors.New("application config directory is empty")
	}
	iconDir := filepath.Join(configDir, windowsApplicationIconDirectoryName)
	return persistWindowsApplicationIconSelection(iconPath, iconDir)
}

func clearPersistedWindowsApplicationIcon(configDir string) error {
	configDir = strings.TrimSpace(configDir)
	if configDir == "" {
		return errors.New("application config directory is empty")
	}
	iconDir := filepath.Join(configDir, windowsApplicationIconDirectoryName)
	if err := os.MkdirAll(iconDir, 0o755); err != nil {
		return fmt.Errorf("create Windows application icon directory: %w", err)
	}
	return writeWindowsApplicationIconState("", iconDir)
}

func persistWindowsApplicationIconSelection(iconPath, iconDir string) error {
	cleanIconPath := filepath.Clean(iconPath)
	cleanIconDir := filepath.Clean(iconDir)
	iconName := filepath.Base(cleanIconPath)
	if iconName == "." || iconName == string(filepath.Separator) || filepath.Dir(cleanIconPath) != cleanIconDir {
		return errors.New("Windows application icon state path is not a direct icon file")
	}
	return writeWindowsApplicationIconState(iconName, iconDir)
}

func writeWindowsApplicationIconState(iconName, iconDir string) error {
	statePath := filepath.Join(iconDir, windowsApplicationIconStateFileName)
	temporary, err := os.CreateTemp(iconDir, ".gonavi-active-icon-*.tmp")
	if err != nil {
		return fmt.Errorf("create Windows application icon state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.WriteString(iconName + "\n"); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write Windows application icon state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync Windows application icon state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close Windows application icon state: %w", err)
	}
	if err := os.Remove(statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("replace Windows application icon state: %w", err)
	}
	if err := os.Rename(temporaryPath, statePath); err != nil {
		return fmt.Errorf("commit Windows application icon state: %w", err)
	}
	return nil
}

func loadPersistedWindowsApplicationIcon(configDir string) (string, error) {
	configDir = strings.TrimSpace(configDir)
	if configDir == "" {
		return "", errors.New("application config directory is empty")
	}
	iconDir := filepath.Join(configDir, windowsApplicationIconDirectoryName)
	statePath := filepath.Join(iconDir, windowsApplicationIconStateFileName)
	data, err := os.ReadFile(statePath)
	if err == nil {
		iconName := strings.TrimSpace(string(data))
		if iconName == "" {
			return "", nil
		}
		if filepath.Base(iconName) == iconName && filepath.Ext(iconName) == ".ico" {
			iconPath := filepath.Join(iconDir, iconName)
			if info, statErr := os.Stat(iconPath); statErr == nil && info.Mode().IsRegular() {
				return iconPath, nil
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("read Windows application icon state: %w", err)
	}

	// Versions before the startup pointer was introduced already left
	// content-addressed ICO files behind. Pick the newest valid file once so
	// an existing user's selected icon is migrated without requiring a second
	// manual selection. New writes always create active-icon.txt and take
	// precedence above this compatibility fallback.
	entries, err := os.ReadDir(iconDir)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read Windows application icon directory: %w", err)
	}
	var newestPath string
	var newestModTime time.Time
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "gonavi-brand-") || filepath.Ext(entry.Name()) != ".ico" {
			continue
		}
		info, statErr := entry.Info()
		if statErr != nil || !info.Mode().IsRegular() {
			continue
		}
		if newestPath == "" || info.ModTime().After(newestModTime) ||
			(info.ModTime().Equal(newestModTime) && entry.Name() > filepath.Base(newestPath)) {
			newestPath = filepath.Join(iconDir, entry.Name())
			newestModTime = info.ModTime()
		}
	}
	return newestPath, nil
}
