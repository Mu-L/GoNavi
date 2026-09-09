//go:build !windows && (!darwin || !cgo)

package app

import "errors"

func setApplicationIconPNG(png []byte, _ string) error {
	_ = png
	return errors.New("application icon updates are only supported on macOS and Windows")
}
