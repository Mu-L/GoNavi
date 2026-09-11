//go:build !windows

package app

import "errors"

func prepareWindowsBrandIconRestartPNG(_ []byte, _ string) error {
	return errors.New("Windows brand icon restart is only supported on Windows")
}
