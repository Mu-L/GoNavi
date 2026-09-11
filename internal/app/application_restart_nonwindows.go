//go:build !windows

package app

import "fmt"

func restartApplicationProcess() error {
	return fmt.Errorf("application restart is only supported on Windows")
}
