//go:build !windows

package syncworker

import (
	"context"

	"GoNavi-Wails/internal/syncjob"
)

// 非 Windows 平台暂未接入一次性调度模型：登录自启动仍走常驻 worker 的
// Register/Unregister（autostart_*.go）。以下桩仅保证接线代码可全平台编译；
// 调用方必须先用 runningOSSupportsJobSchedules() 分流。

// RunningOSSupportsJobSchedules 报告当前平台是否支持按任务的 OS 计划注册。
func RunningOSSupportsJobSchedules() bool { return false }

func RegisterJobSchedule(ctx context.Context, root, executable, jobID string, spec syncjob.ScheduleSpec) error {
	return nil
}

func UnregisterJobSchedule(ctx context.Context, root, jobID string) error {
	return nil
}

func UnregisterLegacyLogonTask(ctx context.Context, root string) error {
	return nil
}

func StopLegacyWorker(ctx context.Context, root string) error {
	return nil
}
