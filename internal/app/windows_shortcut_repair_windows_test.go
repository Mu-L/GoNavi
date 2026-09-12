package app

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsShortcutRepairOnlyMigratesMissingMSIIconForCurrentTarget(t *testing.T) {
	powerShell, err := exec.LookPath("powershell.exe")
	if err != nil {
		t.Skip("powershell.exe is unavailable")
	}

	tempDir := t.TempDir()
	targetPath := filepath.Join(tempDir, "install", "GoNavi.exe")
	foreignTargetPath := filepath.Join(tempDir, "foreign", "GoNavi.exe")
	pinsDirectory := filepath.Join(tempDir, "pins")
	installerDirectory := filepath.Join(tempDir, "Windows", "Installer")
	desktopDirectory := filepath.Join(tempDir, "desktop")
	commonDesktopDirectory := filepath.Join(tempDir, "common-desktop")
	for _, directory := range []string{
		filepath.Dir(targetPath),
		filepath.Dir(foreignTargetPath),
		pinsDirectory,
		installerDirectory,
		desktopDirectory,
		commonDesktopDirectory,
	} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{targetPath, foreignTargetPath} {
		if err := os.WriteFile(path, []byte("test"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	existingIconPath := filepath.Join(installerDirectory, "{22222222-2222-2222-2222-222222222222}", "GoNaviIcon")
	if err := os.MkdirAll(filepath.Dir(existingIconPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(existingIconPath, []byte("icon"), 0o644); err != nil {
		t.Fatal(err)
	}

	harness := windowsShortcutRepairPowerShellScript + `
$ErrorActionPreference = 'Stop'
$shell = New-Object -ComObject WScript.Shell
$shellApplication = New-Object -ComObject Shell.Application

function New-TestShortcut {
    param(
        [string]$Path,
        [string]$TargetPath,
        [string]$IconLocation
    )

    $shortcut = $shell.CreateShortcut($Path)
    $shortcut.TargetPath = $TargetPath
    if (-not [string]::IsNullOrEmpty($IconLocation)) {
        $shortcut.IconLocation = $IconLocation
    }
    $shortcut.Save()
}

function Test-ShortcutIconLocation {
    param(
        [string]$IconLocation,
        [string]$ExpectedPath
    )

    $indexedIcon = [regex]::Match($IconLocation, '^(?<path>.+),\s*(?<index>-?\d+)$')
    if (-not $indexedIcon.Success -or $indexedIcon.Groups['index'].Value -ne '0') {
        return $false
    }
    return Test-SameFilePath $indexedIcon.Groups['path'].Value.Trim('"') $ExpectedPath
}

$target = $env:GONAVI_TEST_TARGET
$foreignTarget = $env:GONAVI_TEST_FOREIGN_TARGET
$pins = $env:GONAVI_TEST_PINS
$installer = $env:GONAVI_TEST_INSTALLER
$desktop = $env:GONAVI_TEST_DESKTOP
$commonDesktop = $env:GONAVI_TEST_COMMON_DESKTOP
$missingIcon = Join-Path $installer '{11111111-1111-1111-1111-111111111111}\GoNaviIcon'
$existingIcon = Join-Path $installer '{22222222-2222-2222-2222-222222222222}\GoNaviIcon'

New-TestShortcut (Join-Path $pins 'missing-icon.lnk') $target ($missingIcon + ',0')
New-TestShortcut (Join-Path $pins 'foreign-target.lnk') $foreignTarget ($missingIcon + ',0')
New-TestShortcut (Join-Path $pins 'existing-icon.lnk') $target ($existingIcon + ',0')
New-TestShortcut (Join-Path $pins 'blank-icon.lnk') $target ''
New-TestShortcut (Join-Path $pins 'other-missing-icon.lnk') $target ((Join-Path $installer '{33333333-3333-3333-3333-333333333333}\OtherIcon') + ',0')
$alternateGoNaviTarget = Join-Path $env:GONAVI_TEST_ROOT 'alternate-install\GoNavi.exe'
New-TestShortcut (Join-Path $pins 'GoNavi.lnk') $alternateGoNaviTarget ''
[void](Set-GoNaviShortcutRelaunchProperties -ShortcutPath (Join-Path $pins 'GoNavi.lnk') -TargetPath $alternateGoNaviTarget -IconPath $missingIcon)
# A pin left behind by a previous rotated brand identity only ties back to
# GoNavi through its AppUserModel.ID, so the brand update must recognize every
# identity in the Syngnat.GoNavi family without redirecting its launch target.
New-TestShortcut (Join-Path $pins 'GoNavi-rotated.lnk') $alternateGoNaviTarget ''
[void](Set-GoNaviShortcutRelaunchProperties -ShortcutPath (Join-Path $pins 'GoNavi-rotated.lnk') -TargetPath $alternateGoNaviTarget -IconPath $missingIcon -ApplicationUserModelID 'Syngnat.GoNavi.Icon.deadbeefdeadbeefdeadbeef')
$alternateTargetBefore = $shell.CreateShortcut((Join-Path $pins 'GoNavi.lnk')).TargetPath
$blankIconBefore = $shell.CreateShortcut((Join-Path $pins 'blank-icon.lnk')).IconLocation
$otherMissingIconBefore = $shell.CreateShortcut((Join-Path $pins 'other-missing-icon.lnk')).IconLocation

$repairCount = Repair-LegacyGoNaviTaskbarPins -TargetPath $target -PinsDirectory $pins -WindowsInstallerDirectory $installer
if ($repairCount -ne 1) {
    throw ('unexpected repair count: ' + $repairCount)
}

$missingShortcut = $shell.CreateShortcut((Join-Path $pins 'missing-icon.lnk'))
if (-not (Test-ShortcutIconLocation $missingShortcut.IconLocation $target)) {
    throw ('missing MSI icon was not migrated: ' + $missingShortcut.IconLocation)
}
$foreignShortcut = $shell.CreateShortcut((Join-Path $pins 'foreign-target.lnk'))
if (-not (Test-ShortcutIconLocation $foreignShortcut.IconLocation $missingIcon)) {
    throw ('foreign target was modified: ' + $foreignShortcut.IconLocation)
}
$existingShortcut = $shell.CreateShortcut((Join-Path $pins 'existing-icon.lnk'))
if (-not (Test-ShortcutIconLocation $existingShortcut.IconLocation $existingIcon)) {
    throw ('existing MSI icon was modified: ' + $existingShortcut.IconLocation)
}
$blankShortcut = $shell.CreateShortcut((Join-Path $pins 'blank-icon.lnk'))
if (-not [string]::Equals($blankShortcut.IconLocation, $blankIconBefore, [StringComparison]::OrdinalIgnoreCase)) {
    throw ('blank icon was modified: ' + $blankShortcut.IconLocation)
}
$otherMissingIconShortcut = $shell.CreateShortcut((Join-Path $pins 'other-missing-icon.lnk'))
if (-not [string]::Equals($otherMissingIconShortcut.IconLocation, $otherMissingIconBefore, [StringComparison]::OrdinalIgnoreCase)) {
    throw ('non-GoNavi MSI icon was modified: ' + $otherMissingIconShortcut.IconLocation)
}

$brandIcon = Join-Path $env:GONAVI_TEST_ROOT 'gonavi-brand-test.ico'
[IO.File]::WriteAllBytes($brandIcon, [byte[]](0, 0, 1, 0, 0, 0))
$brandUpdateCount = Set-GoNaviShortcutBrandIcon -TargetPath $target -IconPath $brandIcon -ShortcutDirectories @($pins) -TaskbarDirectory $pins
if ($brandUpdateCount -ne 6) {
    throw ('unexpected brand icon shortcut update count: ' + $brandUpdateCount)
}
foreach ($shortcutName in @('missing-icon.lnk', 'existing-icon.lnk', 'blank-icon.lnk', 'other-missing-icon.lnk')) {
    $updatedShortcut = $shell.CreateShortcut((Join-Path $pins $shortcutName))
    if (-not (Test-ShortcutIconLocation $updatedShortcut.IconLocation $brandIcon)) {
        throw ('brand icon was not applied to ' + $shortcutName + ': ' + $updatedShortcut.IconLocation)
    }
}
if (-not (Test-ShortcutIconLocation $shell.CreateShortcut((Join-Path $pins 'foreign-target.lnk')).IconLocation $missingIcon)) {
    throw 'brand icon update modified a foreign target shortcut'
}
$alternateShortcut = $shell.CreateShortcut((Join-Path $pins 'GoNavi.lnk'))
if (-not (Test-ShortcutIconLocation $alternateShortcut.IconLocation $brandIcon)) {
    throw ('brand icon was not applied to the alternate GoNavi pin: ' + $alternateShortcut.IconLocation)
}
if (-not [string]::Equals($alternateShortcut.TargetPath, $alternateTargetBefore, [StringComparison]::OrdinalIgnoreCase)) {
    throw ('alternate GoNavi pin target was changed: ' + $alternateShortcut.TargetPath)
}
$alternateItem = $shellApplication.Namespace((Split-Path (Join-Path $pins 'GoNavi.lnk') -Parent)).ParseName('GoNavi.lnk')
if ($null -ne $alternateItem -and -not (Test-SameFilePath ([string]$alternateItem.ExtendedProperty('System.AppUserModel.RelaunchCommand')) $alternateTargetBefore)) {
    throw ('alternate GoNavi relaunch target was changed: ' + $alternateItem.ExtendedProperty('System.AppUserModel.RelaunchCommand'))
}
$rotatedShortcut = $shell.CreateShortcut((Join-Path $pins 'GoNavi-rotated.lnk'))
if (-not (Test-ShortcutIconLocation $rotatedShortcut.IconLocation $brandIcon)) {
    throw ('brand icon was not applied to the rotated-identity pin: ' + $rotatedShortcut.IconLocation)
}
if (-not [string]::Equals($rotatedShortcut.TargetPath, $alternateGoNaviTarget, [StringComparison]::OrdinalIgnoreCase)) {
    throw ('rotated-identity pin target was changed: ' + $rotatedShortcut.TargetPath)
}
$rotatedItemBefore = $shellApplication.Namespace($pins).ParseName('GoNavi-rotated.lnk')
if ($null -ne $rotatedItemBefore -and -not [string]::Equals([string]$rotatedItemBefore.ExtendedProperty('System.AppUserModel.ID'), 'Syngnat.GoNavi', [StringComparison]::OrdinalIgnoreCase)) {
    throw ('rotated-identity pin was not moved to the base identity: ' + $rotatedItemBefore.ExtendedProperty('System.AppUserModel.ID'))
}

# A later selection rotates the identity again: the new value must reach the
# property store of every recognized pin so the taskbar re-renders its icon.
$rotatedIcon = Join-Path $env:GONAVI_TEST_ROOT 'gonavi-brand-0123456789abcdef01234567.ico'
[IO.File]::WriteAllBytes($rotatedIcon, [byte[]](0, 0, 1, 0, 0, 0))
$rotatedAumid = 'Syngnat.GoNavi.Icon.0123456789abcdef01234567'
$rotatedBrandCount = Set-GoNaviShortcutBrandIcon -TargetPath $target -IconPath $rotatedIcon -ApplicationUserModelID $rotatedAumid -ShortcutDirectories @($pins) -TaskbarDirectory $pins
if ($rotatedBrandCount -ne 6) {
    throw ('unexpected rotated brand icon shortcut update count: ' + $rotatedBrandCount)
}
foreach ($shortcutName in @('missing-icon.lnk', 'existing-icon.lnk', 'blank-icon.lnk', 'other-missing-icon.lnk', 'GoNavi.lnk', 'GoNavi-rotated.lnk')) {
    $updatedShortcut = $shell.CreateShortcut((Join-Path $pins $shortcutName))
    if (-not (Test-ShortcutIconLocation $updatedShortcut.IconLocation $rotatedIcon)) {
        throw ('rotated brand icon was not applied to ' + $shortcutName + ': ' + $updatedShortcut.IconLocation)
    }
}
if (-not (Test-ShortcutIconLocation $shell.CreateShortcut((Join-Path $pins 'foreign-target.lnk')).IconLocation $missingIcon)) {
    throw 'rotated brand icon update modified a foreign target shortcut'
}
$rotatedItem = $shellApplication.Namespace($pins).ParseName('GoNavi-rotated.lnk')
if ($null -ne $rotatedItem -and -not [string]::Equals([string]$rotatedItem.ExtendedProperty('System.AppUserModel.ID'), $rotatedAumid, [StringComparison]::OrdinalIgnoreCase)) {
    throw ('rotated identity was not written to the pin property store: ' + $rotatedItem.ExtendedProperty('System.AppUserModel.ID'))
}

$desktopDirectories = @($desktop, $commonDesktop)
$absentState = Save-GoNaviDesktopShortcutState -TargetPath $target -BackupDirectory (Join-Path $env:GONAVI_TEST_ROOT 'backup-absent') -DesktopDirectories $desktopDirectories
if (-not $absentState.Succeeded -or $absentState.InstallValue -ne '0' -or $absentState.Entries.Count -ne 0) {
    throw 'desktop shortcut should remain absent'
}
New-TestShortcut (Join-Path $desktop 'GoNavi.lnk') $foreignTarget ''
$foreignState = Save-GoNaviDesktopShortcutState -TargetPath $target -BackupDirectory (Join-Path $env:GONAVI_TEST_ROOT 'backup-foreign') -DesktopDirectories $desktopDirectories
if (-not $foreignState.Succeeded -or $foreignState.InstallValue -ne '0' -or $foreignState.Entries.Count -ne 1) {
    throw 'foreign desktop shortcut state was not captured'
}
New-TestShortcut (Join-Path $desktop 'GoNavi.lnk') $target ''
if (-not (Remove-GoNaviDesktopShortcutsForTarget -TargetPath $target -DesktopDirectories $desktopDirectories)) {
    throw 'simulated unexpected desktop shortcut could not be removed'
}
if (-not (Restore-GoNaviDesktopShortcutState -State $foreignState -OnlyForeign)) {
    throw 'foreign desktop shortcut restore reported failure'
}
$restoredForeignShortcut = $shell.CreateShortcut((Join-Path $desktop 'GoNavi.lnk'))
if (-not (Test-SameFilePath $restoredForeignShortcut.TargetPath $foreignTarget)) {
    throw 'foreign desktop shortcut was not restored after simulated MSI overwrite'
}
New-TestShortcut (Join-Path $commonDesktop 'GoNavi.lnk') $target ''
$matchingState = Save-GoNaviDesktopShortcutState -TargetPath $target -BackupDirectory (Join-Path $env:GONAVI_TEST_ROOT 'backup-matching') -DesktopDirectories $desktopDirectories
if (-not $matchingState.Succeeded -or $matchingState.InstallValue -ne '1' -or $matchingState.Entries.Count -ne 2) {
    throw 'matching desktop shortcut should be preserved'
}
Remove-Item -LiteralPath (Join-Path $commonDesktop 'GoNavi.lnk') -Force
if (-not (Restore-GoNaviDesktopShortcutState -State $matchingState)) {
    throw 'matching desktop shortcut restore reported failure'
}
$restoredMatchingShortcut = $shell.CreateShortcut((Join-Path $commonDesktop 'GoNavi.lnk'))
if (-not (Test-SameFilePath $restoredMatchingShortcut.TargetPath $target)) {
    throw 'matching desktop shortcut was not restored after simulated MSI failure'
}
`
	scriptPath := filepath.Join(tempDir, "shortcut-repair-test.ps1")
	if err := os.WriteFile(scriptPath, []byte(strings.ReplaceAll(harness, "\n", "\r\n")), 0o644); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(powerShell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "RemoteSigned", "-File", scriptPath)
	command.Env = append(os.Environ(),
		"GONAVI_TEST_TARGET="+targetPath,
		"GONAVI_TEST_FOREIGN_TARGET="+foreignTargetPath,
		"GONAVI_TEST_PINS="+pinsDirectory,
		"GONAVI_TEST_INSTALLER="+installerDirectory,
		"GONAVI_TEST_DESKTOP="+desktopDirectory,
		"GONAVI_TEST_COMMON_DESKTOP="+commonDesktopDirectory,
		"GONAVI_TEST_ROOT="+tempDir,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("shortcut repair integration failed: %v\n%s", err, output)
	}
}
