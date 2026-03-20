//go:build darwin

package app

/*
#cgo CFLAGS: -x objective-c -fblocks
#cgo LDFLAGS: -framework Cocoa
#import <Cocoa/Cocoa.h>
#import <dispatch/dispatch.h>

static void gonaviSetWindowButtonsVisible(NSWindow *window, BOOL visible) {
	if (window == nil) {
		return;
	}
	for (NSWindowButton buttonType = NSWindowCloseButton; buttonType <= NSWindowZoomButton; buttonType++) {
		NSButton *button = [window standardWindowButton:buttonType];
		if (button != nil) {
			[button setHidden:!visible];
			[button setEnabled:visible];
		}
	}
}

static void gonaviApplyMacWindowStyle(BOOL enabled) {
	dispatch_async(dispatch_get_main_queue(), ^{
		for (NSWindow *window in [NSApp windows]) {
			if (window == nil) {
				continue;
			}

			NSUInteger styleMask = [window styleMask];
			styleMask |= NSWindowStyleMaskClosable;
			styleMask |= NSWindowStyleMaskMiniaturizable;
			styleMask |= NSWindowStyleMaskResizable;

			if (enabled) {
				styleMask |= NSWindowStyleMaskTitled;
				styleMask |= NSWindowStyleMaskFullSizeContentView;
				[window setStyleMask:styleMask];
				[window setTitleVisibility:NSWindowTitleHidden];
				[window setTitlebarAppearsTransparent:YES];
				[window setMovableByWindowBackground:YES];
				[window setCollectionBehavior:[window collectionBehavior] | NSWindowCollectionBehaviorFullScreenPrimary];
				gonaviSetWindowButtonsVisible(window, YES);
			} else {
				styleMask &= ~NSWindowStyleMaskTitled;
				styleMask &= ~NSWindowStyleMaskFullSizeContentView;
				[window setStyleMask:styleMask];
				[window setTitleVisibility:NSWindowTitleVisible];
				[window setTitlebarAppearsTransparent:NO];
				[window setMovableByWindowBackground:YES];
				gonaviSetWindowButtonsVisible(window, NO);
			}

			[[window contentView] setNeedsDisplay:YES];
			[window invalidateShadow];
		}
	});
}
*/
import "C"

func setMacNativeWindowControls(enabled bool) {
	state := resolveMacNativeWindowControlState(enabled)
	flag := C.BOOL(false)
	if state.ShowNativeButtons {
		flag = C.BOOL(true)
	}
	C.gonaviApplyMacWindowStyle(flag)
}
