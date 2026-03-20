package app

type macNativeWindowControlState struct {
	ShowNativeButtons     bool
	UseTitledWindow       bool
	UseFullSizeContent    bool
	HideWindowTitle       bool
	TransparentTitlebar   bool
	AllowNativeFullscreen bool
}

func resolveMacNativeWindowControlState(enabled bool) macNativeWindowControlState {
	if enabled {
		return macNativeWindowControlState{
			ShowNativeButtons:     true,
			UseTitledWindow:       true,
			UseFullSizeContent:    true,
			HideWindowTitle:       true,
			TransparentTitlebar:   true,
			AllowNativeFullscreen: true,
		}
	}

	return macNativeWindowControlState{
		ShowNativeButtons:     false,
		UseTitledWindow:       false,
		UseFullSizeContent:    false,
		HideWindowTitle:       false,
		TransparentTitlebar:   false,
		AllowNativeFullscreen: false,
	}
}
