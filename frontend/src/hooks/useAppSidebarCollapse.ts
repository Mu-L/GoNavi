import { useCallback, useLayoutEffect, useRef, useState } from 'react';

type PendingSidebarToggleFocus = 'collapsed' | 'explorer' | null;

/**
 * Owns the explorer collapse state, the titlebar host for docked collapsed
 * actions and the focus hand-off between the two toggle buttons.
 */
export const useAppSidebarCollapse = (shouldDockCollapsedSidebarActionsInTitlebar: boolean) => {
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  const [collapsedSidebarActionsTarget, setCollapsedSidebarActionsTarget] = useState<HTMLDivElement | null>(null);
  const sidebarContentRef = useRef<HTMLDivElement>(null);
  const sidebarCollapsedToggleRef = useRef<HTMLButtonElement>(null);
  const sidebarExplorerToggleRef = useRef<HTMLButtonElement>(null);
  const pendingSidebarToggleFocusRef = useRef<PendingSidebarToggleFocus>(null);
  const isSidebarCollapsedRef = useRef(isSidebarCollapsed);
  isSidebarCollapsedRef.current = isSidebarCollapsed;
  const isCollapsedSidebarActionsDocked = isSidebarCollapsed && shouldDockCollapsedSidebarActionsInTitlebar;

  useLayoutEffect(() => {
    const sidebarContent = sidebarContentRef.current;
    if (!sidebarContent) return;
    // aria-hidden alone does not remove focusable tree wrappers from the tab order.
    sidebarContent.inert = isCollapsedSidebarActionsDocked;
  }, [isCollapsedSidebarActionsDocked]);

  const handleCollapseSidebarPanel = useCallback(() => {
    if (typeof document !== 'undefined') {
      const activeElement = document.activeElement as HTMLElement | null;
      if (activeElement?.closest?.('[data-sidebar-content="true"]')) {
        activeElement.blur();
      }
    }
    pendingSidebarToggleFocusRef.current = 'collapsed';
    setIsSidebarCollapsed(true);
  }, []);

  const handleExpandSidebarPanel = useCallback(() => {
    pendingSidebarToggleFocusRef.current = 'explorer';
    setIsSidebarCollapsed(false);
  }, []);

  // Stable identity keeps the memoized explorer from re-rendering on every toggle.
  const handleEnsureSidebarExpanded = useCallback(() => {
    if (isSidebarCollapsedRef.current) handleExpandSidebarPanel();
  }, [handleExpandSidebarPanel]);

  useLayoutEffect(() => {
    const target = pendingSidebarToggleFocusRef.current;
    if (!target) return;
    if (
      target === 'collapsed'
      && isCollapsedSidebarActionsDocked
      && !collapsedSidebarActionsTarget
    ) return;
    pendingSidebarToggleFocusRef.current = null;
    // The collapsed explorer keeps its expanded width inside a clipping parent;
    // scrolling the focused toggle into view would shift that parent sideways.
    (target === 'collapsed' ? sidebarCollapsedToggleRef : sidebarExplorerToggleRef).current?.focus({ preventScroll: true });
  }, [collapsedSidebarActionsTarget, isCollapsedSidebarActionsDocked, isSidebarCollapsed]);

  return {
    collapsedSidebarActionsTarget,
    handleCollapseSidebarPanel,
    handleEnsureSidebarExpanded,
    handleExpandSidebarPanel,
    isCollapsedSidebarActionsDocked,
    isSidebarCollapsed,
    setCollapsedSidebarActionsTarget,
    setIsSidebarCollapsed,
    sidebarCollapsedToggleRef,
    sidebarContentRef,
    sidebarExplorerToggleRef,
  };
};
