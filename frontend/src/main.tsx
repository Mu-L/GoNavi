import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
// import './index.css' // Optional global styles

// 全局配置 Monaco Editor 使用本地打包的文件，避免从 CDN (jsdelivr) 加载。
// Windows WebView2 环境下访问外部 CDN 可能失败，导致编辑器一直显示 Loading。
import { loader } from '@monaco-editor/react'
import * as monaco from 'monaco-editor'
loader.config({ monaco })

if (typeof window !== 'undefined' && !(window as any).go) {
    (window as any).go = {
        app: {
            App: {
                CheckUpdate: async () => ({ success: false }),
                DownloadUpdate: async () => ({ success: false }),
                GetSavedConnections: async () => [],
                SaveConnection: async () => null,
                DeleteConnection: async () => null,
                OpenConnection: async () => null,
                CloseConnection: async () => null,
                GetDatabases: async () => [],
                GetTables: async () => [],
                GetTableData: async () => ({ columns: [], rows: [], total: 0 }),
                GetTableColumns: async () => [],
                ExecuteQuery: async () => ({ columns: [], rows: [], time: 0 }),
                GetSavedQueries: async () => [],
                SaveQuery: async () => null,
                DeleteQuery: async () => null,
                GetAppInfo: async () => ({}),
                CheckForUpdates: async () => ({ success: false }),
                OpenDownloadedUpdateDirectory: async () => ({ success: false }),
                InstallUpdateAndRestart: async () => ({ success: false }),
                ImportConfigFile: async () => ({ success: false }),
                ExportData: async () => ({ success: false }),
            }
        }
    };
}

// 全局注册透明主题，避免每个 Editor 组件 beforeMount 中重复定义
monaco.editor.defineTheme('transparent-dark', {
  base: 'vs-dark', inherit: true, rules: [],
  colors: { 'editor.background': '#00000000', 'editor.lineHighlightBackground': '#ffffff10', 'editorGutter.background': '#00000000', 'editorStickyScroll.background': '#1e1e1e', 'editorStickyScrollHover.background': '#2a2a2a' }
})
monaco.editor.defineTheme('transparent-light', {
  base: 'vs', inherit: true, rules: [],
  colors: { 'editor.background': '#00000000', 'editor.lineHighlightBackground': '#00000010', 'editorGutter.background': '#00000000', 'editorStickyScroll.background': '#ffffff', 'editorStickyScrollHover.background': '#f5f5f5' }
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
