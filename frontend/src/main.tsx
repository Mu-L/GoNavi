import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
// import './index.css' // Optional global styles

// 全局配置 Monaco Editor 使用本地打包的文件，避免从 CDN (jsdelivr) 加载。
// Windows WebView2 环境下访问外部 CDN 可能失败，导致编辑器一直显示 Loading。
import { loader } from '@monaco-editor/react'
import * as monaco from 'monaco-editor'
loader.config({ monaco })

// 全局注册透明主题，避免每个 Editor 组件 beforeMount 中重复定义
monaco.editor.defineTheme('transparent-dark', {
  base: 'vs-dark', inherit: true, rules: [],
  colors: { 'editor.background': '#00000000', 'editor.lineHighlightBackground': '#ffffff10', 'editorGutter.background': '#00000000' }
})
monaco.editor.defineTheme('transparent-light', {
  base: 'vs', inherit: true, rules: [],
  colors: { 'editor.background': '#00000000', 'editor.lineHighlightBackground': '#00000010', 'editorGutter.background': '#00000000' }
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
