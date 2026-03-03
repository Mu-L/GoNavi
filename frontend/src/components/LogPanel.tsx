import React, { useRef, useEffect } from 'react';
import { Table, Tag, Button, Tooltip } from 'antd';
import { ClearOutlined, CloseOutlined, CaretRightOutlined, BugOutlined } from '@ant-design/icons';
import { useStore } from '../store';
import { normalizeOpacityForPlatform } from '../utils/appearance';

interface LogPanelProps {
    height: number;
    onClose: () => void;
    onResizeStart: (e: React.MouseEvent) => void;
}

const LogPanel: React.FC<LogPanelProps> = ({ height, onClose, onResizeStart }) => {
    const sqlLogs = useStore(state => state.sqlLogs);
    const clearSqlLogs = useStore(state => state.clearSqlLogs);
    const theme = useStore(state => state.theme);
    const appearance = useStore(state => state.appearance);
    const darkMode = theme === 'dark';
    const opacity = normalizeOpacityForPlatform(appearance.opacity);

    // Background Helper
    const getBg = (darkHex: string) => {
        if (!darkMode) return `rgba(255, 255, 255, ${opacity})`;
        const hex = darkHex.replace('#', '');
        const r = parseInt(hex.substring(0, 2), 16);
        const g = parseInt(hex.substring(2, 4), 16);
        const b = parseInt(hex.substring(4, 6), 16);
        return `rgba(${r}, ${g}, ${b}, ${opacity})`;
    };
    const bgMain = getBg('#1d1d1d');
    const panelDividerColor = darkMode ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.08)';
    const panelMutedTextColor = darkMode ? 'rgba(255,255,255,0.62)' : 'rgba(0,0,0,0.58)';
    const logScrollbarThumb = darkMode ? 'rgba(255, 255, 255, 0.34)' : 'rgba(0, 0, 0, 0.26)';
    const logScrollbarThumbHover = darkMode ? 'rgba(255, 255, 255, 0.5)' : 'rgba(0, 0, 0, 0.36)';

    const columns = [
        {
            title: 'Time',
            dataIndex: 'timestamp',
            width: 80,
            render: (ts: number) => <span style={{ color: panelMutedTextColor, fontSize: '12px' }}>{new Date(ts).toLocaleTimeString()}</span>
        },
        {
            title: 'Status',
            dataIndex: 'status',
            width: 70,
            render: (status: string) => (
                <Tag color={status === 'success' ? 'success' : 'error'} style={{ marginRight: 0 }}>
                    {status === 'success' ? 'OK' : 'ERR'}
                </Tag>
            )
        },
        {
            title: 'Duration',
            dataIndex: 'duration',
            width: 70,
            render: (d: number) => <span style={{ color: d > 1000 ? 'orange' : 'inherit', fontSize: '12px' }}>{d}ms</span>
        },
        {
            title: 'SQL / Message',
            dataIndex: 'sql',
            render: (text: string, record: any) => (
                <div style={{ fontFamily: 'monospace', wordBreak: 'break-all', fontSize: '12px', lineHeight: '1.2' }}>
                    <div style={{ color: darkMode ? '#a6e22e' : '#005cc5' }}>{text}</div>
                    {record.message && <div style={{ color: '#ff4d4f', marginTop: 2 }}>{record.message}</div>}
                    {record.affectedRows !== undefined && <div style={{ color: panelMutedTextColor, marginTop: 1 }}>Affected: {record.affectedRows}</div>}
                </div>
            )
        }
    ];

    return (
        <div style={{ 
            height, 
            borderTop: `1px solid ${panelDividerColor}`,
            background: bgMain,
            display: 'flex',
            flexDirection: 'column',
            position: 'relative',
            zIndex: 100 // Ensure above other content
        }}>
            {/* Resize Handle */}
            <div 
                onMouseDown={onResizeStart}
                style={{
                    position: 'absolute',
                    top: -4,
                    left: 0,
                    right: 0,
                    height: 8,
                    cursor: 'row-resize',
                    zIndex: 10
                }}
            />

            {/* Toolbar */}
            <div style={{ 
                padding: '4px 8px', 
                borderBottom: `1px solid ${panelDividerColor}`,
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                height: 32
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontWeight: 'bold', fontSize: '12px' }}>
                    <BugOutlined /> SQL 执行日志
                </div>
                <div>
                    <Tooltip title="清空日志">
                        <Button type="text" size="small" icon={<ClearOutlined />} onClick={clearSqlLogs} />
                    </Tooltip>
                    <Tooltip title="关闭面板">
                        <Button type="text" size="small" icon={<CloseOutlined />} onClick={onClose} />
                    </Tooltip>
                </div>
            </div>

            {/* List */}
            <div className="log-panel-scroll" style={{ flex: 1, overflow: 'auto' }}>
                <Table 
                    className="log-panel-table"
                    dataSource={sqlLogs} 
                    columns={columns} 
                    size="small" 
                    pagination={false} 
                    rowKey="id"
                    showHeader={false}
                    // scroll={{ y: height - 32 }} // Let flex handle it
                />
            </div>
            <style>{`
                .log-panel-scroll {
                    scrollbar-width: thin;
                    scrollbar-color: ${logScrollbarThumb} transparent;
                }
                .log-panel-scroll::-webkit-scrollbar {
                    width: 10px;
                    height: 10px;
                }
                .log-panel-scroll::-webkit-scrollbar-track,
                .log-panel-scroll::-webkit-scrollbar-corner {
                    background: transparent;
                }
                .log-panel-scroll::-webkit-scrollbar-thumb {
                    background: ${logScrollbarThumb};
                    border-radius: 8px;
                    border: 2px solid transparent;
                    background-clip: padding-box;
                }
                .log-panel-scroll::-webkit-scrollbar-thumb:hover {
                    background: ${logScrollbarThumbHover};
                    background-clip: padding-box;
                }
                .log-panel-table .ant-table,
                .log-panel-table .ant-table-container,
                .log-panel-table .ant-table-tbody > tr > td {
                    background: transparent !important;
                }
            `}</style>
        </div>
    );
};

export default LogPanel;
