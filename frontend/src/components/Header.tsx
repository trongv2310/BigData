import React, { useState } from 'react';
import {
  Activity,
  Server,
  Database,
  Radio,
  Zap,
  Menu,
  X,
  Wifi,
  WifiOff,
  RefreshCw,
  GitCommit,
  Sparkles,
  Terminal,
  Layers,
} from 'lucide-react';
import { useStream } from '../context/useStream';

export type TabKey =
  | 'live'
  | 'pipeline'
  | 'overview'
  | 'hdfs'
  | 'kafka'
  | 'spark'
  | 'aqi'
  | 'flows'
  | 'sql'
  | 'predict'
  | 'terminal';

interface HeaderProps {
  currentTab: TabKey;
  onTabChange: (tab: TabKey) => void;
}

export const Header: React.FC<HeaderProps> = ({ currentTab, onTabChange }) => {
  const { status } = useStream();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const streamMode = import.meta.env.VITE_STREAM || 'replay';
  const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8010';

  const tabs: { key: TabKey; label: string; icon: React.FC<{ className?: string }> }[] = [
    { key: 'live', label: 'Dữ liệu sống', icon: Radio },
    { key: 'pipeline', label: 'Pipeline', icon: Layers },
    { key: 'overview', label: 'Tổng quan', icon: Activity },
    { key: 'hdfs', label: 'HDFS', icon: Database },
    { key: 'kafka', label: 'Kafka', icon: Radio },
    { key: 'spark', label: 'Spark', icon: Zap },
    { key: 'aqi', label: 'CO & AQI', icon: Server },
    { key: 'flows', label: 'Sơ đồ luồng', icon: GitCommit },
    { key: 'sql', label: 'SparkSQL', icon: Database },
    { key: 'predict', label: 'Dự đoán CO', icon: Sparkles },
    { key: 'terminal', label: 'Cửa sổ lệnh', icon: Terminal },
  ];

  return (
    <header className="bg-slate-900 border-b border-slate-800 sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Logo & Title */}
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-tr from-sky-500 to-indigo-600 flex items-center justify-center text-white font-bold shadow-lg shadow-sky-500/20">
              CO
            </div>
            <div>
              <h1 className="text-base sm:text-lg font-bold text-white tracking-tight leading-tight">
                Giám sát Khí CO TP.HCM
              </h1>
              <p className="text-xs text-slate-400 hidden sm:block">
                Bảng điều khiển Big Data thời gian thực • Cổng {apiBase.split(':').pop()}
              </p>
            </div>
          </div>

          {/* Desktop Navigation */}
          <nav className="hidden xl:flex items-center space-x-1">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = currentTab === tab.key;
              return (
                <button
                  key={tab.key}
                  onClick={() => onTabChange(tab.key)}
                  className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-all ${
                    isActive
                      ? 'bg-sky-500/20 text-sky-400 border border-sky-500/30 shadow-sm'
                      : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/60'
                  }`}
                >
                  <Icon className="w-3.5 h-3.5" />
                  {tab.label}
                </button>
              );
            })}
          </nav>

          {/* Right Status Badge */}
          <div className="hidden sm:flex items-center gap-3">
            <div
              className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${
                status === 'connected'
                  ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30'
                  : status === 'connecting'
                  ? 'bg-amber-500/10 text-amber-400 border-amber-500/30 animate-pulse'
                  : 'bg-rose-500/10 text-rose-400 border-rose-500/30'
              }`}
            >
              {status === 'connected' ? (
                <>
                  <Wifi className="w-3.5 h-3.5" />
                  <span>
                    {streamMode === 'replay' ? 'Luồng Replay (1s)' : 'WebSocket Kết nối'}
                  </span>
                </>
              ) : status === 'connecting' ? (
                <>
                  <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                  <span>Đang kết nối...</span>
                </>
              ) : (
                <>
                  <WifiOff className="w-3.5 h-3.5" />
                  <span>Mất kết nối</span>
                </>
              )}
            </div>
          </div>

          {/* Mobile/Tablet hamburger button */}
          <div className="flex xl:hidden items-center gap-2">
            <div
              className={`w-2.5 h-2.5 rounded-full ${
                status === 'connected'
                  ? 'bg-emerald-400 ring-4 ring-emerald-500/20'
                  : status === 'connecting'
                  ? 'bg-amber-400 animate-ping'
                  : 'bg-rose-400'
              }`}
            />
            <button
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="p-2 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 focus:outline-none"
              aria-label="Toggle menu"
            >
              {mobileMenuOpen ? <X className="w-6 h-6" /> : <Menu className="w-6 h-6" />}
            </button>
          </div>
        </div>
      </div>

      {/* Mobile Drawer Navigation */}
      {mobileMenuOpen && (
        <div className="xl:hidden border-t border-slate-800 bg-slate-900/95 px-4 pt-2 pb-4 space-y-1">
          <div className="py-2 px-3 mb-2 rounded bg-slate-800/60 text-xs text-slate-400 flex items-center justify-between">
            <span>Trạng thái luồng:</span>
            <span
              className={`font-semibold ${
                status === 'connected'
                  ? 'text-emerald-400'
                  : status === 'connecting'
                  ? 'text-amber-400'
                  : 'text-rose-400'
              }`}
            >
              {status === 'connected'
                ? streamMode === 'replay'
                  ? 'Đang phát lại (Replay)'
                  : 'Đã kết nối'
                : status === 'connecting'
                ? 'Đang kết nối...'
                : 'Mất kết nối'}
            </span>
          </div>
          {tabs.map((tab) => {
            const Icon = tab.icon;
            const isActive = currentTab === tab.key;
            return (
              <button
                key={tab.key}
                onClick={() => {
                  onTabChange(tab.key);
                  setMobileMenuOpen(false);
                }}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-sky-500/20 text-sky-400 border border-sky-500/30'
                    : 'text-slate-300 hover:bg-slate-800'
                }`}
              >
                <Icon className="w-4 h-4" />
                {tab.label}
              </button>
            );
          })}
        </div>
      )}
    </header>
  );
};
