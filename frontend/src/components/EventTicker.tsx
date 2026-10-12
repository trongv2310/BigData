import React from 'react';
import { useStream } from '../context/useStream';
import { formatTimeOnly } from '../utils/format';
import { Radio, Database, Zap, Cpu, Bell } from 'lucide-react';

export const EventTicker: React.FC = () => {
  const { events } = useStream();
  const latestEvent = events[0];

  if (!latestEvent) {
    return (
      <div className="bg-slate-900/60 border-b border-slate-800/80 px-4 py-2 text-xs text-slate-400 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-slate-600 animate-pulse" />
          <span>Đang đợi sự kiện từ luồng thời gian thực...</span>
        </div>
      </div>
    );
  }

  const getEventBadge = () => {
    switch (latestEvent.type) {
      case 'reading':
        return {
          icon: Radio,
          label: 'ĐO ĐẠC MỚI',
          color: 'text-sky-400 bg-sky-500/10 border-sky-500/30',
          desc: `Trạm: ${latestEvent.data.station} • CO: ${latestEvent.data.co} µg/m³ • ${latestEvent.data.status}`,
        };
      case 'hdfs.op':
        return {
          icon: Database,
          label: 'HDFS OP',
          color: 'text-amber-400 bg-amber-500/10 border-amber-500/30',
          desc: `Thao tác: ${latestEvent.data.op} • Đường dẫn: ${latestEvent.data.path}`,
        };
      case 'spark.progress':
        return {
          icon: Zap,
          label: 'SPARK PROGRESS',
          color: 'text-purple-400 bg-purple-500/10 border-purple-500/30',
          desc: `Batch: #${latestEvent.data.batchId} • Số dòng: ${latestEvent.data.numInputRows} • Tốc độ: ${latestEvent.data.processedRowsPerSecond} rows/s`,
        };
      case 'kafka.stats':
        return {
          icon: Cpu,
          label: 'KAFKA STATS',
          color: 'text-emerald-400 bg-emerald-500/10 border-emerald-500/30',
          desc: `Topic: ${latestEvent.data.topic} • ${latestEvent.data.messagesPerSecond} msg/s • Lag: ${latestEvent.data.lagTotal}`,
        };
      case 'service.state':
        return {
          icon: Bell,
          label: 'DỊCH VỤ',
          color: 'text-blue-400 bg-blue-500/10 border-blue-500/30',
          desc: `${latestEvent.data.name} chuyển trạng thái -> ${latestEvent.data.state.toUpperCase()}`,
        };
      default:
        return {
          icon: Bell,
          label: 'SỰ KIỆN',
          color: 'text-slate-400 bg-slate-500/10 border-slate-500/30',
          desc: JSON.stringify(latestEvent.data),
        };
    }
  };

  const badge = getEventBadge();
  const Icon = badge.icon;

  return (
    <div className="bg-slate-900/80 border-b border-slate-800 px-4 py-2 text-xs flex flex-wrap items-center justify-between gap-2">
      <div className="flex items-center gap-2.5 overflow-hidden">
        <span className="flex items-center gap-1 font-mono text-slate-500">
          [{formatTimeOnly(latestEvent.ts)}]
        </span>
        <span
          className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-semibold border ${badge.color}`}
        >
          <Icon className="w-3 h-3" />
          {badge.label}
        </span>
        <span className="text-slate-300 truncate font-mono">{badge.desc}</span>
      </div>
      <div className="text-slate-500 text-[11px] font-mono shrink-0">
        seq: #{latestEvent.seq}
      </div>
    </div>
  );
};
