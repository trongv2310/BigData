import React, { useEffect, useState } from 'react';
import {
  Radio,
  Server,
  Layers,
  Activity,
  RefreshCw,
  AlertTriangle,
  ArrowDownRight,
  Cpu,
} from 'lucide-react';
import { api } from '../services/api';
import { useStream } from '../context/useStream';
import type { KafkaTopics } from '../types';

export const KafkaTab: React.FC = () => {
  const { latestKafkaStats } = useStream();
  const [data, setData] = useState<KafkaTopics | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);
      const res = await api.getKafkaTopics();
      setData(res);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(`Không thể tải thông tin Kafka: ${msg}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    let ignore = false;
    api.getKafkaTopics()
      .then((res) => {
        if (!ignore) {
          setData(res);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!ignore) {
          const msg = err instanceof Error ? err.message : String(err);
          setError(`Không thể tải thông tin Kafka: ${msg}`);
          setLoading(false);
        }
      });
    return () => {
      ignore = true;
    };
  }, []);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
            <Radio className="w-6 h-6 text-emerald-400" />
            Apache Kafka — Dòng Dữ liệu Thời gian thực
          </h2>
          <p className="text-sm text-slate-400">
            Giám sát Topics, Phân vùng (Partitions), Consumer Groups và Độ trễ (Lag)
          </p>
        </div>
        <button
          onClick={loadData}
          disabled={loading}
          className="inline-flex items-center gap-2 px-3.5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs sm:text-sm font-medium rounded-lg border border-slate-700 transition disabled:opacity-50 self-start sm:self-auto"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Làm mới Kafka
        </button>
      </div>

      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm flex items-start gap-3">
          <AlertTriangle className="w-5 h-5 shrink-0 text-rose-400 mt-0.5" />
          <div>{error}</div>
        </div>
      )}

      {/* Thông số Realtime từ Stream (kafka.stats event) */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="p-4 rounded-xl bg-slate-900 border border-slate-800">
          <div className="flex items-center justify-between text-slate-400 text-xs mb-1">
            <span>Kafka Broker</span>
            <Server className="w-4 h-4 text-emerald-400" />
          </div>
          <div className="text-base font-bold text-slate-100 font-mono truncate">
            {data?.bootstrapServers || 'localhost:9092'}
          </div>
          <div className="text-[10px] text-slate-500 mt-1">Bootstrap Servers</div>
        </div>

        <div className="p-4 rounded-xl bg-slate-900 border border-slate-800">
          <div className="flex items-center justify-between text-slate-400 text-xs mb-1">
            <span>Tốc độ Gửi Tin (Luồng)</span>
            <Activity className="w-4 h-4 text-sky-400 animate-pulse" />
          </div>
          <div className="text-2xl font-bold text-white font-mono">
            {latestKafkaStats ? `${latestKafkaStats.messagesPerSecond.toFixed(1)} msg/s` : '—'}
          </div>
          <div className="text-[10px] text-sky-400 font-mono mt-1">
            {latestKafkaStats ? `Topic: ${latestKafkaStats.topic}` : 'Chờ sự kiện stats...'}
          </div>
        </div>

        <div className="p-4 rounded-xl bg-slate-900 border border-slate-800">
          <div className="flex items-center justify-between text-slate-400 text-xs mb-1">
            <span>Tổng Độ trễ (Lag Total)</span>
            <Cpu className="w-4 h-4 text-amber-400" />
          </div>
          <div className="text-2xl font-bold font-mono text-emerald-400">
            {latestKafkaStats ? latestKafkaStats.lagTotal : '0'}
          </div>
          <div className="text-[10px] text-slate-500 mt-1">
            {latestKafkaStats?.lagTotal === 0 ? 'Xử lý kịp thời, không nghẽn' : 'Có độ trễ tích lũy'}
          </div>
        </div>
      </div>

      {/* Danh sách Topics */}
      <div className="rounded-2xl bg-slate-900 border border-slate-800 p-5 space-y-4">
        <div className="flex items-center gap-2">
          <Layers className="w-5 h-5 text-emerald-400" />
          <h3 className="text-base font-bold text-white">
            Danh sách Topics ({data?.topics?.length || 0})
          </h3>
        </div>

        {error ? (
          <div className="p-6 rounded-xl bg-slate-800/30 border border-dashed border-rose-500/30 text-center space-y-2">
            <AlertTriangle className="w-6 h-6 text-rose-400 mx-auto" />
            <p className="text-sm font-semibold text-rose-300">
              Không thể kết nối đến Apache Kafka
            </p>
            <p className="text-xs text-slate-400 max-w-md mx-auto">
              {error.includes('KAFKA_UNREACHABLE')
                ? 'Broker Kafka hiện chưa khởi động hoặc gặp sự cố kết nối (KAFKA_UNREACHABLE). Hãy khởi động Kafka để xem danh sách topics và thông số consumer groups.'
                : error}
            </p>
          </div>
        ) : !data || data.topics.length === 0 ? (
          <div className="p-6 rounded-xl bg-slate-800/20 border border-dashed border-slate-800 text-center text-xs text-slate-500">
            Chưa có topic nào trên cụm Kafka.
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-4">
            {data.topics.map((topic) => (
              <div
                key={topic.name}
                className="p-4 rounded-xl bg-slate-800/40 border border-slate-800 space-y-3"
              >

              <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800 pb-3">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-bold text-emerald-400 font-mono">
                    {topic.name}
                  </span>
                  <span className="px-2 py-0.5 rounded text-[10px] bg-slate-800 text-slate-300 font-mono">
                    {topic.partitions} partitions
                  </span>
                  <span className="px-2 py-0.5 rounded text-[10px] bg-slate-800 text-slate-300 font-mono">
                    rf: {topic.replicationFactor}
                  </span>
                </div>
                <div className="text-xs text-slate-400 font-mono">
                  Ước tính tin nhắn: <strong className="text-white">{topic.approxMessages ?? '—'}</strong>
                </div>
              </div>

              {/* Consumer Groups */}
              <div>
                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
                  Consumer Groups ({topic.consumerGroups.length})
                </h4>
                <div className="space-y-1.5">
                  {topic.consumerGroups.length === 0 ? (
                    <div className="text-xs text-slate-500 italic">Không có consumer group nào.</div>
                  ) : (
                    topic.consumerGroups.map((cg) => (
                      <div
                        key={cg.group}
                        className="flex items-center justify-between p-2 rounded bg-slate-900/60 text-xs font-mono"
                      >
                        <div className="flex items-center gap-2 text-slate-300">
                          <ArrowDownRight className="w-3.5 h-3.5 text-slate-500" />
                          <span>{cg.group}</span>
                        </div>
                        <div className="flex items-center gap-1.5">
                          <span className="text-slate-500">Lag:</span>
                          <span
                            className={`font-bold px-2 py-0.5 rounded text-[11px] ${
                              cg.lag === 0
                                ? 'bg-emerald-500/20 text-emerald-400'
                                : 'bg-amber-500/20 text-amber-400'
                            }`}
                          >
                            {cg.lag}
                          </span>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  </div>


  );
};
