import React, { useEffect, useState } from 'react';
import {
  Zap,
  PlayCircle,
  CheckCircle,
  XCircle,
  ExternalLink,
  RefreshCw,
  AlertTriangle,
  Activity,
  Layers,
} from 'lucide-react';
import { api } from '../services/api';
import { useStream } from '../context/useStream';
import type { SparkApplications } from '../types';
import { formatDateTime } from '../utils/format';

export const SparkTab: React.FC = () => {
  const { latestSparkProgress } = useStream();
  const [data, setData] = useState<SparkApplications | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);
      const res = await api.getSparkApplications();
      setData(res);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(`Không thể tải thông tin Spark: ${msg}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    let ignore = false;
    api.getSparkApplications()
      .then((res) => {
        if (!ignore) {
          setData(res);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!ignore) {
          const msg = err instanceof Error ? err.message : String(err);
          setError(`Không thể tải thông tin Spark: ${msg}`);
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
            <Zap className="w-6 h-6 text-amber-400" />
            Apache Spark — Công cụ Xử lý Dữ liệu Lớn
          </h2>
          <p className="text-sm text-slate-400">
            Giám sát Ứng dụng Spark, Tác vụ (Jobs) và Structured Streaming Micro-batches
          </p>
        </div>
        <button
          onClick={loadData}
          disabled={loading}
          className="inline-flex items-center gap-2 px-3.5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs sm:text-sm font-medium rounded-lg border border-slate-700 transition disabled:opacity-50 self-start sm:self-auto"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Làm mới Spark
        </button>
      </div>

      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm flex items-start gap-3">
          <AlertTriangle className="w-5 h-5 shrink-0 text-rose-400 mt-0.5" />
          <div>{error}</div>
        </div>
      )}

      {/* Thẻ Structured Streaming Realtime (từ spark.progress event) */}
      <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 space-y-4 shadow-xl">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Activity className="w-5 h-5 text-amber-400" />
            <h3 className="text-base font-bold text-white">
              Spark Structured Streaming — Tiến trình Micro-batch
            </h3>
          </div>
          <span className="text-xs font-mono text-slate-400">
            {latestSparkProgress ? `Tên ứng dụng (Query): ${latestSparkProgress.queryName}` : 'Chưa có sự kiện spark.progress'}
          </span>
        </div>

        {latestSparkProgress ? (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div className="p-3 bg-slate-800/40 rounded-xl border border-slate-800">
              <span className="text-xs text-slate-400 block mb-1">Batch ID Hiện tại</span>
              <span className="text-xl font-bold text-amber-400 font-mono">
                #{latestSparkProgress.batchId}
              </span>
            </div>

            <div className="p-3 bg-slate-800/40 rounded-xl border border-slate-800">
              <span className="text-xs text-slate-400 block mb-1">Số dòng nhập (Rows)</span>
              <span className="text-xl font-bold text-white font-mono">
                {latestSparkProgress.numInputRows}
              </span>
            </div>

            <div className="p-3 bg-slate-800/40 rounded-xl border border-slate-800">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-slate-400">Tốc độ nạp vào</span>
                <span className="text-[10px] px-1.5 py-0.2 rounded bg-slate-800 text-slate-400 border border-slate-700">ước tính</span>
              </div>
              <span className="text-xl font-bold text-sky-400 font-mono">
                {latestSparkProgress.inputRowsPerSecond.toFixed(1)} rows/s
              </span>
            </div>

            <div className="p-3 bg-slate-800/40 rounded-xl border border-slate-800">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-slate-400">Tốc độ xử lý</span>
                <span className="text-[10px] px-1.5 py-0.2 rounded bg-slate-800 text-slate-400 border border-slate-700">ước tính</span>
              </div>
              <span className="text-xl font-bold text-emerald-400 font-mono">
                {latestSparkProgress.processedRowsPerSecond.toFixed(1)} rows/s
              </span>
            </div>
          </div>
        ) : (
          <div className="p-6 rounded-xl bg-slate-800/20 border border-dashed border-slate-800 text-center space-y-1">
            <p className="text-sm font-medium text-slate-300">Chưa có dữ liệu tiến trình Streaming</p>
            <p className="text-xs text-slate-500">
              Luồng thời gian thực hiện chưa phát sự kiện <code>spark.progress</code> (chỉ phát khi có ứng dụng Structured Streaming đang chạy và phát tiến trình).
            </p>
          </div>
        )}
      </div>


      {/* Danh sách Ứng dụng Spark */}
      <div className="rounded-2xl bg-slate-900 border border-slate-800 p-5 space-y-4">
        <div className="flex items-center gap-2">
          <Layers className="w-5 h-5 text-indigo-400" />
          <h3 className="text-base font-bold text-white">
            Danh sách Ứng dụng Spark ({data?.applications.length || 0})
          </h3>
        </div>

        <div className="grid grid-cols-1 gap-4">
          {data?.applications.map((app) => (
            <div
              key={app.id}
              className="p-4 rounded-xl bg-slate-800/40 border border-slate-800 flex flex-col md:flex-row md:items-center justify-between gap-4"
            >
              <div className="space-y-1">
                <div className="flex flex-wrap items-center gap-2">
                  <span
                    className={`inline-flex items-center gap-1 px-2.5 py-0.5 rounded text-xs font-bold font-mono uppercase ${
                      app.state === 'running'
                        ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                        : app.state === 'completed'
                        ? 'bg-sky-500/20 text-sky-400 border border-sky-500/30'
                        : 'bg-rose-500/20 text-rose-400 border border-rose-500/30'
                    }`}
                  >
                    {app.state === 'running' && (
                      <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                    )}
                    {app.state}
                  </span>
                  <h4 className="text-base font-bold text-white font-mono">{app.name}</h4>
                </div>
                <div className="text-xs text-slate-400 font-mono">
                  ID: <span className="text-slate-300">{app.id}</span> • Bắt đầu lúc:{' '}
                  {formatDateTime(app.startedAt)}
                </div>
              </div>

              {/* Jobs Statistics */}
              <div className="flex items-center gap-4 text-xs font-mono">
                <div className="flex items-center gap-1.5">
                  <PlayCircle className="w-4 h-4 text-amber-400" />
                  <span className="text-slate-400">Đang chạy:</span>
                  <span className="font-bold text-white">{app.jobsRunning}</span>
                </div>

                <div className="flex items-center gap-1.5">
                  <CheckCircle className="w-4 h-4 text-emerald-400" />
                  <span className="text-slate-400">Thành công:</span>
                  <span className="font-bold text-emerald-400">{app.jobsSucceeded}</span>
                </div>

                <div className="flex items-center gap-1.5">
                  <XCircle className="w-4 h-4 text-rose-400" />
                  <span className="text-slate-400">Thất bại:</span>
                  <span className="font-bold text-rose-400">{app.jobsFailed}</span>
                </div>

                {app.uiUrl && (
                  <a
                    href={app.uiUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="ml-2 inline-flex items-center gap-1 px-3 py-1.5 rounded bg-sky-500/10 text-sky-400 hover:bg-sky-500/20 border border-sky-500/30 transition"
                  >
                    Spark UI <ExternalLink className="w-3.5 h-3.5" />
                  </a>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};
