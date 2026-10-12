import React, { useState } from 'react';
import {
  Database,
  Play,
  Clock,
  AlertTriangle,
  Info,
  CheckCircle,
  Code2,
  Table,
} from 'lucide-react';
import { api, ApiError } from '../services/api';
import type { SqlResponse } from '../types';

const SAMPLE_QUERIES = [
  {
    title: 'Trung bình CO theo Tháng',
    sql: 'SELECT year_vn, month_vn, round(avg(co), 2) as avg_co, count(*) as n FROM aqi GROUP BY year_vn, month_vn ORDER BY year_vn, month_vn',
  },
  {
    title: '20 Bản ghi Gần nhất',
    sql: 'SELECT datetime_vn, co, pm2_5, pm10, aqi_label FROM aqi ORDER BY datetime_utc DESC LIMIT 20',
  },
  {
    title: 'Trung bình CO theo Giờ (từ 2025)',
    sql: 'SELECT hour_vn, round(avg(co), 2) as avg_co, round(avg(temperature_C), 1) as avg_temp FROM aqi WHERE year_vn >= 2025 GROUP BY hour_vn ORDER BY hour_vn',
  },
];

const AQI_COLUMNS = [
  'datetime_utc',
  'datetime_vn',
  'hour_vn',
  'month_vn',
  'year_vn',
  'aqi_label',
  'co',
  'no',
  'no2',
  'o3',
  'so2',
  'pm2_5',
  'pm10',
  'nh3',
  'temperature_C',
  'humidity_pct',
  'rain_mm',
  'wind_speed_kmh',
  'is_event_anomaly',
  'o3_was_missing',
];

export const SqlTab: React.FC = () => {
  const [query, setQuery] = useState<string>(
    'SELECT year_vn, month_vn, round(avg(co), 2) as avg_co, count(*) as n FROM aqi GROUP BY year_vn, month_vn ORDER BY year_vn, month_vn'
  );
  const [limit, setLimit] = useState<number>(100);
  const [loading, setLoading] = useState<boolean>(false);
  const [result, setResult] = useState<SqlResponse | null>(null);
  const [error, setError] = useState<{ code?: string; message: string; status?: number } | null>(
    null
  );

  const handleExecute = async () => {
    if (!query.trim()) return;

    try {
      setLoading(true);
      setError(null);
      const res = await api.runSql({
        query: query.trim(),
        limit,
      });
      setResult(res);
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        setError({
          code: err.code,
          message: err.message,
          status: err.status,
        });
      } else {
        const msg = err instanceof Error ? err.message : String(err);
        setError({
          message: msg,
        });
      }
      setResult(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
            <Database className="w-6 h-6 text-amber-400" />
            SparkSQL — Truy vấn Dữ liệu Lớn Trực tiếp
          </h2>
          <p className="text-sm text-slate-400">
            Truy vấn bảng <code>aqi</code> đọc từ HDFS <code>/raw/Dataset.csv</code> thông qua Apache Spark engine
          </p>
        </div>
      </div>

      {/* Editor Box */}
      <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 space-y-4 shadow-xl">
        {/* Sample queries buttons */}
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs text-slate-400 flex items-center gap-1">
            <Code2 className="w-3.5 h-3.5 text-slate-500" />
            Mẫu truy vấn:
          </span>
          {SAMPLE_QUERIES.map((sample, idx) => (
            <button
              key={idx}
              onClick={() => setQuery(sample.sql)}
              className="text-xs px-2.5 py-1 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 transition"
            >
              {sample.title}
            </button>
          ))}
        </div>

        {/* Textarea */}
        <div className="relative">
          <textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            rows={5}
            placeholder="Nhập câu lệnh SELECT... (Ví dụ: SELECT * FROM aqi LIMIT 10)"
            className="w-full font-mono text-sm bg-slate-950 text-slate-100 p-4 rounded-xl border border-slate-700/80 focus:border-amber-500/80 focus:ring-1 focus:ring-amber-500/80 outline-none resize-y"
          />
        </div>

        {/* Control bar */}
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pt-1">
          <div className="flex items-center gap-4 text-xs">
            <div className="flex items-center gap-2">
              <label htmlFor="limit-input" className="text-slate-400 font-medium">
                Giới hạn dòng (LIMIT):
              </label>
              <input
                id="limit-input"
                type="number"
                min={1}
                max={1000}
                value={limit}
                onChange={(e) => setLimit(Math.max(1, Math.min(1000, Number(e.target.value))))}
                className="w-20 px-2.5 py-1 bg-slate-800 border border-slate-700 rounded text-slate-200 font-mono text-xs focus:outline-none focus:border-amber-500"
              />
            </div>
            <span className="text-slate-500 hidden sm:inline">|</span>
            <span className="text-slate-400 text-[11px] hidden sm:inline">
              Chỉ chấp nhận lệnh SELECT / WITH...SELECT chỉ đọc
            </span>
          </div>

          <button
            onClick={handleExecute}
            disabled={loading || !query.trim()}
            className="inline-flex items-center justify-center gap-2 px-5 py-2.5 bg-amber-500 hover:bg-amber-400 text-slate-950 font-bold text-sm rounded-xl transition shadow-lg shadow-amber-500/20 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? (
              <>
                <Clock className="w-4 h-4 animate-spin" />
                Đang thực thi Spark...
              </>
            ) : (
              <>
                <Play className="w-4 h-4 fill-slate-950" />
                Chạy truy vấn SQL
              </>
            )}
          </button>
        </div>

        {/* Notice on first Spark query cold start */}
        <div className="flex items-center gap-2 text-[11px] text-slate-400 pt-1">
          <Info className="w-4 h-4 text-slate-500 shrink-0" />
          <span>
            Lần chạy đầu tiên có thể mất ~8–10 giây để khởi động Spark JVM Session. Timeout được cấu hình ≥ 60 giây.
          </span>
        </div>
      </div>

      {/* Error alert */}
      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-xs sm:text-sm space-y-1">
          <div className="flex items-center gap-2 font-bold text-rose-400">
            <AlertTriangle className="w-4 h-4 shrink-0" />
            <span>
              Lỗi Thực thi SQL
              {error.code ? ` [${error.code}]` : ''}
              {error.status ? ` (Mã ${error.status})` : ''}
            </span>
          </div>
          <p className="font-mono text-slate-200 pl-6">{error.message}</p>
          {error.code === 'SQL_NOT_ALLOWED' && (
            <p className="text-[11px] text-slate-400 pl-6">
              * Lưu ý: Hệ thống chỉ cho phép các câu lệnh SELECT đơn lẻ trên bảng <code>aqi</code>. Không cho phép dấu chấm phẩy (;), lệnh ghi (INSERT, DROP...), hoặc hàm reflect.
            </p>
          )}
          {error.code === 'SQL_TIMEOUT' && (
            <p className="text-[11px] text-slate-400 pl-6">
              * Truy vấn vượt quá thời gian cho phép (60 giây). Vui lòng tối ưu điều kiện lọc hoặc giảm khối lượng tính toán.
            </p>
          )}
          {error.code === 'SQL_UNAVAILABLE' && (
            <p className="text-[11px] text-slate-400 pl-6">
              * Không thể kết nối với Spark hoặc HDFS. Vui lòng kiểm tra dịch vụ Hadoop/Spark trong WSL Ubuntu.
            </p>
          )}
        </div>
      )}

      {/* Results Section */}
      {result && (
        <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 space-y-4 shadow-xl">
          {/* Metadata Bar */}
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 pb-3">
            <div className="flex items-center gap-3">
              <span className="flex items-center gap-1.5 text-xs text-emerald-400 font-medium">
                <CheckCircle className="w-4 h-4 text-emerald-400" />
                Truy vấn thành công
              </span>
              <span className="text-xs font-mono text-slate-400">
                Số dòng: <strong className="text-white">{result.rowCount}</strong>
              </span>
              <span className="text-xs font-mono text-slate-400">
                Thời gian: <strong className="text-amber-400">{result.elapsedMs} ms</strong>
              </span>
            </div>

            {result.truncated && (
              <span className="px-2 py-0.5 rounded text-[10px] font-mono uppercase bg-amber-500/20 text-amber-400 border border-amber-500/30">
                Đã cắt ngắn kết quả (Truncated)
              </span>
            )}
          </div>

          {/* Data Table */}
          {result.rows.length > 0 ? (
            <div className="overflow-x-auto rounded-xl border border-slate-800 max-h-[500px]">
              <table className="w-full text-left text-xs">
                <thead className="bg-slate-950 text-slate-300 font-mono uppercase text-[11px] border-b border-slate-800 sticky top-0 z-10 shadow-sm">
                  <tr>
                    <th className="px-3 py-2.5 text-slate-500 w-12 text-center">#</th>
                    {result.columns.map((col, idx) => (
                      <th key={idx} className="px-4 py-2.5">
                        <div className="font-bold text-slate-200">{col.name}</div>
                        <div className="text-[10px] text-slate-500 font-normal lowercase">
                          {col.type}
                        </div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 bg-slate-900/40 font-mono text-slate-300">
                  {result.rows.map((row, rIdx) => (
                    <tr key={rIdx} className="hover:bg-slate-800/30">
                      <td className="px-3 py-2 text-slate-500 text-center">{rIdx + 1}</td>
                      {row.map((cell, cIdx) => (
                        <td key={cIdx} className="px-4 py-2 whitespace-nowrap">
                          {cell === null ? (
                            <span className="text-slate-600 italic">null</span>
                          ) : typeof cell === 'boolean' ? (
                            <span className={cell ? 'text-emerald-400' : 'text-rose-400'}>
                              {String(cell)}
                            </span>
                          ) : typeof cell === 'number' ? (
                            <span className="text-sky-300">{cell}</span>
                          ) : (
                            <span className="text-slate-200">{String(cell)}</span>
                          )}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="p-8 text-center text-xs text-slate-500">
              Truy vấn trả về 0 dòng kết quả
            </div>
          )}
        </div>
      )}

      {/* Schema reference card */}
      <div className="p-5 rounded-2xl bg-slate-900/60 border border-slate-800/80 space-y-3">
        <h4 className="text-xs font-bold text-slate-300 flex items-center gap-2 uppercase tracking-wider">
          <Table className="w-4 h-4 text-amber-400" />
          Bảng <code>aqi</code> & Danh sách các cột khả dụng
        </h4>
        <p className="text-xs text-slate-400">
          Tập dữ liệu chất lượng không khí TP.HCM sau tiền xử lý (35 064 dòng từ 2022-04 đến 2026-04). Các cột ngày giờ đã được chuyển sang múi giờ Việt Nam (UTC+7):
        </p>
        <div className="flex flex-wrap gap-1.5 pt-1">
          {AQI_COLUMNS.map((col) => (
            <code
              key={col}
              onClick={() => {
                if (!query.includes(col)) {
                  setQuery((prev) => `${prev} ${col}`);
                }
              }}
              title="Nhấn để chèn vào câu truy vấn"
              className="text-xs px-2 py-1 rounded bg-slate-800 text-slate-300 font-mono border border-slate-700/80 hover:bg-slate-700 hover:text-amber-300 cursor-pointer transition"
            >
              {col}
            </code>
          ))}
        </div>
      </div>
    </div>
  );
};
