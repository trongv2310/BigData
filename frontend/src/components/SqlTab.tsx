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
    title: 'Bảng aqi: Trung bình CO theo Tháng',
    sql: 'SELECT year_vn, month_vn, round(avg(co), 2) as avg_co, count(*) as n FROM aqi GROUP BY year_vn, month_vn ORDER BY year_vn, month_vn',
  },
  {
    title: 'Bảng aqi: 20 Bản ghi Gần nhất',
    sql: 'SELECT datetime_vn, co, pm2_5, pm10, aqi_label FROM aqi ORDER BY datetime_utc DESC LIMIT 20',
  },
  {
    title: 'Bảng aqi: Trung bình CO theo Giờ (từ 2025)',
    sql: 'SELECT hour_vn, round(avg(co), 2) as avg_co, round(avg(temperature_C), 1) as avg_temp FROM aqi WHERE year_vn >= 2025 GROUP BY hour_vn ORDER BY hour_vn',
  },
  {
    title: 'Bảng live: 10 Bản ghi Mới nhất',
    sql: 'SELECT datetime_vn, co, wind_speed_kmh, temperature_C, canh_bao, kafka_offset FROM live ORDER BY datetime_vn DESC LIMIT 10',
  },
  {
    title: 'Bảng live: Thống kê CO theo Trạm',
    sql: 'SELECT station, round(avg(co), 2) as avg_co, count(*) as n, min(co) as min_co, max(co) as max_co FROM live GROUP BY station',
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

const LIVE_COLUMNS = [
  'datetime_vn',
  'station',
  'co',
  'wind_speed_kmh',
  'temperature_C',
  'co_accumulation_index',
  'canh_bao',
  'co_observed_at',
  'weather_observed_at',
  'provider',
  'kafka_partition',
  'kafka_offset',
  'kafka_timestamp',
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
            Truy vấn bảng <code>aqi</code> (bộ Kaggle trên HDFS) và bảng <code>live</code> (luồng Spark Kafka Streaming trên HDFS) thông qua Apache Spark engine
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
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-xs sm:text-sm space-y-2">
          <div className="flex items-center gap-2 font-bold text-rose-400">
            <AlertTriangle className="w-4 h-4 shrink-0" />
            <span>
              Lỗi Thực thi SQL
              {error.code ? ` [${error.code}]` : ''}
              {error.status ? ` (Mã ${error.status})` : ''}
            </span>
          </div>
          <p className="font-mono text-slate-200 pl-6">{error.message}</p>
          {error.code === 'SQL_ERROR' && (
            <div className="ml-6 p-3 rounded-lg bg-slate-950/70 border border-amber-500/20 text-xs space-y-1.5 text-slate-300">
              <div className="font-semibold text-amber-300 flex items-center gap-1.5">
                <Info className="w-3.5 h-3.5 text-amber-400" />
                Giải thích lỗi câu lệnh:
              </div>
              {error.message.toLowerCase().includes('live') || query.toLowerCase().includes('live') ? (
                <p className="text-slate-300 leading-relaxed">
                  Nếu bạn đang truy vấn bảng <code>live</code>: Bảng này chỉ tồn tại sau khi Spark Streaming hoàn tất ít nhất một micro-batch ghi vào HDFS <code>/live/co_stream</code>. Nếu luồng live chưa phát dữ liệu hoặc Spark vừa khởi động lại, lỗi <code>Table or view not found: live</code> là hoàn toàn bình thường.
                </p>
              ) : (
                <p className="text-slate-300 leading-relaxed">
                  Vui lòng kiểm tra cú pháp câu lệnh SQL hoặc tên các cột trong bảng được liệt kê ở phần cấu trúc schema bên dưới.
                </p>
              )}
            </div>
          )}
          {error.code === 'SQL_NOT_ALLOWED' && (
            <p className="text-[11px] text-slate-400 pl-6">
              * Lưu ý: Hệ thống chỉ cho phép các câu lệnh SELECT đơn lẻ trên bảng <code>aqi</code> hoặc <code>live</code>. Không cho phép dấu chấm phẩy (;), lệnh ghi (INSERT, DROP...), hoặc hàm reflect.
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

      {/* Important Data Source Warning */}
      <div className="p-4 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-300 text-xs sm:text-sm space-y-1.5">
        <div className="flex items-center gap-2 font-bold text-amber-400">
          <AlertTriangle className="w-4 h-4 shrink-0" />
          <span>Quy ước Nguồn Dữ liệu: Không trộn bảng live và bảng aqi</span>
        </div>
        <p className="text-slate-300 pl-6 leading-relaxed">
          Bảng <code>aqi</code> là tập dữ liệu lịch sử OpenWeather (Kaggle), giá trị trung bình sau điểm gãy 01/07/2025 là ~300 µg/m³. Bảng <code>live</code> thu thập trực tiếp từ mô hình khí quyển Open-Meteo (CAMS), giá trị đo quan sát thực tế dao động 1678–2508 µg/m³ (ngày 12–13/10/2026). Hai nguồn sử dụng phương pháp mô hình và chuẩn gốc khác nhau hoàn toàn, tuyệt đối <strong>không trộn (JOIN / UNION)</strong> vào cùng một chuỗi phân tích.
        </p>
      </div>

      {/* Schema reference cards */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Table aqi */}
        <div className="p-5 rounded-2xl bg-slate-900/60 border border-slate-800/80 space-y-3">
          <div className="flex items-center justify-between">
            <h4 className="text-xs font-bold text-slate-300 flex items-center gap-2 uppercase tracking-wider">
              <Table className="w-4 h-4 text-amber-400" />
              Bảng <code>aqi</code> (Lịch sử Kaggle)
            </h4>
            <span className="text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 font-mono">
              35 064 dòng • HDFS /raw
            </span>
          </div>
          <p className="text-xs text-slate-400">
            Dữ liệu chất lượng không khí TP.HCM sau tiền xử lý (2022-04 đến 2026-04). Giờ đã chuyển múi giờ Việt Nam (UTC+7):
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

        {/* Table live */}
        <div className="p-5 rounded-2xl bg-slate-900/60 border border-slate-800/80 space-y-3">
          <div className="flex items-center justify-between">
            <h4 className="text-xs font-bold text-slate-300 flex items-center gap-2 uppercase tracking-wider">
              <Table className="w-4 h-4 text-emerald-400" />
              Bảng <code>live</code> (Kafka Streaming)
            </h4>
            <span className="text-[10px] px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 font-mono">
              Parquet • HDFS /live/co_stream
            </span>
          </div>
          <p className="text-xs text-slate-400">
            Dữ liệu sống Spark Structured Streaming nạp từ topic Kafka <code>hcmc_co_stream</code> (Open-Meteo). Bảng dựng lại mỗi truy vấn:
          </p>
          <div className="flex flex-wrap gap-1.5 pt-1">
            {LIVE_COLUMNS.map((col) => (
              <code
                key={col}
                onClick={() => {
                  if (!query.includes(col)) {
                    setQuery((prev) => `${prev} ${col}`);
                  }
                }}
                title="Nhấn để chèn vào câu truy vấn"
                className="text-xs px-2 py-1 rounded bg-slate-800 text-slate-300 font-mono border border-slate-700/80 hover:bg-slate-700 hover:text-emerald-300 cursor-pointer transition"
              >
                {col}
              </code>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};
