import React, { useState } from 'react';
import {
  Sparkles,
  AlertTriangle,
  Info,
  Clock,
  Send,
  HelpCircle,
  Thermometer,
  Wind,
  Calendar,
} from 'lucide-react';
import { api, ApiError } from '../services/api';
import type { PredictResponse, CoStatus } from '../types';

const DAYS_OF_WEEK = [
  { value: 0, label: 'Thứ Hai' },
  { value: 1, label: 'Thứ Ba' },
  { value: 2, label: 'Thứ Tư' },
  { value: 3, label: 'Thứ Năm' },
  { value: 4, label: 'Thứ Sáu' },
  { value: 5, label: 'Thứ Bảy' },
  { value: 6, label: 'Chủ Nhật' },
];

export const PredictTab: React.FC = () => {
  const [hour, setHour] = useState<number>(8);
  const [month, setMonth] = useState<number>(10);
  const [dayOfWeek, setDayOfWeek] = useState<number>(0);
  const [temperatureC, setTemperatureC] = useState<number>(31.5);
  const [windSpeedKmh, setWindSpeedKmh] = useState<number>(12.0);

  const [loading, setLoading] = useState<boolean>(false);
  const [result, setResult] = useState<PredictResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setLoading(true);
      setError(null);
      const res = await api.predictCo({
        hour,
        month,
        dayOfWeek,
        temperatureC,
        windSpeedKmh,
      });
      setResult(res);
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        setError(`[${err.code || 'ERROR'}] ${err.message}`);
      } else {
        const msg = err instanceof Error ? err.message : String(err);
        setError(`Lỗi khi dự đoán: ${msg}`);
      }
    } finally {
      setLoading(false);
    }
  };

  const getStatusBadge = (status: CoStatus) => {
    switch (status) {
      case 'SAFE':
        return (
          <span className="px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wider bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
            SAFE (An toàn &lt; 500)
          </span>
        );
      case 'WARNING':
        return (
          <span className="px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wider bg-amber-500/20 text-amber-400 border border-amber-500/30">
            WARNING (Cảnh báo 500–1000)
          </span>
        );
      case 'DANGER':
        return (
          <span className="px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wider bg-rose-500/20 text-rose-400 border border-rose-500/30">
            DANGER (Nguy hiểm &gt; 1000)
          </span>
        );
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
            <Sparkles className="w-6 h-6 text-purple-400" />
            Dự đoán Nồng độ CO (Heuristic Thống kê)
          </h2>
          <p className="text-sm text-slate-400">
            Ước lượng khoảng nồng độ CO theo giờ dựa trên trung vị mẫu Kaggle sau điểm gãy 01/07/2025
          </p>
        </div>
      </div>

      {/* Cảnh báo Heuristic vs Machine Learning */}
      <div className="p-4 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-300 text-xs sm:text-sm space-y-2">
        <div className="flex items-center gap-2 font-bold text-amber-400">
          <AlertTriangle className="w-5 h-5 shrink-0" />
          <span>Lưu ý quan trọng: Đây là Heuristic Thống kê, KHÔNG PHẢI Mô hình Học máy (ML)</span>
        </div>
        <p className="text-slate-300 text-xs leading-relaxed">
          Thử nghiệm trên dữ liệu thật sau 01/07/2025 cho thấy hồi quy theo nhiệt độ và gió đạt R² ≈ 0 (không tương quan). Do đó thuật toán sử dụng{' '}
          <strong className="text-amber-300">trung vị và khoảng bách phân vị 10%–90%</strong> quanh giờ được hỏi (±1h).
          Nhiệt độ, tốc độ gió, tháng và thứ được gửi lên để kiểm tra tính hợp lệ nhưng <strong className="text-amber-300">chưa được đưa vào công thức tính toán</strong>.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* Form nhập tham số */}
        <div className="lg:col-span-5 p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-5">
          <h3 className="text-base font-bold text-white flex items-center gap-2">
            <Calendar className="w-4 h-4 text-purple-400" />
            Tham số đầu vào
          </h3>

          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Giờ trong ngày (DÙNG ĐỂ TÍNH TOÁN) */}
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label htmlFor="hour-select" className="text-xs font-semibold text-slate-200 flex items-center gap-1.5">
                  <Clock className="w-4 h-4 text-sky-400" />
                  Giờ trong ngày (0 – 23)
                </label>
                <span className="text-[10px] px-2 py-0.5 rounded bg-sky-500/20 text-sky-400 font-bold border border-sky-500/30">
                  ĐƯỢC DÙNG TÍNH TOÁN
                </span>
              </div>
              <select
                id="hour-select"
                value={hour}
                onChange={(e) => setHour(Number(e.target.value))}
                className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded-xl text-slate-200 text-sm focus:outline-none focus:border-purple-500 font-mono"
              >
                {Array.from({ length: 24 }, (_, i) => (
                  <option key={i} value={i}>
                    {i.toString().padStart(2, '0')}:00 (Giờ {i})
                  </option>
                ))}
              </select>
            </div>

            {/* Tháng trong năm (Chưa dùng) */}
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label htmlFor="month-select" className="text-xs font-semibold text-slate-200 flex items-center gap-1.5">
                  <Calendar className="w-4 h-4 text-slate-400" />
                  Tháng (1 – 12)
                </label>
                <span className="text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700">
                  Chưa dùng trong heuristic
                </span>
              </div>
              <select
                id="month-select"
                value={month}
                onChange={(e) => setMonth(Number(e.target.value))}
                className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded-xl text-slate-200 text-sm focus:outline-none focus:border-purple-500 font-mono"
              >
                {Array.from({ length: 12 }, (_, i) => (
                  <option key={i + 1} value={i + 1}>
                    Tháng {i + 1}
                  </option>
                ))}
              </select>
            </div>

            {/* Thứ trong tuần (Chưa dùng) */}
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label htmlFor="dow-select" className="text-xs font-semibold text-slate-200">
                  Thứ trong tuần
                </label>
                <span className="text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700">
                  Chưa dùng trong heuristic
                </span>
              </div>
              <select
                id="dow-select"
                value={dayOfWeek}
                onChange={(e) => setDayOfWeek(Number(e.target.value))}
                className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded-xl text-slate-200 text-sm focus:outline-none focus:border-purple-500 font-mono"
              >
                {DAYS_OF_WEEK.map((d) => (
                  <option key={d.value} value={d.value}>
                    {d.label}
                  </option>
                ))}
              </select>
            </div>

            {/* Nhiệt độ (°C) (Chưa dùng) */}
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label htmlFor="temp-input" className="text-xs font-semibold text-slate-200 flex items-center gap-1.5">
                  <Thermometer className="w-4 h-4 text-slate-400" />
                  Nhiệt độ (°C)
                </label>
                <span className="text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700">
                  Chưa dùng trong heuristic
                </span>
              </div>
              <input
                id="temp-input"
                type="number"
                step="0.1"
                value={temperatureC}
                onChange={(e) => setTemperatureC(Number(e.target.value))}
                className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded-xl text-slate-200 text-sm focus:outline-none focus:border-purple-500 font-mono"
              />
            </div>

            {/* Tốc độ gió (km/h) (Chưa dùng) */}
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label htmlFor="wind-input" className="text-xs font-semibold text-slate-200 flex items-center gap-1.5">
                  <Wind className="w-4 h-4 text-slate-400" />
                  Tốc độ gió (km/h)
                </label>
                <span className="text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700">
                  Chưa dùng trong heuristic
                </span>
              </div>
              <input
                id="wind-input"
                type="number"
                min="0"
                step="0.1"
                value={windSpeedKmh}
                onChange={(e) => setWindSpeedKmh(Math.max(0, Number(e.target.value)))}
                className="w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded-xl text-slate-200 text-sm focus:outline-none focus:border-purple-500 font-mono"
              />
            </div>

            <button
              type="submit"
              disabled={loading}
              className="w-full inline-flex items-center justify-center gap-2 px-5 py-2.5 bg-purple-600 hover:bg-purple-500 text-white font-bold text-sm rounded-xl transition shadow-lg shadow-purple-600/20 disabled:opacity-50"
            >
              {loading ? (
                <>
                  <Clock className="w-4 h-4 animate-spin" />
                  Đang tính toán Heuristic...
                </>
              ) : (
                <>
                  <Send className="w-4 h-4" />
                  Dự đoán Nồng độ CO
                </>
              )}
            </button>
          </form>
        </div>

        {/* Kết quả dự đoán */}
        <div className="lg:col-span-7 space-y-6">
          {error && (
            <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm flex items-start gap-3">
              <AlertTriangle className="w-5 h-5 shrink-0 text-rose-400 mt-0.5" />
              <div>{error}</div>
            </div>
          )}

          {result ? (
            <div className="p-6 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-6">
              <div className="flex items-center justify-between border-b border-slate-800 pb-4">
                <div className="space-y-1">
                  <h3 className="text-base font-bold text-white">Kết quả Ước lượng CO</h3>
                  <p className="text-xs text-slate-400">
                    Dự đoán cho mốc <strong>{hour}:00</strong>
                  </p>
                </div>
                {getStatusBadge(result.status)}
              </div>

              {/* TRỌNG TÂM: Khoảng dao động bách phân vị 10%–90% */}
              {result.interval && (
                <div className="p-4 rounded-xl bg-purple-500/10 border border-purple-500/30 space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-semibold text-purple-300 uppercase tracking-wider">
                      Khoảng tin cậy chính (Bách phân vị 10% – 90%)
                    </span>
                    <span className="text-xs font-mono text-purple-400 font-bold">
                      {result.interval.low} – {result.interval.high} {result.unit}
                    </span>
                  </div>
                  <div className="relative pt-2 pb-1">
                    <div className="h-3 bg-slate-800 rounded-full overflow-hidden flex">
                      <div className="h-full bg-purple-500/60 rounded-full w-full" />
                    </div>
                  </div>
                  <p className="text-[11px] text-slate-400">
                    * Đây là thông tin quan trọng nhất: 80% các giờ tương tự trong lịch sử có giá trị CO nằm trong khoảng này.
                  </p>
                </div>
              )}

              {/* Giá trị trung vị */}
              <div className="grid grid-cols-2 gap-4">
                <div className="p-4 bg-slate-800/40 rounded-xl border border-slate-800">
                  <span className="text-xs text-slate-400 block mb-1">
                    Giá trị trung vị dự đoán (coPredicted)
                  </span>
                  <div className="flex items-baseline gap-2">
                    <span className="text-3xl font-extrabold text-white font-mono">
                      {result.coPredicted}
                    </span>
                    <span className="text-sm font-semibold text-slate-400">{result.unit}</span>
                  </div>
                </div>

                <div className="p-4 bg-slate-800/40 rounded-xl border border-slate-800">
                  <span className="text-xs text-slate-400 block mb-1">Mức độ cảnh báo</span>
                  <div className="mt-1">{getStatusBadge(result.status)}</div>
                </div>
              </div>

              {/* Thông tin mô hình & Dữ liệu huấn luyện */}
              <div className="p-4 rounded-xl bg-slate-800/30 border border-slate-800 space-y-2 text-xs">
                <div className="flex items-center justify-between font-mono text-slate-400 border-b border-slate-800 pb-2">
                  <span>
                    Mô hình: <strong className="text-slate-200">{result.model.name}</strong> v
                    {result.model.version}
                  </span>
                  <span className="px-2 py-0.5 rounded bg-slate-800 text-purple-300 uppercase font-bold border border-slate-700">
                    Loại: {result.model.kind}
                  </span>
                </div>

                {result.model.trainedOn && (
                  <div className="pt-1 space-y-1">
                    <span className="text-[11px] font-semibold text-slate-300 flex items-center gap-1">
                      <Info className="w-3.5 h-3.5 text-purple-400" />
                      Căn cứ dữ liệu (trainedOn):
                    </span>
                    <p className="text-slate-400 leading-relaxed text-[11px]">
                      {result.model.trainedOn}
                    </p>
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="p-12 rounded-2xl bg-slate-900/40 border border-dashed border-slate-800 text-center space-y-3">
              <HelpCircle className="w-10 h-10 text-slate-600 mx-auto" />
              <h4 className="text-sm font-bold text-white">Chưa có kết quả dự đoán</h4>
              <p className="text-xs text-slate-400 max-w-sm mx-auto">
                Chọn giờ trong ngày và nhấn "Dự đoán Nồng độ CO" để nhận khoảng giá trị trung vị từ thuật toán heuristic.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
