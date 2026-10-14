import React, { useEffect, useState } from 'react';
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
  BrainCircuit,
  History,
  FlaskConical,
  ChevronDown,
  RotateCcw,
  Radio,
} from 'lucide-react';
import { api, ApiError } from '../services/api';
import type { PredictResponse, PredictRequest, CoStatus, ModelMetrics } from '../types';

// Các đầu vào tùy chọn của mô hình (hợp đồng 0.2.0). Bỏ trống thì KHÔNG gửi; backend điền trung vị tập huấn luyện và liệt kê ở filledInputs.
type OptionalKey = 'coLag1h' | 'coLag2h' | 'coLag3h' | 'coLag24h' | 'no' | 'no2' | 'o3' | 'so2' | 'pm25' | 'pm10' | 'nh3';

const CO_LAGS: { key: OptionalKey; label: string; hint: string }[] = [
  { key: 'coLag1h', label: 'CO giờ t-1', hint: 'Quan trọng nhất' },
  { key: 'coLag2h', label: 'CO giờ t-2', hint: '' },
  { key: 'coLag3h', label: 'CO giờ t-3', hint: '' },
  { key: 'coLag24h', label: 'CO cùng giờ hôm qua (t-24)', hint: '' },
];

const POLLUTANTS: { key: OptionalKey; label: string }[] = [
  { key: 'no', label: 'NO' },
  { key: 'no2', label: 'NO₂' },
  { key: 'o3', label: 'O₃' },
  { key: 'so2', label: 'SO₂' },
  { key: 'pm25', label: 'PM2.5' },
  { key: 'pm10', label: 'PM10' },
  { key: 'nh3', label: 'NH₃' },
];

const FEATURE_LABEL: Record<string, string> = {
  hour: 'Giờ',
  month: 'Tháng',
  temperatureC: 'Nhiệt độ t-1',
  windSpeedKmh: 'Gió t-1',
  ...Object.fromEntries(CO_LAGS.map((f) => [f.key, f.label])),
  ...Object.fromEntries(POLLUTANTS.map((f) => [f.key, `${f.label} t-1`])),
};

const EMPTY_OPTIONAL: Record<OptionalKey, string> = {
  coLag1h: '',
  coLag2h: '',
  coLag3h: '',
  coLag24h: '',
  no: '',
  no2: '',
  o3: '',
  so2: '',
  pm25: '',
  pm10: '',
  nh3: '',
};

const inputCls =
  'w-full px-3 py-2 bg-slate-800 border border-slate-700 rounded-xl text-slate-200 text-sm focus:outline-none focus:border-purple-500 font-mono placeholder:text-slate-600';

const fmt = (v: number, digits = 1) => v.toLocaleString('vi-VN', { maximumFractionDigits: digits });

export const PredictTab: React.FC = () => {
  const [hour, setHour] = useState<number>(8);
  const [month, setMonth] = useState<number>(10);
  const [temperatureC, setTemperatureC] = useState<number>(31.5);
  const [windSpeedKmh, setWindSpeedKmh] = useState<number>(12.0);
  const [optional, setOptional] = useState<Record<OptionalKey, string>>(EMPTY_OPTIONAL);
  const [showPollutants, setShowPollutants] = useState<boolean>(false);

  const [metrics, setMetrics] = useState<ModelMetrics | null>(null);
  const [metricsError, setMetricsError] = useState<{ code: string; message: string } | null>(null);

  const [filling, setFilling] = useState<boolean>(false);
  const [fillNote, setFillNote] = useState<string | null>(null);

  const [loading, setLoading] = useState<boolean>(false);
  const [result, setResult] = useState<PredictResponse | null>(null);
  const [submitted, setSubmitted] = useState<PredictRequest | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Mô hình đang được dùng (bước 6b của pipeline). Chưa huấn luyện thì backend rơi về heuristic.
  useEffect(() => {
    let cancelled = false;
    api
      .getModelMetrics()
      .then((m) => {
        if (!cancelled) setMetrics(m);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (err instanceof ApiError) {
          setMetricsError({ code: err.code || 'ERROR', message: err.message });
        } else {
          setMetricsError({ code: 'ERROR', message: err instanceof Error ? err.message : String(err) });
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const bestModel = metrics?.models.find((m) => m.isBest) ?? null;
  const prevHourBaseline = metrics?.baselines.find((b) => b.id === 'previous-hour') ?? null;
  const gainVsPrevHour =
    bestModel && prevHourBaseline && prevHourBaseline.test.rmse > 0
      ? ((prevHourBaseline.test.rmse - bestModel.test.rmse) / prevHourBaseline.test.rmse) * 100
      : null;

  const setOpt = (key: OptionalKey, value: string) => setOptional((prev) => ({ ...prev, [key]: value }));

  // Điền từ bản ghi DỮ LIỆU THẬT mới nhất (OpenWeather): giá trị giờ đó làm "giờ t-1", giờ cần dự đoán là giờ kế tiếp.
  // Bản ghi không có CO t-2, t-3, t-24 và các chất khác nên các ô đó để trống (backend điền trung vị và báo rõ).
  const fillFromLatest = async () => {
    try {
      setFilling(true);
      setFillNote(null);
      const r = await api.getAqiLatest('live-openweather');
      const ms = Date.parse(r.datetime);
      if (Number.isNaN(ms)) throw new Error('Không đọc được thời điểm của bản ghi');
      // Giờ kế tiếp theo giờ +07 (hợp đồng: mọi thời gian ở Asia/Ho_Chi_Minh), không phụ thuộc múi giờ của trình duyệt
      const next = new Date(ms + 3600_000 + 7 * 3600_000).toISOString();
      const nextHour = Number(next.slice(11, 13));
      setHour(nextHour);
      setMonth(Number(next.slice(5, 7)));
      setTemperatureC(Math.round(r.temperatureC * 10) / 10);
      setWindSpeedKmh(Math.round(r.windSpeedKmh * 10) / 10);
      setOptional({ ...EMPTY_OPTIONAL, coLag1h: String(Math.round(r.co * 100) / 100) });
      setFillNote(
        `Đã điền từ bản ghi thật lúc ${r.datetime.slice(11, 16)} ngày ${r.datetime.slice(8, 10)}/${r.datetime.slice(5, 7)}` +
          ` (CO ${fmt(r.co, 2)} µg/m³), dự đoán cho ${String(nextHour).padStart(2, '0')}:00.` +
          (r.coIsForecast === true ? ' Lưu ý: CO của bản ghi này là điểm DỰ BÁO của OpenWeather, không phải quan sát.' : '') +
          ' CO t-2, t-3, t-24 và các chất khác không có trong bản ghi nên để trống.',
      );
    } catch (err: unknown) {
      const msg = err instanceof ApiError ? `[${err.code || 'ERROR'}] ${err.message}` : err instanceof Error ? err.message : String(err);
      setFillNote(`Không lấy được bản ghi thật mới nhất: ${msg}`);
    } finally {
      setFilling(false);
    }
  };

  const clearOptional = () => {
    setOptional(EMPTY_OPTIONAL);
    setFillNote(null);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const req: PredictRequest = { hour, month, temperatureC, windSpeedKmh };
    for (const [key, raw] of Object.entries(optional) as [OptionalKey, string][]) {
      const t = raw.trim().replace(',', '.');
      if (t === '') continue;
      const v = Number(t);
      if (!Number.isFinite(v) || v < 0) {
        setError(`Giá trị "${FEATURE_LABEL[key]}" không hợp lệ: phải là số ≥ 0 hoặc để trống.`);
        return;
      }
      req[key] = v;
    }
    try {
      setLoading(true);
      setError(null);
      const res = await api.predictCo(req);
      setResult(res);
      setSubmitted(req);
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
            SAFE (An toàn)
          </span>
        );
      case 'WARNING':
        return (
          <span className="px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wider bg-amber-500/20 text-amber-400 border border-amber-500/30">
            WARNING (Cảnh báo)
          </span>
        );
      case 'DANGER':
        return (
          <span className="px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wider bg-rose-500/20 text-rose-400 border border-rose-500/30">
            DANGER (Nguy hiểm)
          </span>
        );
    }
  };

  const isMl = result?.model.kind === 'ml';
  const filledNames = new Set((result?.filledInputs ?? []).map((f) => f.name));
  const lag1Filled = isMl && filledNames.has('coLag1h');
  const providedLag1 = submitted?.coLag1h ?? null;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
          <Sparkles className="w-6 h-6 text-purple-400" />
          Dự đoán CO giờ tới
        </h2>
        <p className="text-sm text-slate-400">
          Mô hình Spark MLlib dự báo trước 1 giờ: dùng thông tin đến giờ t-1 để đoán nồng độ CO của giờ t.
        </p>
      </div>

      {/* Mô hình đang dùng */}
      {bestModel ? (
        <div className="p-4 rounded-xl bg-sky-500/10 border border-sky-500/30 text-xs sm:text-sm space-y-2">
          <div className="flex flex-wrap items-center gap-2 font-bold text-sky-300">
            <BrainCircuit className="w-5 h-5 shrink-0 text-sky-400" />
            <span>Mô hình đang dùng: {bestModel.name}</span>
            <span className="font-mono text-[11px] font-normal text-sky-400/80">{bestModel.version}</span>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-[11px]">
            <div className="p-2 rounded-lg bg-slate-900/60 border border-slate-800">
              <div className="text-slate-400">RMSE kiểm tra</div>
              <div className="font-mono font-bold text-white">{fmt(bestModel.test.rmse)} µg/m³</div>
            </div>
            <div className="p-2 rounded-lg bg-slate-900/60 border border-slate-800">
              <div className="text-slate-400">Mốc “CO giờ trước”</div>
              <div className="font-mono font-bold text-white">
                {prevHourBaseline ? `${fmt(prevHourBaseline.test.rmse)} µg/m³` : '—'}
              </div>
            </div>
            <div className="p-2 rounded-lg bg-slate-900/60 border border-slate-800">
              <div className="text-slate-400">Giảm sai số so với mốc</div>
              <div className={`font-mono font-bold ${gainVsPrevHour !== null && gainVsPrevHour > 0 ? 'text-emerald-400' : 'text-amber-400'}`}>
                {gainVsPrevHour !== null ? `${gainVsPrevHour > 0 ? '−' : '+'}${fmt(Math.abs(gainVsPrevHour))}%` : '—'}
              </div>
            </div>
            <div className="p-2 rounded-lg bg-slate-900/60 border border-slate-800">
              <div className="text-slate-400">Tập kiểm tra</div>
              <div className="font-mono font-bold text-white">{metrics?.testSet.rows.toLocaleString('vi-VN')} giờ</div>
            </div>
          </div>
          <p className="text-[11px] text-slate-400 leading-relaxed">
            Chia dữ liệu theo thời gian, mô hình chọn theo đoạn kiểm định. Chi tiết và so sánh các mô hình ở tab Pipeline.
          </p>
        </div>
      ) : metricsError ? (
        <div
          className={`p-4 rounded-xl text-xs sm:text-sm flex items-start gap-3 ${
            metricsError.code === 'MODEL_NOT_TRAINED'
              ? 'bg-amber-500/10 border border-amber-500/30 text-amber-300'
              : 'bg-slate-800/40 border border-slate-700 text-slate-300'
          }`}
        >
          <AlertTriangle className="w-5 h-5 shrink-0 mt-0.5" />
          <div>
            {metricsError.code === 'MODEL_NOT_TRAINED'
              ? 'Chưa huấn luyện mô hình (bước 6b của pipeline). Backend sẽ trả ước lượng heuristic thống kê thay cho mô hình học máy.'
              : `Không đọc được thông tin mô hình [${metricsError.code}]: ${metricsError.message}`}
          </div>
        </div>
      ) : null}

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* Form nhập tham số */}
        <div className="lg:col-span-5 p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-5">
          <div className="flex items-center justify-between gap-2">
            <h3 className="text-base font-bold text-white flex items-center gap-2">
              <Calendar className="w-4 h-4 text-purple-400" />
              Đầu vào của mô hình
            </h3>
          </div>

          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={fillFromLatest}
              disabled={filling}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-medium rounded-lg border border-slate-700 transition disabled:opacity-50"
            >
              <Radio className="w-3.5 h-3.5 text-emerald-400" />
              {filling ? 'Đang lấy…' : 'Điền từ bản ghi thật mới nhất'}
            </button>
            <button
              type="button"
              onClick={clearOptional}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-medium rounded-lg border border-slate-700 transition"
            >
              <RotateCcw className="w-3.5 h-3.5" />
              Xóa ô tùy chọn
            </button>
          </div>
          {fillNote && (
            <p className="text-[11px] text-slate-300 leading-relaxed p-2.5 rounded-lg bg-slate-800/50 border border-slate-700">
              {fillNote}
            </p>
          )}

          <form onSubmit={handleSubmit} className="space-y-5">
            {/* 1. Thời điểm cần dự đoán */}
            <fieldset className="space-y-3">
              <legend className="text-[11px] font-bold uppercase tracking-wider text-slate-400 mb-2">
                1. Thời điểm cần dự đoán (giờ t)
              </legend>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label htmlFor="hour-select" className="text-xs font-semibold text-slate-200 flex items-center gap-1.5 mb-1.5">
                    <Clock className="w-4 h-4 text-sky-400" />
                    Giờ (0 – 23)
                  </label>
                  <select id="hour-select" value={hour} onChange={(e) => setHour(Number(e.target.value))} className={inputCls}>
                    {Array.from({ length: 24 }, (_, i) => (
                      <option key={i} value={i}>
                        {i.toString().padStart(2, '0')}:00
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label htmlFor="month-select" className="text-xs font-semibold text-slate-200 flex items-center gap-1.5 mb-1.5">
                    <Calendar className="w-4 h-4 text-sky-400" />
                    Tháng
                  </label>
                  <select id="month-select" value={month} onChange={(e) => setMonth(Number(e.target.value))} className={inputCls}>
                    {Array.from({ length: 12 }, (_, i) => (
                      <option key={i + 1} value={i + 1}>
                        Tháng {i + 1}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
            </fieldset>

            {/* 2. Thời tiết giờ trước */}
            <fieldset className="space-y-3">
              <legend className="text-[11px] font-bold uppercase tracking-wider text-slate-400 mb-2">
                2. Thời tiết giờ trước (t-1)
              </legend>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label htmlFor="temp-input" className="text-xs font-semibold text-slate-200 flex items-center gap-1.5 mb-1.5">
                    <Thermometer className="w-4 h-4 text-orange-400" />
                    Nhiệt độ (°C)
                  </label>
                  <input
                    id="temp-input"
                    type="number"
                    step="0.1"
                    required
                    value={temperatureC}
                    onChange={(e) => setTemperatureC(Number(e.target.value))}
                    className={inputCls}
                  />
                </div>
                <div>
                  <label htmlFor="wind-input" className="text-xs font-semibold text-slate-200 flex items-center gap-1.5 mb-1.5">
                    <Wind className="w-4 h-4 text-teal-400" />
                    Gió (km/h)
                  </label>
                  <input
                    id="wind-input"
                    type="number"
                    min="0"
                    step="0.1"
                    required
                    value={windSpeedKmh}
                    onChange={(e) => setWindSpeedKmh(Math.max(0, Number(e.target.value)))}
                    className={inputCls}
                  />
                </div>
              </div>
            </fieldset>

            {/* 3. CO các giờ trước */}
            <fieldset className="space-y-3">
              <legend className="text-[11px] font-bold uppercase tracking-wider text-slate-400 mb-1 flex items-center gap-1.5">
                <History className="w-3.5 h-3.5" />
                3. CO các giờ trước (µg/m³)
              </legend>
              <p className="text-[11px] text-slate-500 -mt-1">
                Bỏ trống ô nào thì backend điền trung vị tập huấn luyện và ghi rõ trong kết quả.
              </p>
              <div className="grid grid-cols-2 gap-3">
                {CO_LAGS.map((f) => (
                  <div key={f.key}>
                    <label htmlFor={`opt-${f.key}`} className="text-xs font-semibold text-slate-200 flex items-center justify-between gap-1 mb-1.5">
                      <span>{f.label}</span>
                      {f.hint && (
                        <span className="text-[9px] px-1.5 py-0.5 rounded bg-purple-500/20 text-purple-300 border border-purple-500/30 font-bold">
                          {f.hint}
                        </span>
                      )}
                    </label>
                    <input
                      id={`opt-${f.key}`}
                      type="text"
                      inputMode="decimal"
                      placeholder="trống = trung vị"
                      value={optional[f.key]}
                      onChange={(e) => setOpt(f.key, e.target.value)}
                      className={inputCls}
                    />
                  </div>
                ))}
              </div>
            </fieldset>

            {/* 4. Các chất khác giờ trước (thu gọn) */}
            <fieldset className="rounded-xl border border-slate-800">
              <button
                type="button"
                onClick={() => setShowPollutants((v) => !v)}
                aria-expanded={showPollutants}
                className="w-full flex items-center justify-between px-3 py-2.5 text-[11px] font-bold uppercase tracking-wider text-slate-400 hover:text-slate-200"
              >
                <span className="flex items-center gap-1.5">
                  <FlaskConical className="w-3.5 h-3.5" />
                  4. Các chất khác giờ t-1 (tùy chọn)
                  {POLLUTANTS.some((p) => optional[p.key].trim() !== '') && (
                    <span className="normal-case font-normal text-purple-300">
                      · đã nhập {POLLUTANTS.filter((p) => optional[p.key].trim() !== '').length}
                    </span>
                  )}
                </span>
                <ChevronDown className={`w-4 h-4 transition ${showPollutants ? 'rotate-180' : ''}`} />
              </button>
              {showPollutants && (
                <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 px-3 pb-3">
                  {POLLUTANTS.map((f) => (
                    <div key={f.key}>
                      <label htmlFor={`opt-${f.key}`} className="text-xs font-semibold text-slate-200 block mb-1.5">
                        {f.label}
                      </label>
                      <input
                        id={`opt-${f.key}`}
                        type="text"
                        inputMode="decimal"
                        placeholder="trung vị"
                        value={optional[f.key]}
                        onChange={(e) => setOpt(f.key, e.target.value)}
                        className={inputCls}
                      />
                    </div>
                  ))}
                </div>
              )}
            </fieldset>

            <button
              type="submit"
              disabled={loading}
              className="w-full inline-flex items-center justify-center gap-2 px-5 py-2.5 bg-purple-600 hover:bg-purple-500 text-white font-bold text-sm rounded-xl transition shadow-lg shadow-purple-600/20 disabled:opacity-50"
            >
              {loading ? (
                <>
                  <Clock className="w-4 h-4 animate-spin" />
                  Đang dự đoán…
                </>
              ) : (
                <>
                  <Send className="w-4 h-4" />
                  Dự đoán CO
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

          {result && submitted ? (
            <div className="p-6 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-5">
              <div className="flex items-center justify-between gap-3 border-b border-slate-800 pb-4">
                <div className="space-y-1">
                  <h3 className="text-base font-bold text-white">Kết quả dự đoán</h3>
                  <p className="text-xs text-slate-400">
                    CO lúc <strong>{String(submitted.hour).padStart(2, '0')}:00</strong>, tháng {submitted.month}
                  </p>
                </div>
                {getStatusBadge(result.status)}
              </div>

              {/* Heuristic: chỉ hiện khi backend thật sự trả heuristic (chưa có mô hình) */}
              {!isMl && (
                <div className="p-3 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-300 text-xs flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
                  <span>
                    Backend chưa có mô hình học máy đã huấn luyện nên trả <strong>heuristic thống kê</strong> (trung vị CO quanh giờ được hỏi).
                    Các đầu vào khác ngoài giờ không được dùng.
                  </span>
                </div>
              )}

              {/* Giá trị dự đoán */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div className="p-4 bg-purple-500/10 rounded-xl border border-purple-500/30">
                  <span className="text-xs text-purple-300 block mb-1">
                    {isMl ? 'CO dự đoán (mô hình)' : 'CO ước lượng (trung vị)'}
                  </span>
                  <div className="flex items-baseline gap-2">
                    <span className="text-3xl font-extrabold text-white font-mono">{fmt(result.coPredicted)}</span>
                    <span className="text-sm font-semibold text-slate-400">{result.unit}</span>
                  </div>
                </div>

                {isMl && providedLag1 !== null && providedLag1 !== undefined ? (
                  <div className="p-4 bg-slate-800/40 rounded-xl border border-slate-800">
                    <span className="text-xs text-slate-400 block mb-1">Mốc so sánh: giữ nguyên CO giờ t-1</span>
                    <div className="flex items-baseline gap-2">
                      <span className="text-2xl font-bold text-slate-200 font-mono">{fmt(providedLag1)}</span>
                      <span className="text-sm text-slate-500">{result.unit}</span>
                    </div>
                    <span className="text-[11px] text-slate-500">
                      Mô hình {result.coPredicted >= providedLag1 ? 'tăng' : 'giảm'} {fmt(Math.abs(result.coPredicted - providedLag1))} µg/m³ so với mốc
                    </span>
                  </div>
                ) : (
                  <div className="p-4 bg-slate-800/40 rounded-xl border border-slate-800">
                    <span className="text-xs text-slate-400 block mb-1">Mức độ cảnh báo</span>
                    <div className="mt-1">{getStatusBadge(result.status)}</div>
                    <span className="text-[11px] text-slate-500 block mt-2">Theo thang CO của OpenWeather (SAFE &lt; 4 400 µg/m³)</span>
                  </div>
                )}
              </div>

              {/* Khoảng 10%–90%: chỉ heuristic có; mô hình ml trả interval = null */}
              {result.interval && (
                <div className="p-4 rounded-xl bg-slate-800/40 border border-slate-800 space-y-1.5">
                  <div className="flex items-center justify-between text-xs">
                    <span className="font-semibold text-slate-300">Khoảng bách phân vị 10% – 90%</span>
                    <span className="font-mono text-slate-200 font-bold">
                      {result.interval.low} – {result.interval.high} {result.unit}
                    </span>
                  </div>
                  <p className="text-[11px] text-slate-500">80% các giờ tương tự trong lịch sử có CO nằm trong khoảng này.</p>
                </div>
              )}

              {/* Độ tin cậy theo đầu vào */}
              {isMl && lag1Filled && (
                <div className="p-3 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-300 text-xs flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
                  <span>
                    Chưa nhập <strong>CO giờ t-1</strong> nên mô hình dùng trung vị thay vào: kết quả chỉ mang tính minh họa, độ tin cậy thấp.
                    Hãy nhập CO các giờ trước hoặc bấm “Điền từ bản ghi thật mới nhất”.
                  </span>
                </div>
              )}

              {/* Thông tin mô hình */}
              <div className="p-4 rounded-xl bg-slate-800/30 border border-slate-800 space-y-2 text-xs">
                <div className="flex flex-wrap items-center justify-between gap-2 font-mono text-slate-400">
                  <span>
                    Mô hình: <strong className="text-slate-200">{result.model.name}</strong>{' '}
                    <span className="text-slate-500">({result.model.version})</span>
                  </span>
                  <span
                    className={`px-2 py-0.5 rounded font-bold border ${
                      isMl ? 'bg-sky-500/20 text-sky-300 border-sky-500/40' : 'bg-amber-500/15 text-amber-300 border-amber-500/40'
                    }`}
                  >
                    {isMl ? 'ML · Spark MLlib' : 'HEURISTIC'}
                  </span>
                </div>
                {result.model.trainedOn && (
                  <details className="group pt-1">
                    <summary className="cursor-pointer text-[11px] font-semibold text-slate-300 flex items-center gap-1 list-none">
                      <Info className="w-3.5 h-3.5 text-purple-400" />
                      Căn cứ dữ liệu huấn luyện
                      <ChevronDown className="w-3.5 h-3.5 transition group-open:rotate-180" />
                    </summary>
                    <p className="text-slate-400 leading-relaxed text-[11px] mt-1.5">{result.model.trainedOn}</p>
                  </details>
                )}
              </div>

              {/* Đầu vào backend đã điền thay (filledInputs) */}
              {result.filledInputs && result.filledInputs.length > 0 && (
                <div className="p-4 rounded-xl bg-slate-800/30 border border-slate-800 space-y-2 text-xs">
                  <div className="flex items-center justify-between">
                    <span className="font-semibold text-slate-300 flex items-center gap-1.5">
                      <Info className="w-4 h-4 text-sky-400" />
                      Ô bỏ trống được điền bằng trung vị
                    </span>
                    <span className="text-[11px] text-slate-500 font-mono">{result.filledInputs.length} trường</span>
                  </div>
                  <div className="flex flex-wrap gap-1.5">
                    {result.filledInputs.map((item) => (
                      <span
                        key={item.name}
                        title={item.origin}
                        className={`px-2 py-1 rounded-md border font-mono text-[11px] ${
                          item.name === 'coLag1h'
                            ? 'bg-amber-500/10 border-amber-500/30 text-amber-300'
                            : 'bg-slate-900/60 border-slate-800 text-slate-300'
                        }`}
                      >
                        {FEATURE_LABEL[item.name] ?? item.name}: {fmt(item.value, 2)}
                      </span>
                    ))}
                  </div>
                  <p className="text-[11px] text-slate-500">Nguồn: {result.filledInputs[0].origin}.</p>
                </div>
              )}
            </div>
          ) : (
            <div className="p-12 rounded-2xl bg-slate-900/40 border border-dashed border-slate-800 text-center space-y-3">
              <HelpCircle className="w-10 h-10 text-slate-600 mx-auto" />
              <h4 className="text-sm font-bold text-white">Chưa có kết quả dự đoán</h4>
              <p className="text-xs text-slate-400 max-w-sm mx-auto">
                Nhập giờ, thời tiết và CO các giờ trước (hoặc bấm “Điền từ bản ghi thật mới nhất”), rồi nhấn “Dự đoán CO”.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
