import React, { useEffect, useState } from 'react';
import {
  Activity,
  Wind,
  Thermometer,
  Gauge,
  AlertTriangle,
  Server,
  Radio,
  Clock,
  Layers,
  CheckCircle2,
  XCircle,
  HelpCircle,
  RefreshCw,
  ExternalLink,
} from 'lucide-react';
import { api } from '../services/api';
import { useStream } from '../context/useStream';
import type { ServiceStatus, AqiReading } from '../types';
import { formatDateTime, formatTimeOnly, getCoStatusBadge, getSourceLabel } from '../utils/format';

export const OverviewTab: React.FC = () => {
  const { latestReading: streamReading, hasStreamReading, serviceOverrides, events } = useStream();
  const [services, setServices] = useState<ServiceStatus[]>([]);
  const [initialReading, setInitialReading] = useState<AqiReading | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);
      const [servicesData, latestData] = await Promise.all([
        api.getServices(),
        api.getAqiLatest(),
      ]);
      setServices(servicesData.services);
      setInitialReading(latestData);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(`Không thể tải dữ liệu: ${msg}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    let ignore = false;
    Promise.all([api.getServices(), api.getAqiLatest()])
      .then(([servicesData, latestData]) => {
        if (!ignore) {
          setServices(servicesData.services);
          setInitialReading(latestData);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!ignore) {
          const msg = err instanceof Error ? err.message : String(err);
          setError(`Không thể tải dữ liệu: ${msg}`);
          setLoading(false);
        }
      });
    return () => {
      ignore = true;
    };
  }, []);

  const [currentTime, setCurrentTime] = useState<number>(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(Date.now());
    }, 30000);
    return () => clearInterval(timer);
  }, []);

  // Ưu tiên reading từ luồng thời gian thực nếu có, nếu chưa có thì dùng initial reading từ REST
  const currentReading: AqiReading | null = streamReading || initialReading;

  // Tính tuổi của dữ liệu quan sát
  const getDataAgeText = (datetimeStr?: string) => {
    if (!datetimeStr) return null;
    const recordTime = new Date(datetimeStr).getTime();
    if (isNaN(recordTime)) return null;
    const diffMs = Math.max(0, currentTime - recordTime);
    const diffMinutes = Math.floor(diffMs / (1000 * 60));
    if (diffMinutes < 1) return 'vừa mới đây (< 1 phút)';
    if (diffMinutes < 60) return `cách đây ${diffMinutes} phút`;
    const diffHours = Math.floor(diffMinutes / 60);
    const remMinutes = diffMinutes % 60;
    return `cách đây ${diffHours} giờ ${remMinutes > 0 ? `${remMinutes} phút` : ''}`;
  };

  // Áp dụng override từ event service.state lên danh sách services
  const displayedServices = services.map((s) => {
    if (serviceOverrides[s.name]) {
      return serviceOverrides[s.name];
    }
    return s;
  });

  const statusBadge = currentReading ? getCoStatusBadge(currentReading.status) : null;

  return (
    <div className="space-y-6">
      {/* Header và Nút làm mới */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight">
            Tổng quan Hệ thống & Nồng độ CO
          </h2>
          <p className="text-sm text-slate-400">
            Giám sát thời gian thực nồng độ CO và trạng thái hạ tầng Big Data tại TP.HCM
          </p>
        </div>
        <button
          onClick={loadData}
          disabled={loading}
          className="inline-flex items-center gap-2 px-3.5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs sm:text-sm font-medium rounded-lg border border-slate-700 transition disabled:opacity-50 self-start sm:self-auto"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Tải lại API
        </button>
      </div>

      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm flex items-start gap-3">
          <AlertTriangle className="w-5 h-5 shrink-0 text-rose-400 mt-0.5" />
          <div>
            <div className="font-semibold">Lỗi kết nối REST API</div>
            <div>{error}</div>
            <div className="text-xs text-rose-400/80 mt-1">
              Hãy kiểm tra Mock Server (`python mock/serve.py` cổng 8010) hoặc Backend thật (`localhost:8000`).
            </div>
          </div>
        </div>
      )}

      {/* Card Nồng độ CO thời gian thực (Trọng tâm P1) */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 rounded-2xl bg-slate-900/90 border border-slate-800 p-5 sm:p-6 shadow-xl relative overflow-hidden">
          {/* Header Card */}
          <div className="flex flex-wrap items-center justify-between gap-3 pb-4 border-b border-slate-800/80">
            <div className="flex items-center gap-2.5">
              <div className="w-8 h-8 rounded-lg bg-sky-500/10 border border-sky-500/20 flex items-center justify-center text-sky-400">
                <Radio className="w-4 h-4 animate-pulse" />
              </div>
              <div>
                <span className="text-xs font-semibold text-sky-400 uppercase tracking-wider">
                  {hasStreamReading ? 'Bản ghi đo thời gian thực' : 'Bản ghi đo gần nhất'}
                </span>
                <h3 className="text-lg font-bold text-white flex items-center gap-2">
                  <span>Trạm: {currentReading ? currentReading.station : 'Đang tải...'}</span>
                </h3>
              </div>
            </div>

            {statusBadge && (
              <div
                className={`px-3 py-1.5 rounded-lg text-xs font-bold uppercase tracking-wider ${statusBadge.badgeClass}`}
              >
                {statusBadge.label}
              </div>
            )}
          </div>

          {/* Ghi chú về nguồn Open-Meteo và tuổi dữ liệu (P11) */}
          {currentReading?.source === 'live-open-meteo' && (
            <div className="mt-4 p-3 rounded-xl bg-sky-500/10 border border-sky-500/20 text-xs text-sky-300 space-y-1">
              <div className="flex items-center justify-between font-semibold text-sky-200">
                <span className="flex items-center gap-1.5">
                  <Clock className="w-4 h-4 text-sky-400" />
                  Thời điểm quan sát: {getDataAgeText(currentReading.datetime)}
                </span>
                <span className="text-[11px] font-mono text-sky-400 bg-sky-950/60 px-2 py-0.5 rounded border border-sky-800">
                  Cập nhật ~15 phút/lần
                </span>
              </div>
              <p className="text-slate-300 text-[11px] leading-relaxed">
                Nguồn Open-Meteo (mô hình dự báo CAMS) cập nhật tối đa 15 phút một lần (thời tiết mỗi 15 phút, CO mỗi giờ), không phải cảm biến đo trực tiếp từng giây.
              </p>
            </div>
          )}

          {/* CẢNH BÁO BẮT BUỘC: TRẠM MÔ PHỎNG */}
          {currentReading?.isSimulatedStation && (
            <div className="mt-4 p-3 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-300 text-xs flex items-start gap-2.5">
              <AlertTriangle className="w-4 h-4 shrink-0 text-amber-400 mt-0.5" />
              <div>
                <span className="font-bold">Ghi chú: Trạm mô phỏng. </span>
                {currentReading.source === 'replay-dataset'
                  ? currentReading.station.includes('không có trạm')
                    ? `Nhãn "${currentReading.station}" từ bộ dữ liệu lịch sử Kaggle (không có trạm đo thực tế tại vị trí này).`
                    : `Nhãn "${currentReading.station}" là nhãn được gán luân phiên theo luồng phát lại (replay), không phải trạm đo thực tế tại vị trí này.`
                  : currentReading.source === 'live-open-meteo'
                  ? `Dữ liệu đo mô phỏng vị trí TP.HCM từ mô hình Open-Meteo, không phải trạm vật lý cố định mặt đất.`
                  : `Dữ liệu từ nguồn ${getSourceLabel(currentReading.source)} mang tính chất mô phỏng, không phải trạm đo thực tế.`}
              </div>
            </div>
          )}


          {/* Chỉ số CO chính */}
          <div className="py-6 flex flex-col sm:flex-row sm:items-baseline gap-2 sm:gap-4">
            <div className="flex items-baseline gap-2">
              <span className="text-5xl sm:text-6xl font-extrabold text-white tracking-tight font-mono">
                {currentReading ? currentReading.co.toFixed(1) : '---'}
              </span>
              <span className="text-lg sm:text-xl font-semibold text-slate-400">
                µg/m³
              </span>
            </div>
            <div className="text-xs text-slate-400 sm:border-l sm:border-slate-800 sm:pl-4">
              <div>Chỉ số nồng độ Carbon Monoxide (CO) trong không khí.</div>
              <div className="text-slate-500 italic mt-0.5">
                *Ngưỡng SAFE/WARNING/DANGER do backend trả về, chưa có quy chuẩn chính thức.
              </div>
            </div>
          </div>

          {/* 3 Metric phụ: Tốc độ gió, Nhiệt độ, Chỉ số tích tụ */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 pt-4 border-t border-slate-800/80">
            <div className="p-3 rounded-xl bg-slate-800/40 border border-slate-800">
              <div className="flex items-center justify-between text-slate-400 text-xs mb-1">
                <span>Tốc độ gió</span>
                <Wind className="w-3.5 h-3.5 text-sky-400" />
              </div>
              <div className="text-lg font-bold text-slate-100 font-mono">
                {currentReading ? `${currentReading.windSpeedKmh} km/h` : '—'}
              </div>
            </div>

            <div className="p-3 rounded-xl bg-slate-800/40 border border-slate-800">
              <div className="flex items-center justify-between text-slate-400 text-xs mb-1">
                <span>Nhiệt độ</span>
                <Thermometer className="w-3.5 h-3.5 text-amber-400" />
              </div>
              <div className="text-lg font-bold text-slate-100 font-mono">
                {currentReading ? `${currentReading.temperatureC} °C` : '—'}
              </div>
            </div>

            <div className="p-3 rounded-xl bg-slate-800/40 border border-slate-800">
              <div className="flex items-center justify-between text-slate-400 text-xs mb-1">
                <span>Tích tụ CO</span>
                <Gauge className="w-3.5 h-3.5 text-purple-400" />
              </div>
              <div className="text-lg font-bold text-slate-100 font-mono">
                {currentReading ? currentReading.coAccumulationIndex.toFixed(2) : '—'}
              </div>
              <div className="text-[10px] text-slate-500">CO / (Gió + 1)</div>
            </div>
          </div>

          {/* Footer thông tin Nguồn & Thời gian */}
          <div className="mt-4 pt-3 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-400 border-t border-slate-800/60">
            <div className="flex items-center gap-1.5">
              <Layers className="w-3.5 h-3.5 text-slate-500" />
              <span>Nguồn dữ liệu:</span>
              <span className="font-semibold text-sky-400">
                {getSourceLabel(currentReading?.source)}
              </span>
            </div>
            <div className="flex items-center gap-1.5 font-mono text-slate-400">
              <Clock className="w-3.5 h-3.5 text-slate-500" />
              <span>{formatDateTime(currentReading?.datetime)}</span>
            </div>
          </div>
        </div>

        {/* Cột Tóm tắt nhanh Trạng thái Hạ tầng */}
        <div className="rounded-2xl bg-slate-900/90 border border-slate-800 p-5 sm:p-6 shadow-xl flex flex-col justify-between">
          <div>
            <div className="flex items-center gap-2 mb-4">
              <Server className="w-5 h-5 text-indigo-400" />
              <h3 className="text-base font-bold text-white">Tình trạng Cụm Big Data</h3>
            </div>
            <p className="text-xs text-slate-400 mb-4">
              Kiểm tra nhanh các dịch vụ phân tán đang hoạt động trong hệ thống:
            </p>

            <div className="space-y-2.5">
              {displayedServices.slice(0, 6).map((svc) => (
                <div
                  key={svc.name}
                  className="flex items-center justify-between p-2 rounded-lg bg-slate-800/40 text-xs"
                >
                  <div className="flex items-center gap-2">
                    {svc.state === 'up' ? (
                      <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                    ) : svc.state === 'down' ? (
                      <XCircle className="w-3.5 h-3.5 text-rose-400" />
                    ) : (
                      <HelpCircle className="w-3.5 h-3.5 text-slate-400" />
                    )}
                    <span className="font-medium text-slate-200">{svc.name}</span>
                  </div>
                  <span
                    className={`px-2 py-0.5 rounded font-mono text-[10px] font-bold uppercase ${
                      svc.state === 'up'
                        ? 'bg-emerald-500/20 text-emerald-400'
                        : svc.state === 'down'
                        ? 'bg-rose-500/20 text-rose-400'
                        : 'bg-slate-700 text-slate-400'
                    }`}
                  >
                    {svc.state}
                  </span>
                </div>
              ))}
            </div>
          </div>

          <div className="mt-4 pt-4 border-t border-slate-800 text-xs text-slate-400">
            Tổng cộng: {displayedServices.filter((s) => s.state === 'up').length} /{' '}
            {displayedServices.length} dịch vụ hoạt động tốt.
          </div>
        </div>
      </div>

      {/* Lưới toàn bộ dịch vụ (Services Grid) */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <Activity className="w-5 h-5 text-sky-400" />
          <h3 className="text-lg font-bold text-white">
            Chi tiết các Dịch vụ Hạ tầng ({displayedServices.length})
          </h3>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {displayedServices.map((svc) => (
            <div
              key={svc.name}
              className="p-4 rounded-xl bg-slate-900 border border-slate-800 hover:border-slate-700 transition flex flex-col justify-between space-y-3"
            >
              <div>
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs uppercase font-mono tracking-wider text-slate-500">
                    {svc.kind}
                  </span>
                  <span
                    className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-bold uppercase ${
                      svc.state === 'up'
                        ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
                        : svc.state === 'down'
                        ? 'bg-rose-500/20 text-rose-400 border border-rose-500/30'
                        : 'bg-slate-700 text-slate-400 border border-slate-600'
                    }`}
                  >
                    {svc.state === 'up' && <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" />}
                    {svc.state}
                  </span>
                </div>

                <h4 className="text-base font-bold text-slate-100 flex items-center gap-1.5">
                  {svc.name}
                </h4>

                {svc.detail && (
                  <p className="text-xs text-slate-400 font-mono mt-1 break-all">
                    {svc.detail}
                  </p>
                )}
              </div>

              <div className="pt-2 border-t border-slate-800/60 flex items-center justify-between text-[11px] text-slate-500">
                <span>{formatTimeOnly(svc.checkedAt)}</span>
                {svc.url && (
                  <a
                    href={svc.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-sky-400 hover:text-sky-300 inline-flex items-center gap-1 font-mono"
                  >
                    Mở UI <ExternalLink className="w-3 h-3" />
                  </a>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Nhật ký sự kiện thời gian thực gần đây */}
      <div className="rounded-2xl bg-slate-900 border border-slate-800 p-5">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Radio className="w-4 h-4 text-sky-400" />
            <h3 className="text-base font-bold text-white">
              Nhật ký Luồng Sự kiện Gần nhất ({events.length})
            </h3>
          </div>
          <span className="text-xs text-slate-500">
            Tự động cập nhật qua StreamSource
          </span>
        </div>

        <div className="overflow-x-auto max-h-72 overflow-y-auto border border-slate-800 rounded-lg">
          <table className="w-full text-left text-xs font-mono">
            <thead className="bg-slate-800/80 text-slate-400 sticky top-0">
              <tr>
                <th className="py-2 px-3">seq</th>
                <th className="py-2 px-3">Thời gian</th>
                <th className="py-2 px-3">Loại</th>
                <th className="py-2 px-3">Dữ liệu chi tiết</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 text-slate-300">
              {events.length === 0 ? (
                <tr>
                  <td colSpan={4} className="py-6 text-center text-slate-500">
                    Chưa có sự kiện nào trong luồng.
                  </td>
                </tr>
              ) : (
                events.map((evt, idx) => (
                  <tr key={`${evt.seq}-${idx}`} className="hover:bg-slate-800/30">
                    <td className="py-2 px-3 text-slate-500">#{evt.seq}</td>
                    <td className="py-2 px-3 text-slate-400">
                      {formatTimeOnly(evt.ts)}
                    </td>
                    <td className="py-2 px-3">
                      <span className="px-1.5 py-0.5 rounded text-[10px] bg-slate-800 text-sky-300 border border-slate-700">
                        {evt.type}
                      </span>
                    </td>
                    <td className="py-2 px-3 text-slate-300 truncate max-w-md">
                      {JSON.stringify(evt.data)}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
