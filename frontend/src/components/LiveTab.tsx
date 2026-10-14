import React, { useEffect, useState, useCallback, useMemo } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
  type ChartOptions,
  type ChartData,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import {
  Radio,
  Play,
  Pause,
  FastForward,
  Clock,
  AlertTriangle,
  Info,
  Activity,
  RefreshCw,
} from 'lucide-react';
import { api, isMockMode, onMockChange } from '../services/api';
import { useStream } from '../context/useStream';
import type { AqiReading, ReplayStatus } from '../types';
import {
  formatDateTime,
  formatTimeOnly,
  formatChartTime,
  formatDataAge,
  getCoStatusBadge,
} from '../utils/format';

// Đăng ký các module ChartJS
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

export const LiveTab: React.FC = () => {
  const {
    latestReplayReading: streamReplayReading,
    latestLiveReading: streamLiveReading,
    replayHistory,
    liveHistory,
    latestKafkaStats,
    latestSparkProgress,
  } = useStream();

  // State cục bộ cho Replay
  const [initialReplay, setInitialReplay] = useState<AqiReading | null>(null);
  const [replayStatus, setReplayStatus] = useState<ReplayStatus | null>(null);
  const [replayLoading, setReplayLoading] = useState<boolean>(false);
  const [replayControlError, setReplayControlError] = useState<string | null>(null);

  // State cục bộ cho Live
  const [initialLive, setInitialLive] = useState<AqiReading | null>(null);
  const [liveLoading, setLiveLoading] = useState<boolean>(false);

  // Phát hiện mock server
  const [isMock, setIsMock] = useState<boolean>(() => isMockMode());
  useEffect(() => {
    return onMockChange(setIsMock);
  }, []);

  // Timer cập nhật tuổi dữ liệu mỗi 15 giây
  const [currentTime, setCurrentTime] = useState<number>(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setCurrentTime(Date.now()), 15000);
    return () => clearInterval(timer);
  }, []);

  // Nạp trạng thái phát lại
  const fetchReplayStatus = useCallback(async () => {
    try {
      const status = await api.getReplayStatus();
      setReplayStatus(status);
    } catch {
      // Backend thật trước A1 trả 404, mock trả 200
    }
  }, []);

  // Nạp dữ liệu ban đầu từ REST API khi bấm nút "Tải lại API"
  const loadInitialData = useCallback(async () => {
    setReplayLoading(true);
    setLiveLoading(true);
    try {
      const [rLatest, rStatus, lLatest] = await Promise.allSettled([
        api.getAqiLatest('replay-dataset'),
        api.getReplayStatus(),
        api.getAqiLatest('live-openweather'),
      ]);
      if (rLatest.status === 'fulfilled') {
        setInitialReplay(rLatest.value);
      } else {
        setInitialReplay(null);
      }
      if (rStatus.status === 'fulfilled') {
        setReplayStatus(rStatus.value);
      }
      if (lLatest.status === 'fulfilled') {
        setInitialLive(lLatest.value);
      } else {
        setInitialLive(null);
      }
    } finally {
      setReplayLoading(false);
      setLiveLoading(false);
    }
  }, []);

  // Tải dữ liệu lần đầu khi mount
  useEffect(() => {
    let ignore = false;
    Promise.allSettled([
      api.getAqiLatest('replay-dataset'),
      api.getReplayStatus(),
      api.getAqiLatest('live-openweather'),
    ]).then(([rLatest, rStatus, lLatest]) => {
      if (ignore) return;
      if (rLatest.status === 'fulfilled') {
        setInitialReplay(rLatest.value);
      } else {
        setInitialReplay(null);
      }
      if (rStatus.status === 'fulfilled') {
        setReplayStatus(rStatus.value);
      }
      if (lLatest.status === 'fulfilled') {
        setInitialLive(lLatest.value);
      } else {
        setInitialLive(null);
      }
    });

    return () => {
      ignore = true;
    };
  }, []);

  // Polling định kỳ cho Replay status mỗi 4 giây
  useEffect(() => {
    const timer = setInterval(() => {
      fetchReplayStatus();
    }, 4000);
    return () => clearInterval(timer);
  }, [fetchReplayStatus]);

  // Bản ghi mới nhất hiển thị
  const activeReplayReading = streamReplayReading || initialReplay;
  const activeLiveReading = streamLiveReading || initialLive;

  // Xử lý điều khiển Replay (Pause / Resume / Speed)
  const handleControlReplay = async (action: 'pause' | 'resume', speed?: 1 | 10 | 60) => {
    try {
      setReplayControlError(null);
      const updated = await api.controlReplay({
        action,
        speed: speed ?? replayStatus?.speed ?? 10,
      });
      setReplayStatus(updated);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setReplayControlError(msg);
    }
  };

  // Chuẩn bị dữ liệu biểu đồ Replay
  const replayChartData = useMemo(() => {
    // Ưu tiên lịch sử nhận từ stream, nếu chưa có thì dùng initial reading
    const points: AqiReading[] = replayHistory.length > 0
      ? replayHistory
      : activeReplayReading
      ? [activeReplayReading]
      : [];

    const labels = points.map((p) =>
      p.originalDatetime ? formatChartTime(p.originalDatetime) : formatTimeOnly(p.datetime)
    );

    const actualCo = points.map((p) => p.co);
    const predictedCo = points.map((p) => (p.coPredicted !== null ? p.coPredicted : null));
    const hasPredicted = points.some((p) => p.coPredicted !== null);

    const datasets: ChartData<'line'>['datasets'] = [
      {
        label: 'CO Thực tế (Mô phỏng)',
        data: actualCo,
        borderColor: '#a855f7', // Tím tím cho mô phỏng
        backgroundColor: 'rgba(168, 85, 247, 0.1)',
        fill: true,
        tension: 0.25,
        pointRadius: points.length === 1 ? 5 : 3,
        pointHoverRadius: 6,
      },
    ];

    if (hasPredicted) {
      datasets.push({
        label: `CO Dự đoán (${points.find((p) => p.modelVersion)?.modelVersion || 'ML'})`,
        data: predictedCo as (number | null)[],
        borderColor: '#38bdf8', // Xanh sky
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.25,
        borderDash: [5, 5],
        pointRadius: points.length === 1 ? 4 : 2.5,
        pointHoverRadius: 5,
      });
    }

    return { labels, datasets };
  }, [replayHistory, activeReplayReading]);

  // Chuẩn bị dữ liệu biểu đồ Live
  const liveChartData = useMemo(() => {
    const points: AqiReading[] = liveHistory.length > 0
      ? liveHistory
      : activeLiveReading
      ? [activeLiveReading]
      : [];

    const labels = points.map((p) => formatChartTime(p.datetime));
    const actualCo = points.map((p) => p.co);
    const predictedCo = points.map((p) => (p.coPredicted !== null ? p.coPredicted : null));
    const hasPredicted = points.some((p) => p.coPredicted !== null);

    const datasets: ChartData<'line'>['datasets'] = [
      {
        label: 'CO Quan sát Thật',
        data: actualCo,
        borderColor: '#06b6d4', // Cyan cho dữ liệu thật
        backgroundColor: 'rgba(6, 182, 212, 0.1)',
        fill: true,
        tension: 0.25,
        pointRadius: points.length === 1 ? 5 : 3,
        pointHoverRadius: 6,
      },
    ];

    if (hasPredicted) {
      datasets.push({
        label: `CO Dự đoán (${points.find((p) => p.modelVersion)?.modelVersion || 'ML'})`,
        data: predictedCo as (number | null)[],
        borderColor: '#f59e0b', // Vàng amber
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.25,
        borderDash: [5, 5],
        pointRadius: points.length === 1 ? 4 : 2.5,
        pointHoverRadius: 5,
      });
    }

    return { labels, datasets };
  }, [liveHistory, activeLiveReading]);

  const commonChartOptions: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      mode: 'index',
      intersect: false,
    },
    plugins: {
      legend: {
        position: 'top',
        labels: {
          color: '#94a3b8',
          font: { size: 11 },
          boxWidth: 12,
        },
      },
      tooltip: {
        backgroundColor: '#0f172a',
        titleColor: '#f8fafc',
        bodyColor: '#e2e8f0',
        borderColor: '#334155',
        borderWidth: 1,
        padding: 8,
        callbacks: {
          label: (context) => {
            if (context.parsed.y !== null) {
              return `${context.dataset.label}: ${context.parsed.y.toFixed(1)} µg/m³`;
            }
            return '';
          },
        },
      },
    },
    scales: {
      x: {
        grid: { color: '#1e293b' },
        ticks: { color: '#94a3b8', font: { size: 10 }, maxRotation: 45 },
      },
      y: {
        grid: { color: '#1e293b' },
        ticks: { color: '#94a3b8', font: { size: 10 } },
        title: {
          display: true,
          text: 'CO (µg/m³)',
          color: '#64748b',
          font: { size: 10 },
        },
      },
    },
  };

  return (
    <div className="space-y-6 min-w-0 max-w-full">
      {/* Banner Tiêu đề & Thông báo Mock */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
              <Radio className="w-6 h-6 text-sky-400 animate-pulse" />
              Dữ liệu Sống Song Song: Phát Lại & Dữ Liệu Thật
            </h2>
          </div>
          <p className="text-sm text-slate-400 mt-1">
            Minh bạch hai luồng dữ liệu độc lập: Phát lại mô phỏng lịch sử (Kaggle) và Dữ liệu thật (OpenWeather API)
          </p>
        </div>

        <button
          onClick={loadInitialData}
          disabled={replayLoading || liveLoading}
          className="inline-flex items-center gap-2 px-3.5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs sm:text-sm font-medium rounded-lg border border-slate-700 transition disabled:opacity-50 self-start sm:self-auto"
        >
          <RefreshCw className={`w-4 h-4 ${replayLoading || liveLoading ? 'animate-spin' : ''}`} />
          Tải lại API
        </button>
      </div>

      {/* Cảnh báo dữ liệu mẫu nếu phát hiện Mock Server */}
      {isMock && (
        <div className="p-3.5 rounded-xl bg-indigo-500/10 border border-indigo-500/30 text-indigo-200 text-xs sm:text-sm flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Info className="w-4 h-4 text-indigo-400 shrink-0" />
            <span>
              <strong className="font-semibold text-indigo-300">[DỮ LIỆU MẪU MOCK]</strong> Đang kết nối Mock Server cổng 8010. Mọi số liệu và chỉ số là minh họa phục vụ kiểm thử giao diện.
            </span>
          </div>
          <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-indigo-500/20 text-indigo-300 shrink-0">
            X-Mock: true
          </span>
        </div>
      )}

      {/* Lưới 2 cột song song (Xếp dọc trên điện thoại) */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 min-w-0">
        {/* ========================================================= */}
        {/* CỘT TRÁI: MÔ PHỎNG: PHÁT LẠI DỮ LIỆU LỊCH SỬ               */}
        {/* ========================================================= */}
        <div className="rounded-2xl bg-slate-900/90 border border-purple-500/30 p-5 sm:p-6 shadow-xl flex flex-col justify-between space-y-5 min-w-0">
          <div>
            {/* Header Cột Trái */}
            <div className="flex flex-wrap items-center justify-between gap-2 pb-4 border-b border-slate-800/80">
              <div className="flex items-center gap-2.5">
                <span className="px-2.5 py-0.5 rounded-md text-xs font-bold uppercase tracking-wider bg-purple-500/20 text-purple-300 border border-purple-500/40 shadow-sm">
                  MÔ PHỎNG
                </span>
                <div>
                  <h3 className="text-base sm:text-lg font-bold text-white tracking-tight">
                    MÔ PHỎNG: phát lại dữ liệu lịch sử
                  </h3>
                  <div className="text-xs text-purple-400/90 font-mono">
                    source = replay-dataset
                  </div>
                </div>
              </div>

              {/* Huy hiệu trạng thái Replay */}
              {replayStatus && (
                <div className="flex items-center gap-1.5 text-xs">
                  <span
                    className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full font-medium ${
                      replayStatus.state === 'running'
                        ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/30'
                        : replayStatus.state === 'paused'
                        ? 'bg-amber-500/10 text-amber-400 border border-amber-500/30'
                        : 'bg-rose-500/10 text-rose-400 border border-rose-500/30'
                    }`}
                  >
                    <span
                      className={`w-1.5 h-1.5 rounded-full ${
                        replayStatus.state === 'running'
                          ? 'bg-emerald-400 animate-pulse'
                          : replayStatus.state === 'paused'
                          ? 'bg-amber-400'
                          : 'bg-rose-400'
                      }`}
                    />
                    {replayStatus.state === 'running'
                      ? 'Đang phát'
                      : replayStatus.state === 'paused'
                      ? 'Đã tạm dừng'
                      : 'Đã dừng'}
                  </span>
                  <span className="px-1.5 py-0.5 rounded bg-slate-800 text-slate-300 font-mono text-[11px]">
                    {replayStatus.speed}×
                  </span>
                </div>
              )}
            </div>

            {/* Bảng điều khiển tốc độ & Tạm dừng/Tiếp tục (/api/replay/control) */}
            <div className="mt-4 p-3.5 rounded-xl bg-slate-950/60 border border-slate-800 space-y-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                  <FastForward className="w-4 h-4 text-purple-400" />
                  Điều khiển phát lại:
                </span>
                {replayStatus && (
                  <span className="text-[11px] text-slate-400 font-mono">
                    Đã phát: {replayStatus.emittedCount} bản ghi • Chu kỳ: {replayStatus.intervalSeconds}s
                  </span>
                )}
              </div>

              <div className="flex flex-wrap items-center gap-2">
                {/* Nút Pause / Resume */}
                <button
                  onClick={() =>
                    handleControlReplay(
                      replayStatus?.state === 'paused' ? 'resume' : 'pause'
                    )
                  }
                  disabled={replayStatus ? !replayStatus.controllable : false}
                  className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition ${
                    replayStatus?.state === 'paused'
                      ? 'bg-emerald-600 hover:bg-emerald-500 text-white shadow-sm'
                      : 'bg-amber-600/80 hover:bg-amber-600 text-white shadow-sm'
                  } disabled:opacity-40 disabled:cursor-not-allowed`}
                >
                  {replayStatus?.state === 'paused' ? (
                    <>
                      <Play className="w-3.5 h-3.5 fill-current" />
                      Tiếp tục phát
                    </>
                  ) : (
                    <>
                      <Pause className="w-3.5 h-3.5 fill-current" />
                      Tạm dừng
                    </>
                  )}
                </button>

                {/* Chọn tốc độ: 1x, 10x, 60x */}
                <div className="flex items-center rounded-lg bg-slate-900 border border-slate-800 p-0.5 text-xs">
                  {([1, 10, 60] as const).map((s) => (
                    <button
                      key={s}
                      onClick={() =>
                        handleControlReplay(
                          replayStatus?.state === 'paused' ? 'pause' : 'resume',
                          s
                        )
                      }
                      disabled={replayStatus ? !replayStatus.controllable : false}
                      className={`px-2.5 py-1 rounded font-medium transition ${
                        replayStatus?.speed === s
                          ? 'bg-purple-500/20 text-purple-300 font-bold border border-purple-500/40'
                          : 'text-slate-400 hover:text-slate-200'
                      } disabled:opacity-40 disabled:cursor-not-allowed`}
                    >
                      {s}×
                    </button>
                  ))}
                </div>

                {replayStatus?.controllable === false && replayStatus.detail && (
                  <span className="text-[11px] text-amber-400 italic">
                    {replayStatus.detail}
                  </span>
                )}
              </div>

              {replayControlError && (
                <div className="text-xs text-rose-400 flex items-center gap-1.5 bg-rose-500/10 p-2 rounded border border-rose-500/20">
                  <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                  <span>{replayControlError}</span>
                </div>
              )}
            </div>

            {/* Thẻ bản ghi mới nhất của Luồng Mô phỏng */}
            <div className="mt-4 p-4 rounded-xl bg-purple-950/20 border border-purple-500/30 space-y-3">
              <div className="flex items-center justify-between text-xs text-slate-400">
                <span className="font-semibold text-purple-300">Bản ghi phát lại mới nhất</span>
                {activeReplayReading && (
                  <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-purple-500/20 text-purple-300 border border-purple-500/30">
                    Trạm: {activeReplayReading.station} {activeReplayReading.isSimulatedStation && '(Mô phỏng)'}
                  </span>
                )}
              </div>

              {activeReplayReading ? (
                <div>
                  <div className="flex flex-wrap items-baseline gap-3">
                    <span className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight">
                      {activeReplayReading.co.toFixed(1)}
                    </span>
                    <span className="text-sm font-semibold text-slate-400">µg/m³ CO</span>

                    {/* Huy hiệu cảnh báo từ backend */}
                    <span
                      className={`px-2.5 py-1 rounded-full text-xs font-bold ${
                        getCoStatusBadge(activeReplayReading.status).badgeClass
                      }`}
                    >
                      {getCoStatusBadge(activeReplayReading.status).label}
                    </span>
                  </div>

                  {/* CO Dự đoán nếu có */}
                  {activeReplayReading.coPredicted !== null ? (
                    <div className="mt-2 text-xs text-slate-300 flex items-center gap-2">
                      <span className="text-slate-400">CO Mô hình Dự đoán:</span>
                      <span className="font-bold text-sky-400 text-sm">
                        {activeReplayReading.coPredicted.toFixed(1)} µg/m³
                      </span>
                      {activeReplayReading.modelVersion && (
                        <span className="px-1.5 py-0.2 rounded text-[10px] bg-sky-500/10 text-sky-400 font-mono border border-sky-500/20">
                          {activeReplayReading.modelVersion}
                        </span>
                      )}
                    </div>
                  ) : (
                    <div className="mt-2 text-xs text-slate-500 italic">
                      Chưa có dự đoán cho bản ghi này (coPredicted: null)
                    </div>
                  )}

                  {/* Thời điểm gốc (originalDatetime) — Yêu cầu cốt lõi F1 */}
                  <div className="mt-3 pt-3 border-t border-purple-500/20 space-y-1.5 text-xs">
                    <div className="flex items-start gap-1.5 text-amber-300 font-medium">
                      <Clock className="w-3.5 h-3.5 shrink-0 mt-0.5 text-amber-400" />
                      <div>
                        <span>Thời điểm đo gốc trong lịch sử: </span>
                        <strong className="text-amber-200 font-bold font-mono text-sm">
                          {activeReplayReading.originalDatetime
                            ? formatDateTime(activeReplayReading.originalDatetime)
                            : 'Chưa có thông tin giờ gốc'}
                        </strong>
                      </div>
                    </div>
                    <div className="text-[11px] text-slate-400 flex items-center gap-1">
                      <span>Thời điểm phát lại lúc:</span>
                      <span className="font-mono text-slate-300">
                        {formatDateTime(activeReplayReading.datetime)}
                      </span>
                      <span className="text-slate-500 italic">
                        (giờ máy phát, không phải thời điểm đo)
                      </span>
                    </div>
                  </div>

                  {/* Thông số phụ */}
                  <div className="mt-3 grid grid-cols-3 gap-2 pt-2 border-t border-purple-500/10 text-center">
                    <div className="p-1.5 rounded bg-slate-900/60 border border-slate-800">
                      <div className="text-[10px] text-slate-400">Gió</div>
                      <div className="text-xs font-semibold text-slate-200">
                        {activeReplayReading.windSpeedKmh} km/h
                      </div>
                    </div>
                    <div className="p-1.5 rounded bg-slate-900/60 border border-slate-800">
                      <div className="text-[10px] text-slate-400">Nhiệt độ</div>
                      <div className="text-xs font-semibold text-slate-200">
                        {activeReplayReading.temperatureC} °C
                      </div>
                    </div>
                    <div className="p-1.5 rounded bg-slate-900/60 border border-slate-800">
                      <div className="text-[10px] text-slate-400">Tích lũy CO</div>
                      <div className="text-xs font-semibold text-slate-200">
                        {activeReplayReading.coAccumulationIndex}
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="py-6 text-center text-xs text-slate-500">
                  Đang nạp bản ghi phát lại...
                </div>
              )}
            </div>

            {/* Biểu đồ riêng cột Mô phỏng */}
            <div className="mt-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                  <Activity className="w-3.5 h-3.5 text-purple-400" />
                  Diễn biến CO Luồng Phát Lại (Mô phỏng)
                </span>
                <span className="text-[11px] text-purple-400 font-mono">
                  {replayChartData.labels.length} điểm
                </span>
              </div>
              <div className="h-56 sm:h-64 w-full bg-slate-950/40 p-2 rounded-xl border border-slate-800">
                {replayChartData.labels.length > 0 ? (
                  <Line data={replayChartData} options={commonChartOptions} />
                ) : (
                  <div className="h-full flex items-center justify-center text-xs text-slate-500">
                    Chưa có điểm dữ liệu để vẽ biểu đồ
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Dòng trạng thái dưới đáy Cột Trái (Kafka offset & Spark batch) */}
          <div className="mt-4 pt-3 border-t border-slate-800/80 text-[11px] text-slate-400 flex flex-wrap items-center justify-between gap-2 font-mono bg-slate-950/40 p-2.5 rounded-lg border border-slate-800">
            <div className="flex items-center gap-1.5">
              <span className="w-2 h-2 rounded-full bg-purple-400" />
              <span>Kafka Topic:</span>
              <span className="text-slate-200 font-semibold">
                {latestKafkaStats?.topic || 'hcmc_co_stream'}
              </span>
              <span>• Lag:</span>
              <span className="text-slate-200 font-semibold">
                {latestKafkaStats?.lagTotal ?? 0}
              </span>
            </div>
            <div className="flex items-center gap-1.5">
              <span>Spark Micro-batch:</span>
              <span className="text-slate-200 font-semibold">
                #{latestSparkProgress?.batchId ?? '—'}
              </span>
              <span className="text-slate-500">(ước tính)</span>
            </div>
          </div>
        </div>

        {/* ========================================================= */}
        {/* CỘT PHẢI: THẬT: OPENWEATHER                                */}
        {/* ========================================================= */}
        <div className="rounded-2xl bg-slate-900/90 border border-cyan-500/30 p-5 sm:p-6 shadow-xl flex flex-col justify-between space-y-5 min-w-0">
          <div>
            {/* Header Cột Phải */}
            <div className="flex flex-wrap items-center justify-between gap-2 pb-4 border-b border-slate-800/80">
              <div className="flex items-center gap-2.5">
                <span className="px-2.5 py-0.5 rounded-md text-xs font-bold uppercase tracking-wider bg-cyan-500/20 text-cyan-300 border border-cyan-500/40 shadow-sm">
                  THẬT
                </span>
                <div>
                  <h3 className="text-base sm:text-lg font-bold text-white tracking-tight">
                    THẬT: OpenWeather
                  </h3>
                  <div className="text-xs text-cyan-400/90 font-mono">
                    source = live-openweather
                  </div>
                </div>
              </div>

              {/* Tuổi dữ liệu quan sát */}
              {activeLiveReading ? (
                <div className="flex items-center gap-1.5 text-xs text-cyan-300 font-mono bg-cyan-500/10 px-2.5 py-1 rounded-full border border-cyan-500/30">
                  <Clock className="w-3.5 h-3.5 text-cyan-400" />
                  <span>{formatDataAge(activeLiveReading.datetime, currentTime)}</span>
                </div>
              ) : (
                <div className="flex items-center gap-1.5 text-xs text-slate-500 italic bg-slate-800/50 px-2.5 py-1 rounded-full border border-slate-700/50">
                  <span>Chưa có dữ liệu</span>
                </div>
              )}
            </div>

            {/* Không có điều khiển tốc độ — Ghi chú minh bạch */}
            <div className="mt-4 p-3.5 rounded-xl bg-slate-950/60 border border-slate-800 text-xs text-slate-400 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Radio className="w-4 h-4 text-cyan-400" />
                <span>
                  Luồng dữ liệu thật cập nhật theo định kỳ của API OpenWeather (không điều khiển tốc độ phát lại).
                </span>
              </div>
              <span className="px-2 py-0.5 rounded text-[10px] bg-slate-800 text-slate-400 font-mono shrink-0">
                Live stream
              </span>
            </div>

            {/* Thẻ bản ghi mới nhất / Hoặc Trạng thái Rỗng Tường Minh */}
            <div className="mt-4 p-4 rounded-xl bg-cyan-950/20 border border-cyan-500/30 space-y-3 min-w-0">
              <div className="flex items-center justify-between text-xs text-slate-400">
                <span className="font-semibold text-cyan-300">Bản ghi quan sát thực tế</span>
                {activeLiveReading && (
                  <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-cyan-500/20 text-cyan-300 border border-cyan-500/30">
                    {activeLiveReading.station} {activeLiveReading.isSimulatedStation && '(Tọa độ TP.HCM)'}
                  </span>
                )}
              </div>

              {activeLiveReading ? (
                <div>
                  <div className="flex flex-wrap items-baseline gap-3">
                    <span className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight">
                      {activeLiveReading.co.toFixed(1)}
                    </span>
                    <span className="text-sm font-semibold text-slate-400">µg/m³ CO</span>

                    {/* Huy hiệu cảnh báo từ backend */}
                    <span
                      className={`px-2.5 py-1 rounded-full text-xs font-bold ${
                        getCoStatusBadge(activeLiveReading.status).badgeClass
                      }`}
                    >
                      {getCoStatusBadge(activeLiveReading.status).label}
                    </span>
                  </div>

                  {/* CO Dự đoán nếu có */}
                  {activeLiveReading.coPredicted !== null ? (
                    <div className="mt-2 text-xs text-slate-300 flex items-center gap-2">
                      <span className="text-slate-400">CO Mô hình Dự đoán:</span>
                      <span className="font-bold text-amber-400 text-sm">
                        {activeLiveReading.coPredicted.toFixed(1)} µg/m³
                      </span>
                      {activeLiveReading.modelVersion && (
                        <span className="px-1.5 py-0.2 rounded text-[10px] bg-amber-500/10 text-amber-400 font-mono border border-amber-500/20">
                          {activeLiveReading.modelVersion}
                        </span>
                      )}
                    </div>
                  ) : (
                    <div className="mt-2 text-xs text-slate-500 italic">
                      Chưa có dự đoán cho bản ghi này (coPredicted: null)
                    </div>
                  )}

                  {/* Thời điểm quan sát thật */}
                  <div className="mt-3 pt-3 border-t border-cyan-500/20 space-y-1.5 text-xs">
                    <div className="flex items-start gap-1.5 text-cyan-200 font-medium">
                      <Clock className="w-3.5 h-3.5 shrink-0 mt-0.5 text-cyan-400" />
                      <div>
                        <span>Thời điểm quan sát thật: </span>
                        <strong className="text-cyan-100 font-bold font-mono text-sm">
                          {formatDateTime(activeLiveReading.datetime)}
                        </strong>
                      </div>
                    </div>
                    <div className="text-[11px] text-slate-400 flex items-center gap-1">
                      <span>Tình trạng cập nhật:</span>
                      <span className="text-cyan-300 font-medium">
                        {formatDataAge(activeLiveReading.datetime, currentTime)}
                      </span>
                    </div>
                  </div>

                  {/* Thông số phụ */}
                  <div className="mt-3 grid grid-cols-3 gap-2 pt-2 border-t border-cyan-500/10 text-center">
                    <div className="p-1.5 rounded bg-slate-900/60 border border-slate-800">
                      <div className="text-[10px] text-slate-400">Gió</div>
                      <div className="text-xs font-semibold text-slate-200">
                        {activeLiveReading.windSpeedKmh} km/h
                      </div>
                    </div>
                    <div className="p-1.5 rounded bg-slate-900/60 border border-slate-800">
                      <div className="text-[10px] text-slate-400">Nhiệt độ</div>
                      <div className="text-xs font-semibold text-slate-200">
                        {activeLiveReading.temperatureC} °C
                      </div>
                    </div>
                    <div className="p-1.5 rounded bg-slate-900/60 border border-slate-800">
                      <div className="text-[10px] text-slate-400">Tích lũy CO</div>
                      <div className="text-xs font-semibold text-slate-200">
                        {activeLiveReading.coAccumulationIndex}
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                /* TRẠNG THÁI RỖNG TƯỜNG MINH KHI CHƯA CÓ BẢN GHI — QUY TẮC BẮT BUỘC */
                <div className="py-8 px-4 text-center space-y-3">
                  <div className="w-12 h-12 mx-auto rounded-full bg-slate-800/80 border border-slate-700 flex items-center justify-center text-slate-400">
                    <Radio className="w-6 h-6 text-slate-500" />
                  </div>
                  <div>
                    <h4 className="text-sm font-semibold text-slate-300">
                      Chưa có bản ghi nào từ nguồn dữ liệu thật OpenWeather
                    </h4>
                    <p className="text-xs text-slate-400 mt-1 max-w-sm mx-auto">
                      Hệ thống đang chờ quan sát CO từ OpenWeather Air Pollution API (mã 404 NO_READING_YET). Tuyệt đối không điền số giả.
                    </p>
                  </div>
                  <div className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-[11px] font-mono bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">
                    <RefreshCw className="w-3 h-3 animate-spin" />
                    Đang lắng nghe luồng sự kiện...
                  </div>
                </div>
              )}
            </div>

            {/* Biểu đồ riêng cột Dữ liệu Thật */}
            <div className="mt-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                  <Activity className="w-3.5 h-3.5 text-cyan-400" />
                  Diễn biến CO Dữ Liệu Thật (OpenWeather)
                </span>
                <span className="text-[11px] text-cyan-400 font-mono">
                  {liveChartData.labels.length} điểm
                </span>
              </div>
              <div className="h-56 sm:h-64 w-full bg-slate-950/40 p-2 rounded-xl border border-slate-800">
                {liveChartData.labels.length > 0 ? (
                  <Line data={liveChartData} options={commonChartOptions} />
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-xs text-slate-500 space-y-2 p-4 text-center">
                    <Activity className="w-6 h-6 text-slate-600" />
                    <span>Chưa có dữ liệu để vẽ biểu đồ</span>
                    <span className="text-[11px] text-slate-600">
                      Biểu đồ sẽ tự động hiển thị khi bản ghi OpenWeather đầu tiên đi qua luồng Kafka
                    </span>
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Dòng trạng thái dưới đáy Cột Phải (Kafka offset & Spark batch) */}
          <div className="mt-4 pt-3 border-t border-slate-800/80 text-[11px] text-slate-400 flex flex-wrap items-center justify-between gap-2 font-mono bg-slate-950/40 p-2.5 rounded-lg border border-slate-800">
            <div className="flex items-center gap-1.5">
              <span className="w-2 h-2 rounded-full bg-cyan-400" />
              <span>Kafka Producer:</span>
              <span className="text-slate-200 font-semibold">
                OpenWeather API
              </span>
              <span>• Topic:</span>
              <span className="text-slate-200 font-semibold">
                {latestKafkaStats?.topic || 'hcmc_co_stream'}
              </span>
            </div>
            <div className="flex items-center gap-1.5">
              <span>Spark Structured Streaming:</span>
              <span className="text-slate-200 font-semibold">
                {latestSparkProgress ? `Batch #${latestSparkProgress.batchId}` : 'Sẵn sàng'}
              </span>
              <span className="text-slate-500">(Parquet /live)</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default LiveTab;
