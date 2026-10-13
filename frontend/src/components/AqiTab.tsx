import React, { useEffect, useState, useCallback } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import {
  TrendingUp,
  Clock,
  AlertTriangle,
  RefreshCw,
  Layers,
  Filter,
  Info,
  Calendar,
} from 'lucide-react';
import { api } from '../services/api';
import type { AqiSeries, HourlyProfile } from '../types';


// Đăng ký các module ChartJS
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

export const AqiTab: React.FC = () => {
  const [series, setSeries] = useState<AqiSeries | null>(null);
  const [hourlyProfile, setHourlyProfile] = useState<HourlyProfile | null>(null);
  const [granularity, setGranularity] = useState<'month' | 'day' | 'hour'>('month');
  const [metric, setMetric] = useState<string>('co');
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const loadData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const [seriesRes, profileRes] = await Promise.all([
        api.getAqiSeries({ metric, granularity }),
        api.getAqiHourlyProfile(),
      ]);
      setSeries(seriesRes);
      setHourlyProfile(profileRes);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(`Không thể nạp chuỗi dữ liệu CO: ${msg}`);
    } finally {
      setLoading(false);
    }
  }, [granularity, metric]);

  useEffect(() => {
    let ignore = false;
    Promise.all([
      api.getAqiSeries({ metric, granularity }),
      api.getAqiHourlyProfile(),
    ])
      .then(([seriesRes, profileRes]) => {
        if (!ignore) {
          setSeries(seriesRes);
          setHourlyProfile(profileRes);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!ignore) {
          const msg = err instanceof Error ? err.message : String(err);
          setError(`Không thể nạp chuỗi dữ liệu CO: ${msg}`);
          setLoading(false);
        }
      });

    return () => {
      ignore = true;
    };
  }, [granularity, metric]);

  // Chuẩn bị dữ liệu cho biểu đồ Chuỗi Thời gian (AqiSeries)
  // Quy ước bắt buộc: ĐÁNH DẤU VÀ TÁCH BIỆT TẠI ĐIỂM GÃY 01/07/2025, KHÔNG ĐƯỢC NỐI MƯỢT!
  const prepareSeriesChartData = () => {
    if (!series || !series.points.length) {
      return { labels: [], datasets: [] };
    }

    const labels = series.points.map((p) => {
      const d = new Date(p.t);
      if (granularity === 'month') {
        return `T${d.getMonth() + 1}/${d.getFullYear()}`;
      }
      return `${d.getDate()}/${d.getMonth() + 1}/${d.getFullYear()}`;
    });

    const breakDate = new Date('2025-07-01T00:00:00+07:00').getTime();

    // Tách thành 2 series: Trước điểm gãy và Sau điểm gãy để KHÔNG nối mượt qua 01/07/2025
    const beforeBreakValues: (number | null)[] = [];
    const afterBreakValues: (number | null)[] = [];

    series.points.forEach((p) => {
      const ptTime = new Date(p.t).getTime();
      if (ptTime < breakDate) {
        beforeBreakValues.push(p.value);
        afterBreakValues.push(null);
      } else {
        beforeBreakValues.push(null);
        afterBreakValues.push(p.value);
      }
    });

    return {
      labels,
      datasets: [
        {
          label: `Trước 01/07/2025 (${series.metric.toUpperCase()} - ${series.unit})`,
          data: beforeBreakValues,
          borderColor: '#f97316', // Cam rực
          backgroundColor: 'rgba(249, 115, 22, 0.1)',
          fill: true,
          tension: 0, // Không làm mịn spline nhân tạo
          spanGaps: false,
          pointRadius: 4,
          pointHoverRadius: 6,
          pointBackgroundColor: '#f97316',
        },
        {
          label: `Từ 01/07/2025 (${series.metric.toUpperCase()} - ${series.unit})`,
          data: afterBreakValues,
          borderColor: '#0284c7', // Sky blue
          backgroundColor: 'rgba(2, 132, 199, 0.1)',
          fill: true,
          tension: 0,
          spanGaps: false,
          pointRadius: 4,
          pointHoverRadius: 6,
          pointBackgroundColor: '#0284c7',
        },
      ],
    };
  };

  // Chuẩn bị dữ liệu cho biểu đồ Nhịp theo Giờ trong Ngày (HourlyProfile: 0h - 23h)
  const prepareProfileChartData = () => {
    if (!hourlyProfile || !hourlyProfile.profiles.length) {
      return { labels: [], datasets: [] };
    }

    const hours = Array.from({ length: 24 }, (_, i) => `${i}h`);

    const colors = [
      { border: '#f43f5e', bg: 'rgba(244, 63, 94, 0.1)' }, // Hồng đỏ cho trước 01/07/2025
      { border: '#38bdf8', bg: 'rgba(56, 189, 248, 0.1)' }, // Sky blue cho từ 01/07/2025
    ];

    const datasets = hourlyProfile.profiles.map((p, idx) => ({
      label: p.label,
      data: p.values,
      borderColor: colors[idx % colors.length].border,
      backgroundColor: colors[idx % colors.length].bg,
      tension: 0.2,
      pointRadius: 3,
      pointHoverRadius: 5,
    }));

    return {
      labels: hours,
      datasets,
    };
  };

  const seriesData = prepareSeriesChartData();
  const profileData = prepareProfileChartData();

  return (
    <div className="space-y-6 min-w-0 max-w-full">
      {/* Header & Controls */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
            <TrendingUp className="w-6 h-6 text-sky-400" />
            Phân tích Dữ liệu Lịch sử Khí CO TP.HCM
          </h2>
          <p className="text-sm text-slate-400">
            Chuỗi thời gian, chu kỳ 24 giờ và nhận diện điểm gãy dữ liệu (01/07/2025)
          </p>
        </div>

        {/* Filter Controls */}
        <div className="flex flex-wrap items-center gap-2 self-start sm:self-auto">
          <div className="flex items-center gap-1 bg-slate-900 border border-slate-800 rounded-lg p-1 text-xs">
            <Filter className="w-3.5 h-3.5 text-slate-500 ml-1.5" />
            <select
              value={metric}
              onChange={(e) => setMetric(e.target.value)}
              className="bg-transparent text-slate-200 py-1 px-2 focus:outline-none"
            >
              <option value="co" className="bg-slate-900">CO (Carbon Monoxide)</option>
              <option value="no2" className="bg-slate-900">NO2</option>
              <option value="o3" className="bg-slate-900">O3</option>
              <option value="so2" className="bg-slate-900">SO2</option>
              <option value="pm2_5" className="bg-slate-900">PM2.5</option>
              <option value="pm10" className="bg-slate-900">PM10</option>
            </select>
          </div>

          <div className="flex items-center gap-1 bg-slate-900 border border-slate-800 rounded-lg p-1 text-xs">
            <Calendar className="w-3.5 h-3.5 text-slate-500 ml-1.5" />
            {(['month', 'day', 'hour'] as const).map((g) => (
              <button
                key={g}
                onClick={() => setGranularity(g)}
                className={`px-2.5 py-1 rounded font-medium transition ${
                  granularity === g
                    ? 'bg-sky-500/20 text-sky-400 font-bold'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {g === 'month' ? 'Tháng' : g === 'day' ? 'Ngày' : 'Giờ'}
              </button>
            ))}
          </div>

          <button
            onClick={loadData}
            disabled={loading}
            className="p-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg border border-slate-700 disabled:opacity-50"
            title="Làm mới dữ liệu"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm flex items-start gap-3">
          <AlertTriangle className="w-5 h-5 shrink-0 text-rose-400 mt-0.5" />
          <div>{error}</div>
        </div>
      )}

      {/* CẢNH BÁO BẮT BUỘC: ĐIỂM GÃY DỮ LIỆU NGÀY 01/07/2025 */}
      <div className="p-4 rounded-2xl bg-gradient-to-r from-amber-500/15 via-rose-500/15 to-transparent border border-amber-500/30 text-amber-200 text-xs sm:text-sm space-y-1.5 shadow-lg">
        <div className="flex items-center gap-2 font-bold text-amber-300 text-sm sm:text-base">
          <AlertTriangle className="w-5 h-5 text-amber-400 shrink-0" />
          <span>Lưu ý: Điểm gãy dữ liệu ngày 01/07/2025</span>
        </div>
        <p className="text-slate-300 leading-relaxed">
          {hourlyProfile?.note ||
            'Mức CO giảm mạnh sau 01/07/2025 (điểm gãy có trong cả bản Kaggle lẫn bản thô OpenWeather).'}
        </p>
        <div className="font-semibold text-amber-400 pt-1">
          Biểu đồ dưới đây được tách đoạn rõ ràng và tuyệt đối KHÔNG nối mượt qua mốc 01/07/2025 nhằm đảm bảo tính trung thực dữ liệu.
        </div>
      </div>


      {/* Biểu đồ Chuỗi thời gian (AqiSeries) */}
      <div className="p-4 sm:p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-4 shadow-xl min-w-0 max-w-full">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800 pb-3 min-w-0">
          <div className="min-w-0">
            <h3 className="text-base font-bold text-white flex items-center gap-2">
              <span>Chuỗi thời gian Nồng độ {metric.toUpperCase()} ({series?.unit || 'µg/m³'})</span>
            </h3>
            <p className="text-xs text-slate-400 mt-0.5">
              Đơn vị: µg/m³ • Múi giờ: {series?.timezone || 'Asia/Ho_Chi_Minh (+07:00)'}
            </p>
          </div>
          <div className="text-xs text-slate-400 flex items-start sm:items-center gap-1.5 font-mono min-w-0 max-w-full">
            <Layers className="w-3.5 h-3.5 text-slate-500 shrink-0 mt-0.5 sm:mt-0" />
            <span className="shrink-0">Nguồn: </span>
            <span className="text-sky-400 font-semibold break-all">{series?.source || 'Đang tải...'}</span>
          </div>
        </div>

        <div className="h-72 sm:h-80 w-full">
          <Line
            data={seriesData}
            options={{
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
                  },
                },
                tooltip: {
                  backgroundColor: '#1e293b',
                  titleColor: '#f8fafc',
                  bodyColor: '#e2e8f0',
                  borderColor: '#334155',
                  borderWidth: 1,
                  padding: 10,
                  callbacks: {
                    label: (context) => {
                      if (context.parsed.y !== null) {
                        return `${context.dataset.label}: ${context.parsed.y.toFixed(1)} ${series?.unit || 'µg/m³'}`;
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
                    text: `${metric.toUpperCase()} (${series?.unit || 'µg/m³'})`,
                    color: '#64748b',
                  },
                },
              },
            }}
          />
        </div>
      </div>

      {/* Biểu đồ Nhịp theo Giờ trong Ngày (HourlyProfile) */}
      <div className="p-4 sm:p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-4 shadow-xl min-w-0 max-w-full">
        <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800 pb-3 min-w-0">
          <div>
            <h3 className="text-base font-bold text-white flex items-center gap-2">
              <Clock className="w-5 h-5 text-indigo-400" />
              <span>Nhịp Sinh học CO theo 24 Giờ trong Ngày (Hourly Profile)</span>
            </h3>
            <p className="text-xs text-slate-400 mt-0.5">
              So sánh chu kỳ biến thiên giờ cao điểm sáng - tối giữa 2 giai đoạn (giờ địa phương +07:00)
            </p>
          </div>
        </div>

        {hourlyProfile?.note && (
          <div className="p-3 bg-slate-800/40 rounded-xl text-xs text-slate-300 flex items-start gap-2 border border-slate-800">
            <Info className="w-4 h-4 text-sky-400 shrink-0 mt-0.5" />
            <div>{hourlyProfile.note}</div>
          </div>
        )}

        <div className="h-72 sm:h-80 w-full">
          <Line
            data={profileData}
            options={{
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
                  },
                },
                tooltip: {
                  backgroundColor: '#1e293b',
                  titleColor: '#f8fafc',
                  bodyColor: '#e2e8f0',
                  borderColor: '#334155',
                  borderWidth: 1,
                  padding: 10,
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
                  ticks: { color: '#94a3b8', font: { size: 11 } },
                  title: {
                    display: true,
                    text: 'Giờ trong ngày (0h đến 23h - Giờ Việt Nam UTC+7)',
                    color: '#64748b',
                  },
                },
                y: {
                  grid: { color: '#1e293b' },
                  ticks: { color: '#94a3b8', font: { size: 10 } },
                  title: {
                    display: true,
                    text: 'Nồng độ CO trung bình (µg/m³)',
                    color: '#64748b',
                  },
                },
              },
            }}
          />
        </div>
      </div>

      {/* Bảng Hướng dẫn & Lưu ý Quy ước Dữ liệu */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
        <div className="p-4 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
          <h4 className="font-bold text-slate-200 uppercase tracking-wider text-[11px]">
            1. Về Trạm đo & Dữ liệu Mô phỏng
          </h4>
          <p className="text-slate-400 leading-relaxed">
            Trong luồng phát lại (`replay`), tên các quận huyện (như Quận 1, Quận 3...) chỉ là nhãn gán luân phiên minh họa luồng vào của pipeline Big Data, không phải dữ liệu thực tế tại từng trạm cố định. Giao diện luôn hiển thị cờ cảnh báo khi `isSimulatedStation: true`.
          </p>
        </div>

        <div className="p-4 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
          <h4 className="font-bold text-slate-200 uppercase tracking-wider text-[11px]">
            2. Về Ngưỡng Đánh giá SAFE / WARNING / DANGER
          </h4>
          <p className="text-slate-400 leading-relaxed">
            Mức nồng độ CO 500 và 1000 µg/m³ được kế thừa trực tiếp từ bộ lọc cảnh báo của consumer mẫu (`spark_streaming_consumer.py`). Đây là ngưỡng xử lý dữ liệu của luồng pipeline kỹ thuật, không phải quy chuẩn y tế công cộng chính thức của WHO. Frontend chỉ hiển thị trạng thái do backend trả về.
          </p>
        </div>
      </div>
    </div>
  );
};
