import React from 'react';
import {
  Database,
  Calendar,
  AlertTriangle,
  TrendingDown,
  Info,
  ArrowDownRight,
  GitCompare,
} from 'lucide-react';
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
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import type { SqlAnalysis } from '../types';
import { formatDateTime } from '../utils/format';

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

interface AnalysisSqlSectionProps {
  data: SqlAnalysis | null;
  loading: boolean;
  error: string | null;
  isNotReady?: boolean;
}

const VARIABLE_LABELS: Record<string, { label: string; unit: string }> = {
  pm2_5: { label: 'Bụi mịn PM2.5', unit: 'µg/m³' },
  pm10: { label: 'Bụi PM10', unit: 'µg/m³' },
  no2: { label: 'Khí Nitơ Dioxit (NO₂)', unit: 'µg/m³' },
  no: { label: 'Khí Nitơ Monoxit (NO)', unit: 'µg/m³' },
  o3: { label: 'Khí Ozone (O₃)', unit: 'µg/m³' },
  so2: { label: 'Khí Lưu huỳnh Dioxit (SO₂)', unit: 'µg/m³' },
  nh3: { label: 'Khí Amoniac (NH₃)', unit: 'µg/m³' },
  wind_speed_kmh: { label: 'Tốc độ gió', unit: 'km/h' },
  temperature_C: { label: 'Nhiệt độ', unit: '°C' },
  humidity_pct: { label: 'Độ ẩm tương đối', unit: '%' },
  rain_mm: { label: 'Lượng mưa', unit: 'mm' },
};

export const AnalysisSqlSection: React.FC<AnalysisSqlSectionProps> = ({
  data,
  loading,
  error,
  isNotReady,
}) => {
  if (loading) {
    return (
      <div className="p-8 rounded-2xl bg-slate-900 border border-slate-800 text-center space-y-3 animate-pulse">
        <div className="inline-flex p-3 rounded-xl bg-slate-800 text-sky-400">
          <Database className="w-6 h-6 animate-spin" />
        </div>
        <p className="text-sm text-slate-400">Đang tải kết quả phân tích Spark SQL (Bước 5)...</p>
      </div>
    );
  }

  if (isNotReady) {
    return (
      <div className="p-6 rounded-2xl bg-slate-900/60 border border-slate-800 text-center space-y-3">
        <div className="inline-flex p-3 rounded-xl bg-amber-500/10 text-amber-400 border border-amber-500/20">
          <AlertTriangle className="w-6 h-6" />
        </div>
        <h4 className="text-sm font-bold text-white">Phân tích Spark SQL Chưa Sẵn Sàng (404 ANALYSIS_NOT_READY)</h4>
        <p className="text-xs text-slate-400 max-w-lg mx-auto leading-relaxed">
          Bước 5 phân tích thống kê chưa được thực thi trên cụm Big Data hoặc tệp <code>/co/output/sql_analysis.json</code> chưa được sinh ra.
        </p>
        <div className="text-[11px] text-slate-500 font-mono">
          Lệnh thực thi trong WSL: <code>spark-submit --driver-memory 1g pipeline/05_sql_analysis.py</code>
        </div>
      </div>
    );
  }

  if (error && !data) {
    return (
      <div className="p-6 rounded-2xl bg-rose-500/10 border border-rose-500/30 text-rose-200 text-sm space-y-2">
        <div className="flex items-center gap-2 font-bold text-rose-300">
          <AlertTriangle className="w-5 h-5 text-rose-400 shrink-0" />
          Không thể tải dữ liệu phân tích Spark SQL
        </div>
        <p className="text-xs text-rose-200/90 font-mono">{error}</p>
      </div>
    );
  }

  if (!data) return null;

  // Cấu hình biểu đồ diễn biến CO trung bình ngày quanh điểm gãy (dailyAroundBreak)
  const breakLabels = data.dailyAroundBreak.map((d) => d.date.slice(5)); // 'MM-DD'
  const breakValues = data.dailyAroundBreak.map((d) => d.mean);

  const chartData = {
    labels: breakLabels,
    datasets: [
      {
        label: 'CO Trung bình ngày (µg/m³)',
        data: breakValues,
        borderColor: '#f43f5e',
        backgroundColor: 'rgba(244, 63, 94, 0.1)',
        borderWidth: 2.5,
        pointBackgroundColor: data.dailyAroundBreak.map((d) =>
          d.date === '2025-07-01' ? '#38bdf8' : '#f43f5e'
        ),
        pointBorderColor: '#0f172a',
        pointBorderWidth: 2,
        pointRadius: data.dailyAroundBreak.map((d) =>
          d.date === '2025-07-01' ? 7 : 4
        ),
        pointHoverRadius: 8,
        tension: 0.25,
        fill: true,
      },
    ],
  };

  const chartOptions: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        backgroundColor: '#0f172a',
        titleColor: '#e2e8f0',
        bodyColor: '#cbd5e1',
        borderColor: '#334155',
        borderWidth: 1,
        padding: 10,
        callbacks: {
          title: (items) => {
            const idx = items[0]?.dataIndex ?? 0;
            const fullDate = data.dailyAroundBreak[idx]?.date || '';
            return `Ngày: ${fullDate} ${fullDate === '2025-07-01' ? '(Mốc quy ước 01/07)' : ''}`;
          },
          label: (item) => `CO Trung bình: ${Number(item.raw).toLocaleString()} µg/m³`,
        },
      },
    },
    scales: {
      x: {
        grid: {
          color: 'rgba(51, 65, 85, 0.3)',
        },
        ticks: {
          color: '#94a3b8',
          font: { size: 10 },
          maxRotation: 45,
          minRotation: 45,
        },
      },
      y: {
        grid: {
          color: 'rgba(51, 65, 85, 0.3)',
        },
        ticks: {
          color: '#94a3b8',
          font: { size: 11 },
          callback: (val) => `${val} µg/m³`,
        },
      },
    },
  };

  return (
    <div className="space-y-6">
      {/* HEADER SECTION */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-800 pb-4">
        <div>
          <h3 className="text-base font-bold text-white flex items-center gap-2">
            <Database className="w-5 h-5 text-indigo-400" />
            Kết quả Phân tích Spark SQL (Bước 5)
          </h3>
          <p className="text-xs text-slate-400 mt-0.5">
            Phân tích số liệu toàn bộ {data.rows.toLocaleString()} dòng từ HDFS (<code>{data.source}</code>), đối chiếu trước vs sau mốc quy ước 01/07/2025.
          </p>
        </div>
        <div className="flex items-center gap-2 text-[11px] text-slate-400 font-mono bg-slate-900 px-3 py-1.5 rounded-lg border border-slate-800 self-start sm:self-auto">
          <Calendar className="w-3.5 h-3.5 text-slate-400" />
          Sinh lúc: {formatDateTime(data.generatedAt)}
        </div>
      </div>

      {/* THẺ SỐ LIỆU TỔNG QUAN VÀ BIẾN THIÊN */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* THẺ 1: BIẾN THIÊN THÔ (PERIODS) */}
        <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-4">
          <div className="flex items-start justify-between">
            <div className="space-y-1">
              <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
                1. So sánh tổng thể (Dữ liệu thô)
              </span>
              <div className="text-xs text-slate-400">
                Toàn bộ dòng trước ({data.periods.before.n.toLocaleString()}) vs sau ({data.periods.after.n.toLocaleString()}) mốc 01/07/2025
              </div>
            </div>
            <div className="px-2.5 py-1 rounded-lg bg-rose-500/10 border border-rose-500/20 text-rose-300 font-bold font-mono text-sm flex items-center gap-1">
              <TrendingDown className="w-4 h-4 text-rose-400" />
              {data.periods.meanChangePct}%
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 pt-2 border-t border-slate-800">
            <div className="p-3 rounded-xl bg-slate-950/60 border border-slate-800">
              <div className="text-[11px] text-slate-400">Trước 01/07/2025</div>
              <div className="text-base font-bold text-white font-mono mt-0.5">
                {data.periods.before.mean?.toLocaleString()} <span className="text-xs text-slate-400 font-normal">µg/m³</span>
              </div>
              <div className="text-[11px] text-slate-500 mt-1">
                Trung vị: {data.periods.before.median?.toLocaleString()} µg/m³
              </div>
            </div>

            <div className="p-3 rounded-xl bg-slate-950/60 border border-slate-800">
              <div className="text-[11px] text-slate-400">Từ 01/07/2025 trở đi</div>
              <div className="text-base font-bold text-sky-400 font-mono mt-0.5">
                {data.periods.after.mean?.toLocaleString()} <span className="text-xs text-slate-400 font-normal">µg/m³</span>
              </div>
              <div className="text-[11px] text-slate-500 mt-1">
                Trung vị: {data.periods.after.median?.toLocaleString()} µg/m³
              </div>
            </div>
          </div>

          {/* Bảng phân phối chi tiết periods */}
          <div className="overflow-x-auto min-w-0">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="border-b border-slate-800 text-[11px] text-slate-400">
                  <th className="py-1.5 font-semibold">Chỉ số</th>
                  <th className="py-1.5 font-semibold text-right">Trước 01/07</th>
                  <th className="py-1.5 font-semibold text-right">Sau 01/07</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-mono text-[11px] text-slate-300">
                <tr>
                  <td className="py-1 text-slate-400 font-sans">Độ lệch chuẩn (Std)</td>
                  <td className="py-1 text-right">{data.periods.before.std?.toLocaleString()}</td>
                  <td className="py-1 text-right text-sky-300">{data.periods.after.std?.toLocaleString()}</td>
                </tr>
                <tr>
                  <td className="py-1 text-slate-400 font-sans">Phân vị 10% (P10)</td>
                  <td className="py-1 text-right">{data.periods.before.p10?.toLocaleString()}</td>
                  <td className="py-1 text-right text-sky-300">{data.periods.after.p10?.toLocaleString()}</td>
                </tr>
                <tr>
                  <td className="py-1 text-slate-400 font-sans">Phân vị 90% (P90)</td>
                  <td className="py-1 text-right">{data.periods.before.p90?.toLocaleString()}</td>
                  <td className="py-1 text-right text-sky-300">{data.periods.after.p90?.toLocaleString()}</td>
                </tr>
                <tr>
                  <td className="py-1 text-slate-400 font-sans">Giá trị lớn nhất (Max)</td>
                  <td className="py-1 text-right text-rose-300">{data.periods.before.max?.toLocaleString()}</td>
                  <td className="py-1 text-right text-sky-300">{data.periods.after.max?.toLocaleString()}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* THẺ 2: ĐỐI CHIẾU CÙNG THÁNG LỊCH (LIKE-FOR-LIKE) */}
        <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-4">
          <div className="flex items-start justify-between">
            <div className="space-y-1">
              <span className="text-[11px] font-semibold uppercase tracking-wider text-emerald-400">
                2. Khử mùa vụ (Cùng tháng lịch)
              </span>
              <div className="text-xs text-slate-400">
                So sánh các tháng 7, 8, 9, 10, 11, 12, 1, 2, 3 giữa các năm
              </div>
            </div>
            <div className="px-2.5 py-1 rounded-lg bg-emerald-500/10 border border-emerald-500/20 text-emerald-300 font-bold font-mono text-sm flex items-center gap-1">
              <ArrowDownRight className="w-4 h-4 text-emerald-400" />
              {data.seasonalLikeForLike.meanChangePct}%
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 pt-2 border-t border-slate-800">
            <div className="p-3 rounded-xl bg-slate-950/60 border border-slate-800">
              <div className="text-[11px] text-slate-400">Trước (Cùng tháng)</div>
              <div className="text-base font-bold text-white font-mono mt-0.5">
                {data.seasonalLikeForLike.before.mean?.toLocaleString()} <span className="text-xs text-slate-400 font-normal">µg/m³</span>
              </div>
              <div className="text-[11px] text-slate-500 mt-1">
                {data.seasonalLikeForLike.before.n?.toLocaleString()} mẫu giờ
              </div>
            </div>

            <div className="p-3 rounded-xl bg-slate-950/60 border border-slate-800">
              <div className="text-[11px] text-slate-400">Sau (Cùng tháng)</div>
              <div className="text-base font-bold text-emerald-400 font-mono mt-0.5">
                {data.seasonalLikeForLike.after.mean?.toLocaleString()} <span className="text-xs text-slate-400 font-normal">µg/m³</span>
              </div>
              <div className="text-[11px] text-slate-500 mt-1">
                {data.seasonalLikeForLike.after.n?.toLocaleString()} mẫu giờ
              </div>
            </div>
          </div>

          {/* Ghi chú khoa học về tính mùa vụ */}
          <div className="p-3 bg-emerald-500/5 rounded-xl border border-emerald-500/20 text-xs text-emerald-200/90 leading-relaxed space-y-1">
            <div className="font-semibold text-emerald-300 flex items-center gap-1.5">
              <Info className="w-3.5 h-3.5 shrink-0" />
              Kết luận phân tích: Mùa vụ không giải thích được mức giảm
            </div>
            <p className="text-[11px] text-slate-300">
              Khi so sánh cùng tháng lịch (loại trừ biến động thời tiết mùa mưa/khô), mức giảm vẫn đạt <strong>{Math.abs(data.seasonalLikeForLike.meanChangePct || 0)}%</strong>. Điều này chứng minh sự suy giảm mạnh không xuất phát từ chu kỳ tự nhiên theo mùa.
            </p>
          </div>

          {/* Tóm tắt biến thiên từng tháng */}
          <div className="overflow-x-auto min-w-0 max-h-32 overflow-y-auto">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="border-b border-slate-800 text-[11px] text-slate-400 sticky top-0 bg-slate-900">
                  <th className="py-1">Tháng</th>
                  <th className="py-1 text-right">Trước</th>
                  <th className="py-1 text-right">Sau</th>
                  <th className="py-1 text-right">Giảm (%)</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-mono text-[11px] text-slate-300">
                {data.seasonalByMonth.map((m) => (
                  <tr key={m.month}>
                    <td className="py-1 font-sans text-slate-400">Tháng {m.month}</td>
                    <td className="py-1 text-right">{m.before?.mean?.toLocaleString() ?? '—'}</td>
                    <td className="py-1 text-right text-emerald-300">{m.after?.mean?.toLocaleString() ?? '—'}</td>
                    <td className="py-1 text-right font-bold text-rose-400">{m.changePct}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* ĐỒ THỊ DIỄN BIẾN CO TRUNG BÌNH THEO NGÀY QUANH ĐIỂM GÃY */}
      <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800 pb-3">
          <div>
            <h4 className="text-sm font-bold text-white flex items-center gap-2">
              <TrendingDown className="w-4 h-4 text-rose-400" />
              Diễn biến CO Trung bình Ngày Quanh Điểm Gãy (17/06/2025 — 14/07/2025)
            </h4>
            <p className="text-xs text-slate-400 mt-0.5">
              Minh họa giai đoạn chuyển tiếp sụt giảm kéo dài ~2 tuần thay vì bước nhảy tức thời tại 01/07/2025.
            </p>
          </div>
          <div className="flex items-center gap-2 text-xs text-slate-400">
            <span className="w-2.5 h-2.5 rounded-full bg-sky-400 inline-block"></span>
            <span className="text-[11px]">Mốc quy ước: 01/07/2025 (381.67 µg/m³)</span>
          </div>
        </div>

        <div className="h-64 sm:h-72 w-full">
          <Line data={chartData} options={chartOptions} />
        </div>

        {/* Khung cảnh báo và phân tích bản chất điểm gãy */}
        <div className="p-3.5 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-200 text-xs space-y-1.5 leading-relaxed">
          <div className="flex items-center gap-2 font-bold text-amber-300">
            <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
            Lưu ý trung thực về bản chất Điểm gãy Dữ liệu
          </div>
          <p className="text-[11px] text-slate-300">
            Đồ thị cho thấy mức CO bắt đầu giảm mạnh từ <strong>18/06/2025</strong> (1,751 µg/m³ xuống 431 µg/m³ vào 21/06), sau đó dao động trong khoảng 380–760 µg/m³ trước khi ổn định quanh 200–310 µg/m³ vào đầu tháng 7.
          </p>
          <p className="text-[11px] text-amber-200/90 font-medium">
            ⚠️ <strong>Quy chuẩn báo cáo:</strong> Chọn ngày 01/07/2025 chỉ là mốc quy ước theo hợp đồng. Nguyên nhân điểm gãy dữ liệu hiện <strong>CHƯA XÁC ĐỊNH</strong> (có mặt ở cả bản cào thô OpenWeather lẫn bộ Kaggle). Tuyệt đối không kết luận nồng độ CO ngoài thực tế giảm thật nếu không có đo đạc vật lý đối chứng.
          </p>
        </div>
      </div>

      {/* BẢNG MA TRẬN TƯƠNG QUAN PEARSON */}
      <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800 pb-3">
          <div>
            <h4 className="text-sm font-bold text-white flex items-center gap-2">
              <GitCompare className="w-4 h-4 text-sky-400" />
              Ma trận Hệ số Tương quan Pearson với Khí CO
            </h4>
            <p className="text-xs text-slate-400 mt-0.5">
              Phân tách ma trận tương quan giữa 2 giai đoạn: Trước điểm gãy vs Sau điểm gãy.
            </p>
          </div>
          <span className="text-[11px] text-amber-300 bg-amber-500/10 px-2.5 py-1 rounded-md border border-amber-500/20 font-sans self-start sm:self-auto">
            Tương quan ≠ Quan hệ Nhân quả
          </span>
        </div>

        <div className="overflow-x-auto min-w-0">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="border-b border-slate-800 text-[11px] text-slate-400">
                <th className="py-2.5 px-3 font-semibold">Chất khí / Thông số thời tiết</th>
                <th className="py-2.5 px-3 font-semibold text-right">Trước 01/07/2025</th>
                <th className="py-2.5 px-3 font-semibold text-right">Sau 01/07/2025</th>
                <th className="py-2.5 px-3 font-semibold text-right">
                  Toàn bộ chuỗi <span className="text-[10px] text-amber-400 font-normal">(*)</span>
                </th>
                <th className="py-2.5 px-3 font-semibold text-center">Xu hướng chuyển dịch</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 font-mono text-xs">
              {data.correlation.variables.map((varName) => {
                const info = VARIABLE_LABELS[varName] || { label: varName, unit: '' };
                const beforeVal = data.correlation.before[varName];
                const afterVal = data.correlation.after[varName];
                const allVal = data.correlation.all[varName];

                return (
                  <tr key={varName} className="hover:bg-slate-800/30 transition">
                    <td className="py-2 px-3 font-sans text-slate-200">
                      <div className="font-medium">{info.label}</div>
                      <div className="text-[10px] text-slate-500 font-mono">{varName} ({info.unit})</div>
                    </td>
                    <td className="py-2 px-3 text-right">
                      <span className={`px-2 py-0.5 rounded ${
                        (beforeVal ?? 0) >= 0.7 ? 'bg-sky-500/20 text-sky-300 font-bold' :
                        (beforeVal ?? 0) <= -0.2 ? 'bg-rose-500/20 text-rose-300' :
                        'text-slate-300'
                      }`}>
                        {beforeVal !== null && beforeVal !== undefined ? beforeVal.toFixed(4) : '—'}
                      </span>
                    </td>
                    <td className="py-2 px-3 text-right">
                      <span className={`px-2 py-0.5 rounded ${
                        (afterVal ?? 0) >= 0.7 ? 'bg-emerald-500/20 text-emerald-300 font-bold' :
                        (afterVal ?? 0) <= -0.2 ? 'bg-rose-500/20 text-rose-300' :
                        'text-slate-300'
                      }`}>
                        {afterVal !== null && afterVal !== undefined ? afterVal.toFixed(4) : '—'}
                      </span>
                    </td>
                    <td className="py-2 px-3 text-right text-slate-400">
                      {allVal !== null && allVal !== undefined ? allVal.toFixed(4) : '—'}
                    </td>
                    <td className="py-2 px-3 text-center text-[11px] font-sans">
                      {beforeVal !== null && afterVal !== null && beforeVal !== undefined && afterVal !== undefined ? (
                        Math.abs(afterVal - beforeVal) < 0.1 ? (
                          <span className="text-slate-400">Ổn định</span>
                        ) : afterVal > beforeVal ? (
                          <span className="text-emerald-400">Tăng tương quan ({(afterVal - beforeVal > 0 ? '+' : '') + (afterVal - beforeVal).toFixed(2)})</span>
                        ) : (
                          <span className="text-rose-400">Giảm tương quan ({(afterVal - beforeVal).toFixed(2)})</span>
                        )
                      ) : '—'}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        <div className="text-[11px] text-slate-500 pt-2 border-t border-slate-800 space-y-1 leading-relaxed">
          <div>
            <span className="text-amber-400 font-semibold">(*) Ghi chú về cột Toàn bộ:</span> Giá trị tương quan toàn bộ trộn lẫn hai mức nồng độ CO khác nhau giữa hai giai đoạn lịch sử nên không phản ánh đúng liên hệ nội tại trong từng thời kỳ. Cần trích dẫn riêng biệt cột <em>Trước</em> và <em>Sau</em>.
          </div>
          <div>
            Tốc độ gió (-0.14 vs -0.11) và nhiệt độ (+0.02 vs 0.00) hầu như không có tương quan tuyến tính với nồng độ khí CO trong tập dữ liệu này.
          </div>
        </div>
      </div>
    </div>
  );
};

export default AnalysisSqlSection;
