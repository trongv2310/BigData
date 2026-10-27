import React, { useState } from 'react';
import {
  PieChart,
  Calendar,
  AlertTriangle,
  Info,
  Layers,
  Sparkles,
  GitBranch,
  ShieldCheck,
  CheckCircle2,
  Clock,
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
  type ChartOptions,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import type { DayClusters, DayClusterInfo } from '../types';
import { formatDateTime } from '../utils/format';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

interface DayClustersSectionProps {
  data: DayClusters | null;
  loading: boolean;
  error: string | null;
  isNotReady?: boolean;
}

const HOURS_24 = Array.from({ length: 24 }, (_, i) => `${i}h`);

export const DayClustersSection: React.FC<DayClustersSectionProps> = ({
  data,
  loading,
  error,
  isNotReady,
}) => {
  // Chế độ xem tâm cụm: k=2 (mặc định chọn) hoặc k=3 (từ bài toán kiểm định độ ổn định)
  const [viewK, setViewK] = useState<2 | 3>(2);

  if (loading) {
    return (
      <div className="p-8 rounded-2xl bg-slate-900 border border-slate-800 text-center space-y-3 animate-pulse">
        <div className="inline-flex p-3 rounded-xl bg-slate-800 text-purple-400">
          <PieChart className="w-6 h-6 animate-spin" />
        </div>
        <p className="text-sm text-slate-400">Đang tải kết quả phân cụm K-Means & độ ổn định (Bước 6a)...</p>
      </div>
    );
  }

  if (isNotReady) {
    return (
      <div className="p-6 rounded-2xl bg-slate-900/60 border border-slate-800 text-center space-y-3">
        <div className="inline-flex p-3 rounded-xl bg-amber-500/10 text-amber-400 border border-amber-500/20">
          <AlertTriangle className="w-6 h-6" />
        </div>
        <h4 className="text-sm font-bold text-white">Phân cụm K-Means Chưa Sẵn Sàng (404 ANALYSIS_NOT_READY)</h4>
        <p className="text-xs text-slate-400 max-w-lg mx-auto leading-relaxed">
          Bước 6a phân cụm ngày chưa được thực thi trên cụm Big Data hoặc tệp <code>/co/output/day_clusters.json</code> chưa được sinh ra.
        </p>
        <div className="text-[11px] text-slate-500 font-mono">
          Lệnh thực thi trong WSL: <code>spark-submit --driver-memory 1g pipeline/06a_cluster_days.py</code>
        </div>
      </div>
    );
  }

  if (error && !data) {
    return (
      <div className="p-6 rounded-2xl bg-rose-500/10 border border-rose-500/30 text-rose-200 text-sm space-y-2">
        <div className="flex items-center gap-2 font-bold text-rose-300">
          <AlertTriangle className="w-5 h-5 text-rose-400 shrink-0" />
          Không thể tải dữ liệu phân cụm K-Means
        </div>
        <p className="text-xs text-rose-200/90 font-mono">{error}</p>
      </div>
    );
  }

  if (!data) return null;

  // Lựa chọn danh sách cụm theo viewK (k=2 hoặc k=3 từ stability)
  const activeClusters: DayClusterInfo[] =
    viewK === 3 && data.stability?.k3?.clusters
      ? data.stability.k3.clusters
      : data.clusters;

  // Màu sắc riêng biệt cho từng cụm
  const CLUSTER_COLORS = [
    { border: '#a855f7', bg: 'rgba(168, 85, 247, 0.1)', name: 'Cụm 0 (Đỉnh khuya/đêm)' },
    { border: '#38bdf8', bg: 'rgba(56, 189, 248, 0.1)', name: 'Cụm 1 (Biến thiên phẳng)' },
    { border: '#f59e0b', bg: 'rgba(245, 158, 11, 0.1)', name: 'Cụm 2 (Đỉnh tối 22h)' },
  ];

  const profileChartData = {
    labels: HOURS_24,
    datasets: activeClusters.map((cluster, idx) => {
      const color = CLUSTER_COLORS[idx % CLUSTER_COLORS.length];
      return {
        label: `Cụm ${cluster.cluster} (${cluster.days} ngày - Đỉnh ${cluster.peakHour}h)`,
        data: cluster.center,
        borderColor: color.border,
        backgroundColor: color.bg,
        borderWidth: 2.5,
        pointBackgroundColor: color.border,
        pointBorderColor: '#0f172a',
        pointRadius: 4,
        pointHoverRadius: 6,
        tension: 0.3,
      };
    }),
  };

  const profileChartOptions: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
        labels: {
          color: '#cbd5e1',
          font: { size: 11 },
          boxWidth: 14,
          boxHeight: 14,
        },
      },
      tooltip: {
        backgroundColor: '#0f172a',
        titleColor: '#e2e8f0',
        bodyColor: '#cbd5e1',
        borderColor: '#334155',
        borderWidth: 1,
        padding: 10,
        callbacks: {
          title: (items) => `Giờ: ${items[0]?.label} (+07:00)`,
          label: (item) => `${item.dataset.label}: ${Number(item.raw).toFixed(3)} (tỷ lệ so với TB ngày)`,
        },
      },
    },
    scales: {
      x: {
        grid: { color: 'rgba(51, 65, 85, 0.3)' },
        ticks: { color: '#94a3b8', font: { size: 10 } },
      },
      y: {
        grid: { color: 'rgba(51, 65, 85, 0.3)' },
        ticks: {
          color: '#94a3b8',
          font: { size: 10 },
          callback: (val) => `${val}×`,
        },
        title: {
          display: true,
          text: 'Tỷ lệ hình dạng (CO giờ / CO trung bình ngày)',
          color: '#64748b',
          font: { size: 10 },
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
            <PieChart className="w-5 h-5 text-purple-400" />
            Phân cụm Ngày K-Means (Bước 6a) & Kiểm định Độ ổn định
          </h3>
          <p className="text-xs text-slate-400 mt-0.5">
            Mô hình Spark MLlib K-Means gom cụm {data.daysUsed.toLocaleString()} ngày theo hình dạng 24 giờ (đã chuẩn hóa loại bỏ mức CO tuyệt đối) và kiểm định ARI qua 6 seed.
          </p>
        </div>
        <div className="flex items-center gap-2 text-[11px] text-slate-400 font-mono bg-slate-900 px-3 py-1.5 rounded-lg border border-slate-800 self-start sm:self-auto">
          <Calendar className="w-3.5 h-3.5 text-slate-400" />
          Sinh lúc: {formatDateTime(data.generatedAt)}
        </div>
      </div>

      {/* THẺ THÔNG TIN TỔNG QUAN PHÂN CỤM */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800 space-y-1">
          <div className="text-[11px] text-slate-400 flex items-center gap-1.5">
            <Layers className="w-3.5 h-3.5 text-purple-400" />
            Số cụm tối ưu (k)
          </div>
          <div className="text-xl font-bold text-white font-mono flex items-center gap-1.5">
            k = {data.k}
            <span className="text-[11px] font-sans font-normal text-purple-300 bg-purple-500/10 px-2 py-0.5 rounded border border-purple-500/20">
              Chọn theo Silhouette
            </span>
          </div>
          <div className="text-[10px] text-slate-500">Seed mặc định: {data.seed}</div>
        </div>

        <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800 space-y-1">
          <div className="text-[11px] text-slate-400 flex items-center gap-1.5">
            <Sparkles className="w-3.5 h-3.5 text-sky-400" />
            Chỉ số Silhouette
          </div>
          <div className="text-xl font-bold text-sky-400 font-mono">
            {data.silhouette.toFixed(4)}
          </div>
          <div className="text-[10px] text-amber-400">
            Cấu trúc cụm: <strong className="uppercase">{data.structure}</strong> (&lt; 0.5)
          </div>
        </div>

        <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800 space-y-1">
          <div className="text-[11px] text-slate-400 flex items-center gap-1.5">
            <Calendar className="w-3.5 h-3.5 text-emerald-400" />
            Số ngày phân tích
          </div>
          <div className="text-xl font-bold text-white font-mono">
            {data.daysUsed.toLocaleString()} <span className="text-xs text-slate-400 font-normal">ngày</span>
          </div>
          <div className="text-[10px] text-slate-500">
            Loại {data.daysExcludedIncomplete} ngày thiếu giờ (+07)
          </div>
        </div>

        <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800 space-y-1">
          <div className="text-[11px] text-slate-400 flex items-center gap-1.5">
            <ShieldCheck className="w-3.5 h-3.5 text-indigo-400" />
            Thời gian dữ liệu
          </div>
          <div className="text-xs font-bold text-slate-200 font-mono mt-1">
            {data.firstDay} → {data.lastDay}
          </div>
          <div className="text-[10px] text-slate-500">Chu kỳ gần 4 năm</div>
        </div>
      </div>

      {/* ĐỒ THỊ TÂM CỤM 24 GIỜ */}
      <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-slate-800 pb-3">
          <div>
            <h4 className="text-sm font-bold text-white flex items-center gap-2">
              <Clock className="w-4 h-4 text-purple-400" />
              Hình dạng Nhịp Sinh học 24 Giờ của các Tâm Cụm (Center Profile)
            </h4>
            <p className="text-xs text-slate-400 mt-0.5">
              Giá trị 1.0 tương ứng với mức CO trung bình trong ngày; &gt; 1.0 là nồng độ cao hơn trung bình ngày.
            </p>
          </div>

          {/* Toggle chọn k=2 vs k=3 */}
          {data.stability?.k3 && (
            <div className="flex items-center gap-1.5 p-1 rounded-xl bg-slate-950 border border-slate-800 self-start sm:self-auto">
              <button
                onClick={() => setViewK(2)}
                className={`px-3 py-1 rounded-lg text-xs font-medium transition ${
                  viewK === 2
                    ? 'bg-purple-500/20 text-purple-300 border border-purple-500/40'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                k = 2 (Tiêu chí chọn)
              </button>
              <button
                onClick={() => setViewK(3)}
                className={`px-3 py-1 rounded-lg text-xs font-medium transition ${
                  viewK === 3
                    ? 'bg-purple-500/20 text-purple-300 border border-purple-500/40'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                k = 3 (Ổn định 6 seed)
              </button>
            </div>
          )}
        </div>

        <div className="h-64 sm:h-72 w-full">
          <Line data={profileChartData} options={profileChartOptions} />
        </div>

        {/* THẺ MÔ TẢ CHI TIẾT TỪNG CỤM */}
        <div className={`grid grid-cols-1 ${activeClusters.length === 3 ? 'sm:grid-cols-3' : 'sm:grid-cols-2'} gap-3 pt-2`}>
          {activeClusters.map((cluster, idx) => {
            const color = CLUSTER_COLORS[idx % CLUSTER_COLORS.length];
            return (
              <div
                key={cluster.cluster}
                className="p-4 rounded-xl bg-slate-950/70 border border-slate-800 space-y-2.5"
              >
                <div className="flex items-center justify-between">
                  <span
                    className="text-xs font-bold px-2 py-0.5 rounded border"
                    style={{
                      color: color.border,
                      backgroundColor: color.bg,
                      borderColor: color.border,
                    }}
                  >
                    Cụm {cluster.cluster}
                  </span>
                  <span className="text-xs text-slate-300 font-mono font-bold">
                    {cluster.days} ngày (
                    {((cluster.days / data.daysUsed) * 100).toFixed(1)}%)
                  </span>
                </div>

                <div className="space-y-1 text-xs text-slate-300">
                  <div className="flex justify-between">
                    <span className="text-slate-400">CO trung bình ngày:</span>
                    <span className="font-mono font-bold text-white">
                      {cluster.meanDailyCo.toLocaleString()} µg/m³
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">Đỉnh / Đáy hình dạng:</span>
                    <span className="font-mono text-purple-300">
                      {cluster.peakHour}h / {cluster.troughHour}h (gấp {cluster.peakToTrough}×)
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">Tỷ lệ ngày cuối tuần:</span>
                    <span className="font-mono text-slate-300">
                      {(cluster.weekendShare * 100).toFixed(1)}% (~ngang nhau)
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">Tỷ lệ sau 01/07/2025:</span>
                    <span className="font-mono text-sky-400">
                      {(cluster.afterBreakShare * 100).toFixed(1)}%
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* BẢNG LỰA CHỌN SỐ CỤM K = 2..8 */}
      <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-3">
        <div className="space-y-1 border-b border-slate-800 pb-3">
          <h4 className="text-sm font-bold text-white flex items-center gap-2">
            <Layers className="w-4 h-4 text-sky-400" />
            Bảng Tiêu chí Lựa chọn Số cụm Tối ưu (k = 2 đến 8)
          </h4>
          <p className="text-xs text-slate-400">
            {data.criterion}. Tiêu chí Silhouette thiên vị các giá trị k nhỏ.
          </p>
        </div>

        <div className="overflow-x-auto min-w-0">
          <table className="w-full text-left text-xs border-collapse">
            <thead>
              <tr className="border-b border-slate-800 text-[11px] text-slate-400">
                <th className="py-2 px-3 font-semibold">Số cụm (k)</th>
                <th className="py-2 px-3 font-semibold text-right">Silhouette Score</th>
                <th className="py-2 px-3 font-semibold text-right">Tổng bình phương sai số (WSSSE)</th>
                <th className="py-2 px-3 font-semibold text-right">Phân bổ kích thước cụm (ngày)</th>
                <th className="py-2 px-3 font-semibold text-center">Trạng thái</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 font-mono text-xs">
              {data.selection.map((item) => (
                <tr
                  key={item.k}
                  className={`hover:bg-slate-800/30 transition ${
                    item.k === data.k ? 'bg-purple-500/5' : ''
                  }`}
                >
                  <td className="py-2 px-3 font-bold text-slate-200">
                    k = {item.k}
                  </td>
                  <td className="py-2 px-3 text-right">
                    <span
                      className={`px-2 py-0.5 rounded font-bold ${
                        item.k === data.k
                          ? 'bg-purple-500/20 text-purple-300'
                          : item.k === 3
                          ? 'bg-sky-500/10 text-sky-300'
                          : 'text-slate-300'
                      }`}
                    >
                      {item.silhouette.toFixed(4)}
                    </span>
                  </td>
                  <td className="py-2 px-3 text-right text-slate-400">
                    {item.wssse.toFixed(1)}
                  </td>
                  <td className="py-2 px-3 text-right text-slate-300 font-sans text-[11px]">
                    {item.sizes.join(' / ')}
                  </td>
                  <td className="py-2 px-3 text-center">
                    {item.k === data.k ? (
                      <span className="inline-flex items-center gap-1 text-[11px] font-sans text-purple-300 bg-purple-500/20 px-2 py-0.5 rounded-full">
                        <CheckCircle2 className="w-3 h-3" /> Được chọn (Max)
                      </span>
                    ) : item.k === 3 ? (
                      <span className="text-[11px] font-sans text-sky-400">Bám rất sát (0.4426)</span>
                    ) : (
                      <span className="text-[11px] font-sans text-slate-500">—</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* KIỂM ĐỊNH ĐỘ ỔN ĐỊNH QUA 6 SEED (STABILITY) */}
      {data.stability ? (
        <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-4">
          <div className="space-y-1 border-b border-slate-800 pb-3">
            <h4 className="text-sm font-bold text-white flex items-center gap-2">
              <GitBranch className="w-4 h-4 text-emerald-400" />
              Kiểm định Độ Ổn định K-Means qua 6 Seed (Adjusted Rand Index - ARI)
            </h4>
            <p className="text-xs text-slate-400">
              Kiểm tra tính lặp lại của thuật toán K-Means qua 6 seed ngẫu nhiên ({data.stability.seeds.join(', ')}).
            </p>
          </div>

          {/* Bảng so sánh k được chọn theo từng seed */}
          <div className="grid grid-cols-2 sm:grid-cols-6 gap-2">
            {data.stability.seeds.map((s) => {
              const chosenK = data.stability?.chosenKBySeed[String(s)];
              const isDiff = chosenK === 3;
              return (
                <div
                  key={s}
                  className={`p-3 rounded-xl border text-center space-y-1 ${
                    isDiff
                      ? 'bg-amber-500/10 border-amber-500/30 text-amber-200'
                      : 'bg-slate-950/60 border-slate-800 text-slate-300'
                  }`}
                >
                  <div className="text-[11px] text-slate-400">Seed {s}</div>
                  <div className={`text-base font-bold font-mono ${isDiff ? 'text-amber-300' : 'text-purple-300'}`}>
                    k = {chosenK}
                  </div>
                  {isDiff && <div className="text-[10px] text-amber-400 font-semibold">Chọn k=3!</div>}
                </div>
              );
            })}
          </div>

          {/* Bảng đối chiếu độ ổn định k=2 vs k=3 */}
          <div className="overflow-x-auto min-w-0">
            <table className="w-full text-left text-xs border-collapse">
              <thead>
                <tr className="border-b border-slate-800 text-[11px] text-slate-400">
                  <th className="py-2 px-3 font-semibold">Số cụm</th>
                  <th className="py-2 px-3 font-semibold text-right">Silhouette [Min — Max]</th>
                  <th className="py-2 px-3 font-semibold text-right">ARI Trung bình cặp seed</th>
                  <th className="py-2 px-3 font-semibold text-right">ARI Thấp nhất (Min)</th>
                  <th className="py-2 px-3 font-semibold text-center">Đánh giá tính ổn định</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60 font-mono text-xs">
                {['2', '3'].map((kKey) => {
                  const stab = data.stability?.stability[kKey];
                  if (!stab) return null;
                  const isK3 = kKey === '3';

                  return (
                    <tr key={kKey} className={isK3 ? 'bg-emerald-500/5' : ''}>
                      <td className="py-2.5 px-3 font-bold text-slate-200 font-sans">
                        k = {kKey}
                      </td>
                      <td className="py-2.5 px-3 text-right">
                        {stab.silhouetteMin.toFixed(4)} — {stab.silhouetteMax.toFixed(4)}
                      </td>
                      <td className="py-2.5 px-3 text-right font-bold text-white">
                        {stab.ariPairwiseMean.toFixed(4)}
                      </td>
                      <td className="py-2.5 px-3 text-right">
                        <span className={`px-2 py-0.5 rounded ${
                          stab.ariPairwiseMin < 0.5 ? 'bg-rose-500/20 text-rose-300 font-bold' : 'bg-emerald-500/20 text-emerald-300'
                        }`}>
                          {stab.ariPairwiseMin.toFixed(4)}
                        </span>
                      </td>
                      <td className="py-2.5 px-3 text-center font-sans text-[11px]">
                        {isK3 ? (
                          <span className="text-emerald-300 font-semibold">Rất ổn định (ARI &gt; 0.987 ở mọi seed)</span>
                        ) : (
                          <span className="text-amber-400">Kém ổn định ở seed 21 (ARI = 0.057)</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* Callout phân tích học thuật trung thực */}
          <div className="p-4 rounded-xl bg-slate-950/80 border border-slate-800 space-y-2 text-xs leading-relaxed text-slate-300">
            <div className="font-bold text-emerald-300 flex items-center gap-1.5">
              <Info className="w-4 h-4 shrink-0" />
              Kết luận Học thuật Trung thực (Academic Integrity)
            </div>
            <p className="text-[11px]">
              - <strong>k = 2 không hoàn toàn ổn định:</strong> Ở 5/6 seed, kết quả đạt tương đồng cao (ARI &gt; 0.98), nhưng tại <strong>seed 21</strong>, thuật toán rơi vào cực tiểu cục bộ kém (Silhouette tụt còn 0.3626, ARI so với seed 42 chỉ còn 0.0569). Do đó tiêu chí tại seed 21 tự động chuyển sang chọn k = 3.
            </p>
            <p className="text-[11px]">
              - <strong>k = 3 cực kỳ ổn định:</strong> Điểm Silhouette dao động rất hẹp từ 0.4414 đến 0.4426 trên mọi seed; ARI giữa mọi cặp seed đều &ge; 0.9874 (trung bình 0.9931). Kích thước các cụm giữ nguyên ở mức ~710, ~409 và ~341 ngày.
            </p>
            <p className="text-[11px] text-slate-400 italic">
              💡 <em>Ý nghĩa:</em> Việc k = 2 "thắng" k = 3 ở seed 42 (0.4452 vs 0.4426) là rất mong manh và chủ yếu do Silhouette ưu tiên k nhỏ. Về độ tin cậy khoa học, k = 3 thể hiện cấu trúc phân nhóm bền vững hơn.
            </p>
          </div>
        </div>
      ) : (
        <div className="p-4 rounded-xl bg-slate-900 border border-slate-800 text-xs text-slate-400 flex items-center gap-2">
          <Info className="w-4 h-4 text-slate-500 shrink-0" />
          <span>Kiểm định độ ổn định (stability) chưa được bật hoặc trả về null cho endpoint này.</span>
        </div>
      )}
    </div>
  );
};

export default DayClustersSection;
