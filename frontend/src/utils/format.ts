import type { CoStatus } from '../types';

/**
 * Định dạng số byte thành chuỗi người đọc được (B, KB, MB, GB, TB)
 */
export function formatBytes(bytes?: number | null): string {
  if (bytes === null || bytes === undefined || isNaN(bytes)) return '—';
  if (bytes === 0) return '0 B';

  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  const val = (bytes / Math.pow(k, i)).toFixed(i === 0 ? 0 : 2);
  return `${val} ${units[i]}`;
}

/**
 * Định dạng thời gian hiển thị thân thiện (Giờ Việt Nam UTC+7)
 */
export function formatDateTime(isoString?: string | null): string {
  if (!isoString) return '—';
  try {
    const d = new Date(isoString);
    if (isNaN(d.getTime())) return isoString;

    // Hiển thị giờ:phút:giây ngày/tháng/năm theo múi giờ Việt Nam
    return new Intl.DateTimeFormat('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour12: false,
    }).format(d);
  } catch {
    return isoString;
  }
}

/**
 * Định dạng thời gian ngắn (chỉ giờ:phút:giây)
 */
export function formatTimeOnly(isoString?: string | null): string {
  if (!isoString) return '—';
  try {
    const d = new Date(isoString);
    if (isNaN(d.getTime())) return isoString;

    return new Intl.DateTimeFormat('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(d);
  } catch {
    return isoString;
  }
}

/**
 * Trả về màu và nhãn cho CoStatus nhận từ backend
 * TUYỆT ĐỐI KHÔNG TỰ TÍNH STATUS, CHỈ HIỂN THỊ STATUS DO BACKEND TRẢ VỀ
 */
export function getCoStatusBadge(status?: CoStatus | null): {
  label: string;
  badgeClass: string;
  bgClass: string;
  borderClass: string;
} {
  switch (status) {
    case 'SAFE':
      return {
        label: 'AN TOÀN (SAFE)',
        badgeClass: 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30',
        bgClass: 'bg-emerald-950/40',
        borderClass: 'border-emerald-500/30',
      };
    case 'WARNING':
      return {
        label: 'CẢNH BÁO (WARNING)',
        badgeClass: 'bg-amber-500/20 text-amber-400 border border-amber-500/30',
        bgClass: 'bg-amber-950/40',
        borderClass: 'border-amber-500/30',
      };
    case 'DANGER':
      return {
        label: 'NGUY HIỂM (DANGER)',
        badgeClass: 'bg-rose-500/20 text-rose-400 border border-rose-500/30',
        bgClass: 'bg-rose-950/40',
        borderClass: 'border-rose-500/30',
      };
    default:
      return {
        label: 'CHƯA XÁC ĐỊNH',
        badgeClass: 'bg-slate-500/20 text-slate-400 border border-slate-500/30',
        bgClass: 'bg-slate-900/40',
        borderClass: 'border-slate-700',
      };
  }
}

/**
 * Nhãn hiển thị nguồn dữ liệu
 */
export function getSourceLabel(source?: string | null): string {
  if (!source) return 'Không rõ nguồn';
  switch (source) {
    case 'replay-dataset':
      return 'Phát lại từ bộ lịch sử Kaggle (MÔ PHỎNG)';
    case 'live-open-meteo':
      return 'Open-Meteo CAMS (Mô hình)';
    case 'live-openweather':
      return 'OpenWeather Air Pollution (THẬT)';
    default:
      return source;
  }
}

/**
 * Định dạng thời gian ngắn gọn cho trục hoành biểu đồ (HH:mm hoặc HH:mm dd/MM)
 */
export function formatChartTime(isoString?: string | null): string {
  if (!isoString) return '';
  try {
    const d = new Date(isoString);
    if (isNaN(d.getTime())) return isoString;
    return new Intl.DateTimeFormat('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    }).format(d);
  } catch {
    return isoString;
  }
}

/**
 * Tính tuổi của dữ liệu so với hiện tại
 */
export function formatDataAge(datetimeStr?: string | null, currentTime: number = Date.now()): string {
  if (!datetimeStr) return '—';
  const recordTime = new Date(datetimeStr).getTime();
  if (isNaN(recordTime)) return '—';
  const diffMs = Math.max(0, currentTime - recordTime);
  const diffMinutes = Math.floor(diffMs / (1000 * 60));
  if (diffMinutes < 1) return 'vừa mới đây (< 1 phút)';
  if (diffMinutes < 60) return `cách đây ${diffMinutes} phút`;
  const diffHours = Math.floor(diffMinutes / 60);
  const remMinutes = diffMinutes % 60;
  return `cách đây ${diffHours} giờ ${remMinutes > 0 ? `${remMinutes} phút` : ''}`;
}

/**
 * Trả về thông tin nhãn và phong cách cho cờ coIsForecast
 * - true: giá trị dự báo của OpenWeather cho giờ tới (nhận sớm ~30 phút)
 * - false: giá trị quan sát của nguồn (hoặc dữ liệu lịch sử)
 * - null: chưa rõ là quan sát hay dự báo (không tự suy đoán)
 */
export function getForecastStatus(coIsForecast?: boolean | null): {
  label: string;
  badgeClass: string;
  description: string;
} {
  if (coIsForecast === true) {
    return {
      label: 'giá trị dự báo của OpenWeather',
      badgeClass: 'bg-amber-500/20 text-amber-300 border border-amber-500/40',
      description: 'Giá trị dự báo của OpenWeather cho giờ tới (nhận sớm ~30 phút), không phải quan sát',
    };
  }
  if (coIsForecast === false) {
    return {
      label: 'giá trị quan sát của nguồn',
      badgeClass: 'bg-cyan-500/10 text-cyan-300 border border-cyan-500/30',
      description: 'Số từ mô hình khí quyển của OpenWeather, không phải cảm biến mặt đất',
    };
  }
  return {
    label: 'chưa rõ là quan sát hay dự báo',
    badgeClass: 'bg-slate-800 text-slate-400 border border-slate-700',
    description: 'Chưa xác định cờ dự báo từ nguồn hoặc bản ghi cũ, không tự suy đoán',
  };
}

