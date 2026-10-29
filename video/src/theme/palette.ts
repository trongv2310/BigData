// Bảng màu cố định cho cả loạt video. Đổi ở đây là đổi cho mọi video.
// Quy ước đầy đủ: storyboard/00_quy_uoc.md, mục "Quy ước hình ảnh".

export const component = {
  hdfs: "#3B82F6", // xanh dương
  kafka: "#22C55E", // xanh lá
  spark: "#F97316", // cam
  mllib: "#A855F7", // tím
  airflow: "#06B6D4", // xanh ngọc
  serving: "#94A3B8", // backend, dashboard
} as const;

export type ComponentKey = keyof typeof component;

// Màu theo nguồn dữ liệu và trạng thái. Không trùng với màu thành phần để
// người xem không nhầm "dữ liệu thật" với "Kafka".
export const data = {
  real: "#F472B6", // OpenWeather, dữ liệu thật
  replay: "#CBD5E1", // phát lại, luôn kèm nhãn MÔ PHỎNG
} as const;

export const state = {
  running: "#FACC15", // đang chạy
  ok: "#4ADE80", // xong, sống
  error: "#EF4444", // lỗi, chết, mất
  idle: "#475569", // chưa tới lượt
  done: "#64748B", // đã xử lý
} as const;

export const ui = {
  bg: "#0B1220",
  bgSoft: "#111A2E",
  panel: "#16213A",
  panelBorder: "#26344F",
  text: "#E2E8F0",
  textDim: "#94A3B8",
  textFaint: "#64748B",
  subtitleBg: "rgba(5, 9, 18, 0.82)",
} as const;

/** Trộn màu với độ trong suốt, ví dụ alpha("#3B82F6", 0.2). */
export const alpha = (hex: string, a: number): string => {
  const n = parseInt(hex.slice(1), 16);
  const r = (n >> 16) & 255;
  const g = (n >> 8) & 255;
  const b = n & 255;
  return `rgba(${r}, ${g}, ${b}, ${a})`;
};
