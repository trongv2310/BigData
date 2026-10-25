import { loadFont as loadBeVietnam } from "@remotion/google-fonts/BeVietnamPro";
import { loadFont as loadMono } from "@remotion/google-fonts/JetBrainsMono";

// Be Vietnam Pro có đủ dấu tiếng Việt; chỉ tải các độ đậm thật sự dùng.
// "đ", "Đ", "ư", "ơ" nằm ở latin-ext, các nguyên âm có dấu nằm ở vietnamese,
// nên cần cả ba nhóm ký tự.
const sans = loadBeVietnam("normal", {
  weights: ["400", "500", "600", "700", "800"],
  subsets: ["vietnamese", "latin-ext", "latin"],
});

// Phông đơn cách cho lệnh, đường dẫn, mã block (chỉ có ký tự ASCII).
const mono = loadMono("normal", {
  weights: ["400", "600"],
  subsets: ["latin"],
});

export const fonts = {
  sans: sans.fontFamily,
  mono: mono.fontFamily,
};
