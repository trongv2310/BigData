// Định dạng số theo storyboard/00_quy_uoc.md: dấu cách ngăn nghìn, dấu phẩy
// thập phân. Đổi THOUSANDS thành "." nếu muốn kiểu 3.179.004.
// Be Vietnam Pro không có khoảng trắng hẹp (U+202F), nên dùng khoảng trắng
// không ngắt dòng thường (U+00A0).
const THOUSANDS = String.fromCharCode(0xa0);
const DECIMAL = ",";

export const formatNumber = (value: number, decimals = 0): string => {
  const fixed = Math.abs(value).toFixed(decimals);
  const [intPart, frac] = fixed.split(".");
  const grouped = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, THOUSANDS);
  const sign = value < 0 ? "−" : "";
  return sign + (frac ? `${grouped}${DECIMAL}${frac}` : grouped);
};
