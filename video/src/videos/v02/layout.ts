import { interpolate } from "remotion";

// Vị trí cố định của các nút trong video HDFS, để người xem thấy cùng một
// "bản đồ" ở mọi cảnh.
export const NN = { x: 960, y: 250 };
export const CLIENT = { x: 250, y: 250 };
export const DN_Y = 640;
export const DN_X = [390, 770, 1150, 1530];
export const DN_PORTS = ["9864", "9874", "9884", "9894"];

/** Hiện trong [start, end): mờ dần vào và ra trong f khung hình. */
export const windowOpacity = (frame: number, start: number, end = Infinity, f = 12): number => {
  if (frame < start || frame >= end + f) return 0;
  const fadeIn = interpolate(frame, [start, start + f], [0, 1], { extrapolateRight: "clamp" });
  const fadeOut =
    end === Infinity ? 1 : interpolate(frame, [end, end + f], [1, 0], { extrapolateLeft: "clamp" });
  return Math.min(fadeIn, fadeOut);
};
