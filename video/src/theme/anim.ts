import { Easing, interpolate, spring } from "remotion";

const clampOpts = {
  extrapolateLeft: "clamp",
  extrapolateRight: "clamp",
} as const;

/** 0 → 1 trong khoảng [start, start + duration], có easing. */
export const progress = (
  frame: number,
  start: number,
  duration: number,
  easing: (t: number) => number = Easing.inOut(Easing.cubic),
): number =>
  interpolate(frame, [start, start + Math.max(1, duration)], [0, 1], {
    ...clampOpts,
    easing,
  });

/** Hiện dần: độ mờ và trượt nhẹ từ dưới lên. */
export const appear = (frame: number, start: number, duration = 12) => {
  const p = progress(frame, start, duration, Easing.out(Easing.cubic));
  return { opacity: p, transform: `translateY(${(1 - p) * 16}px)` };
};

/** Bật nảy nhẹ, trả về hệ số scale. */
export const pop = (frame: number, start: number, fps: number): number =>
  frame < start
    ? 0
    : spring({ frame: frame - start, fps, config: { damping: 14, mass: 0.6 } });

/** Nội suy tuyến tính giữa hai giá trị theo p. */
export const mix = (a: number, b: number, p: number): number => a + (b - a) * p;

/** Nhấp nháy theo chu kỳ (giây), trả về 0..1, đỉnh ngay đầu chu kỳ. */
export const blink = (
  frame: number,
  fps: number,
  periodSec: number,
  start = 0,
): number => {
  if (frame < start) return 0;
  const period = periodSec * fps;
  const t = ((frame - start) % period) / period;
  return Math.max(0, 1 - t * 4);
};
