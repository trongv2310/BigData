export type Point = { x: number; y: number };

/** Điểm điều khiển của đường cong bậc hai, lệch vuông góc khỏi đoạn thẳng. */
export const controlPoint = (a: Point, b: Point, curve: number): Point => {
  const mx = (a.x + b.x) / 2;
  const my = (a.y + b.y) / 2;
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const len = Math.hypot(dx, dy) || 1;
  return { x: mx - (dy / len) * curve, y: my + (dx / len) * curve };
};

/** Vị trí tại t (0..1) trên đường cong bậc hai a → c → b. */
export const pointOnCurve = (a: Point, b: Point, curve: number, t: number): Point => {
  const c = controlPoint(a, b, curve);
  const u = 1 - t;
  return {
    x: u * u * a.x + 2 * u * t * c.x + t * t * b.x,
    y: u * u * a.y + 2 * u * t * c.y + t * t * b.y,
  };
};

/** Vị trí tại t trên đường gấp khúc qua nhiều điểm, chia theo độ dài. */
export const pointOnPolyline = (points: Point[], t: number): Point => {
  if (points.length === 1) return points[0];
  const lengths = points.slice(1).map((p, i) => Math.hypot(p.x - points[i].x, p.y - points[i].y));
  const total = lengths.reduce((s, l) => s + l, 0) || 1;
  let target = Math.min(Math.max(t, 0), 1) * total;
  for (let i = 0; i < lengths.length; i++) {
    if (target <= lengths[i] || i === lengths.length - 1) {
      const k = lengths[i] ? target / lengths[i] : 0;
      return {
        x: points[i].x + (points[i + 1].x - points[i].x) * k,
        y: points[i].y + (points[i + 1].y - points[i].y) * k,
      };
    }
    target -= lengths[i];
  }
  return points[points.length - 1];
};
