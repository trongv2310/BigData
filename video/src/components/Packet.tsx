import React from "react";
import { fonts } from "../theme/fonts";
import { alpha, ui } from "../theme/palette";
import { Point, pointOnCurve, pointOnPolyline } from "./geometry";

type Props = {
  /** Đường đi: hai điểm (có thể cong) hoặc nhiều điểm (gấp khúc). */
  path: Point[];
  /** Vị trí trên đường, 0..1. Ngoài khoảng này gói tin không hiện. */
  progress: number;
  color: string;
  curve?: number;
  label?: string;
  size?: number;
  shape?: "box" | "dot" | "envelope";
  /** Hiện cả khi progress = 0 hoặc 1 (đứng chờ ở đầu hoặc đã tới nơi). */
  stay?: boolean;
};

/** Gói tin chạy dọc một đường: gói dữ liệu, nhịp tim, phong bì báo cáo. */
export const Packet: React.FC<Props> = ({
  path,
  progress,
  color,
  curve = 0,
  label,
  size = 34,
  shape = "box",
  stay = false,
}) => {
  if (!stay && (progress <= 0 || progress >= 1)) return null;
  const t = Math.min(Math.max(progress, 0), 1);
  const pos =
    path.length === 2 ? pointOnCurve(path[0], path[1], curve, t) : pointOnPolyline(path, t);

  const w = shape === "envelope" ? size * 1.4 : size;
  const h = shape === "dot" ? size : shape === "envelope" ? size : size * 0.8;
  return (
    <div
      style={{
        position: "absolute",
        left: pos.x - w / 2,
        top: pos.y - h / 2,
        width: w,
        height: h,
        borderRadius: shape === "dot" ? "50%" : 6,
        background: color,
        boxShadow: `0 0 18px ${alpha(color, 0.75)}`,
        border: `2px solid ${alpha("#FFFFFF", 0.6)}`,
      }}
    >
      {shape === "envelope" ? (
        <svg width={w} height={h} style={{ position: "absolute", left: -2, top: -2 }}>
          <polyline
            points={`3,4 ${w / 2},${h * 0.58} ${w - 3},4`}
            fill="none"
            stroke={ui.bg}
            strokeWidth={2.5}
          />
        </svg>
      ) : null}
      {label ? (
        <div
          style={{
            position: "absolute",
            top: h + 6,
            left: "50%",
            transform: "translateX(-50%)",
            whiteSpace: "nowrap",
            fontFamily: fonts.sans,
            fontSize: 22,
            fontWeight: 600,
            color,
            textShadow: `0 0 6px ${ui.bg}, 0 0 6px ${ui.bg}`,
          }}
        >
          {label}
        </div>
      ) : null}
    </div>
  );
};
