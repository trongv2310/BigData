import React, { useId } from "react";
import { fonts } from "../theme/fonts";
import { ui } from "../theme/palette";
import { controlPoint, Point, pointOnCurve } from "./geometry";

type Props = {
  from: Point;
  to: Point;
  /** Phần đường đã vẽ, 0..1. Đầu mũi tên chỉ hiện khi vẽ xong. */
  progress?: number;
  color?: string;
  width?: number;
  dashed?: boolean;
  /** Độ cong (px), dương là lệch sang trái theo chiều đi. */
  curve?: number;
  label?: string;
  labelColor?: string;
  labelOffset?: number;
  opacity?: number;
  head?: boolean;
};

/** Mũi tên vẽ dần theo progress; nét đứt dùng cho "đọc" hoặc "suy ra". */
export const Arrow: React.FC<Props> = ({
  from,
  to,
  progress = 1,
  color = ui.textDim,
  width = 4,
  dashed = false,
  curve = 0,
  label,
  labelColor,
  labelOffset = -22,
  opacity = 1,
  head = true,
}) => {
  const id = useId().replace(/:/g, "");
  const c = controlPoint(from, to, curve);
  const d = `M ${from.x} ${from.y} Q ${c.x} ${c.y} ${to.x} ${to.y}`;
  const p = Math.min(Math.max(progress, 0), 1);
  if (p <= 0) return null;

  // Hướng đầu mũi tên lấy theo tiếp tuyến ở cuối đường.
  const near = pointOnCurve(from, to, curve, 0.97);
  const angle = (Math.atan2(to.y - near.y, to.x - near.x) * 180) / Math.PI;
  const mid = pointOnCurve(from, to, curve, 0.5);
  const headSize = width * 3.2;

  return (
    <svg
      width={1920}
      height={1080}
      style={{ position: "absolute", left: 0, top: 0, overflow: "visible", opacity }}
    >
      <defs>
        <mask id={`m${id}`} maskUnits="userSpaceOnUse">
          <path
            d={d}
            fill="none"
            stroke="white"
            strokeWidth={width + 6}
            pathLength={1}
            strokeDasharray={`${p} 1`}
          />
        </mask>
      </defs>
      <path
        d={d}
        fill="none"
        stroke={color}
        strokeWidth={width}
        strokeLinecap="round"
        strokeDasharray={dashed ? `${width * 3} ${width * 2.5}` : undefined}
        mask={`url(#m${id})`}
      />
      {head && p >= 0.98 ? (
        <polygon
          points={`0,0 ${-headSize * 1.6},${-headSize} ${-headSize * 1.6},${headSize}`}
          fill={color}
          transform={`translate(${to.x} ${to.y}) rotate(${angle})`}
        />
      ) : null}
      {label && p > 0.4 ? (
        <text
          x={mid.x}
          y={mid.y + labelOffset}
          textAnchor="middle"
          fill={labelColor ?? color}
          fontFamily={fonts.sans}
          fontSize={24}
          fontWeight={600}
          opacity={Math.min(1, (p - 0.4) / 0.3)}
          style={{ paintOrder: "stroke", stroke: ui.bg, strokeWidth: 6 }}
        >
          {label}
        </text>
      ) : null}
    </svg>
  );
};
