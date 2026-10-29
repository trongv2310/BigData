import React from "react";
import { fonts } from "../theme/fonts";
import { alpha, state as stateColor, ui } from "../theme/palette";

export type NodeKind = "namenode" | "datanode" | "client" | "disk" | "service";
export type NodeStatus = "normal" | "active" | "dead" | "off";

type Props = {
  /** Tâm của nút. */
  x: number;
  y: number;
  w?: number;
  h?: number;
  kind: NodeKind;
  label: string;
  sub?: string;
  color: string;
  status?: NodeStatus;
  /** 0..1, độ sáng của nhịp tim ở góc trên phải; không truyền thì không vẽ. */
  heartbeat?: number;
  opacity?: number;
  scale?: number;
  /** Nội dung bên trong thân nút (ví dụ block đang được cất). */
  children?: React.ReactNode;
};

const Icon: React.FC<{ kind: NodeKind; color: string }> = ({ kind, color }) => {
  const s = { fill: "none", stroke: color, strokeWidth: 3, strokeLinecap: "round" as const };
  switch (kind) {
    case "namenode":
      // Máy chủ và cuốn sổ mục lục
      return (
        <svg width={52} height={52} viewBox="0 0 52 52">
          <rect x={4} y={6} width={26} height={40} rx={4} {...s} />
          <line x1={10} y1={16} x2={24} y2={16} {...s} />
          <line x1={10} y1={26} x2={24} y2={26} {...s} />
          <path d="M34 14 h12 v30 h-12 z M34 22 h12" {...s} />
        </svg>
      );
    case "datanode":
      // Kệ có ngăn
      return (
        <svg width={52} height={52} viewBox="0 0 52 52">
          <rect x={6} y={4} width={40} height={44} rx={3} {...s} />
          <line x1={6} y1={19} x2={46} y2={19} {...s} />
          <line x1={6} y1={34} x2={46} y2={34} {...s} />
        </svg>
      );
    case "client":
      // Máy tính xách tay
      return (
        <svg width={52} height={52} viewBox="0 0 52 52">
          <rect x={9} y={10} width={34} height={22} rx={3} {...s} />
          <path d="M4 40 h44 l-4 -6 h-36 z" {...s} />
        </svg>
      );
    case "disk":
      return (
        <svg width={52} height={52} viewBox="0 0 52 52">
          <ellipse cx={26} cy={12} rx={18} ry={6} {...s} />
          <path d="M8 12 v26 c0 8 36 8 36 0 v-26" {...s} />
        </svg>
      );
    default:
      return (
        <svg width={52} height={52} viewBox="0 0 52 52">
          <rect x={6} y={8} width={40} height={36} rx={6} {...s} />
        </svg>
      );
  }
};

const Heart: React.FC<{ glow: number; color: string }> = ({ glow, color }) => (
  <svg
    width={34}
    height={34}
    viewBox="0 0 24 24"
    style={{
      position: "absolute",
      right: 12,
      bottom: 12,
      transform: `scale(${1 + glow * 0.35})`,
      filter: glow > 0.05 ? `drop-shadow(0 0 ${8 * glow}px ${color})` : undefined,
    }}
  >
    <path
      d="M12 21s-7.5-4.6-9.5-9.2C1 8.3 3.2 4.5 7 4.5c2.1 0 3.6 1.2 5 3 1.4-1.8 2.9-3 5-3 3.8 0 6 3.8 4.5 7.3C19.5 16.4 12 21 12 21z"
      fill={alpha(color, 0.25 + glow * 0.75)}
      stroke={color}
      strokeWidth={1.5}
    />
  </svg>
);

/** Một máy hoặc tiến trình trong cụm: NameNode, DataNode, client, ổ đĩa. */
export const Node: React.FC<Props> = ({
  x,
  y,
  w = 240,
  h = 150,
  kind,
  label,
  sub,
  color,
  status = "normal",
  heartbeat,
  opacity = 1,
  scale = 1,
  children,
}) => {
  const dead = status === "dead";
  const off = status === "off";
  const accent = dead ? stateColor.error : off ? stateColor.idle : color;
  const active = status === "active";
  return (
    <div
      style={{
        position: "absolute",
        left: x - w / 2,
        top: y - h / 2,
        width: w,
        height: h,
        opacity: off ? opacity * 0.45 : opacity,
        transform: `scale(${scale})`,
        borderRadius: 18,
        background: dead ? alpha(stateColor.error, 0.12) : alpha(accent, 0.1),
        border: `3px solid ${accent}`,
        boxShadow: active ? `0 0 34px ${alpha(accent, 0.6)}` : "0 6px 24px rgba(0,0,0,0.35)",
        fontFamily: fonts.sans,
        color: ui.text,
        boxSizing: "border-box",
      }}
    >
      <div style={{ position: "absolute", left: 14, top: 12 }}>
        <Icon kind={kind} color={accent} />
      </div>
      {heartbeat !== undefined ? <Heart glow={dead ? 0 : heartbeat} color={dead ? stateColor.idle : stateColor.error} /> : null}
      <div
        style={{
          position: "absolute",
          left: 76,
          right: 12,
          top: 14,
          fontSize: 28,
          fontWeight: 700,
          lineHeight: 1.15,
          color: dead ? stateColor.error : ui.text,
        }}
      >
        {label}
      </div>
      {sub ? (
        <div
          style={{
            position: "absolute",
            left: 76,
            top: 50,
            fontSize: 21,
            // Phông mono không có dấu tiếng Việt: chỉ dùng cho cổng, lệnh.
            fontFamily: /^[\x20-\x7E]*$/.test(sub) ? fonts.mono : fonts.sans,
            color: ui.textDim,
            whiteSpace: "nowrap",
          }}
        >
          {sub}
        </div>
      ) : null}
      {dead ? (
        <div
          style={{
            position: "absolute",
            // Có tim ở góc phải dưới thì chữ DEAD chuyển sang trái.
            ...(heartbeat !== undefined ? { left: 76 } : { right: 12 }),
            bottom: 10,
            fontSize: 22,
            fontWeight: 800,
            color: stateColor.error,
          }}
        >
          DEAD
        </div>
      ) : null}
      {children}
    </div>
  );
};
