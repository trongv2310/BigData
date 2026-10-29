import React from "react";
import { AbsoluteFill, useCurrentFrame } from "remotion";
import { appear } from "../theme/anim";
import { fonts } from "../theme/fonts";
import { alpha, ui } from "../theme/palette";

type Props = {
  /** Ví dụ "Cảnh 2". */
  index: string;
  title: string;
  color: string;
  children: React.ReactNode;
};

/**
 * Nền chung của mỗi cảnh: lưới mờ, nhãn cảnh ở góc trên trái.
 * Vùng vẽ an toàn: y từ 110 đến 860; dưới 880 dành cho phụ đề.
 */
export const SceneShell: React.FC<Props> = ({ index, title, color, children }) => {
  const frame = useCurrentFrame();
  return (
    <AbsoluteFill
      style={{
        background: `radial-gradient(1200px 700px at 50% 35%, ${ui.bgSoft}, ${ui.bg})`,
        fontFamily: fonts.sans,
        color: ui.text,
        overflow: "hidden",
      }}
    >
      <svg width={1920} height={1080} style={{ position: "absolute", inset: 0, opacity: 0.35 }}>
        <defs>
          <pattern id="grid" width={60} height={60} patternUnits="userSpaceOnUse">
            <path d="M 60 0 L 0 0 0 60" fill="none" stroke={ui.panelBorder} strokeWidth={1} />
          </pattern>
        </defs>
        <rect width={1920} height={1080} fill="url(#grid)" />
      </svg>
      <div
        style={{
          position: "absolute",
          left: 56,
          top: 36,
          display: "flex",
          alignItems: "center",
          gap: 16,
          ...appear(frame, 0, 14),
        }}
      >
        <div
          style={{
            padding: "6px 16px",
            borderRadius: 10,
            background: alpha(color, 0.18),
            border: `2px solid ${color}`,
            color,
            fontSize: 22,
            fontWeight: 700,
            letterSpacing: 1,
          }}
        >
          {index}
        </div>
        <div style={{ fontSize: 34, fontWeight: 700 }}>{title}</div>
      </div>
      {children}
    </AbsoluteFill>
  );
};
