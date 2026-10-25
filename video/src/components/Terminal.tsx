import React from "react";
import { fonts } from "../theme/fonts";
import { alpha, ui } from "../theme/palette";

export type TermLine = {
  text: string;
  at: number;
  /** Dòng lệnh gõ từng ký tự; dòng kết quả hiện cả dòng. */
  typed?: boolean;
  color?: string;
  highlight?: boolean;
};

type Props = {
  x: number;
  y: number;
  w: number;
  frame: number;
  lines: TermLine[];
  title?: string;
  fontSize?: number;
  opacity?: number;
};

/** Cửa sổ dòng lệnh: gõ lệnh rồi in kết quả. */
export const Terminal: React.FC<Props> = ({
  x,
  y,
  w,
  frame,
  lines,
  title = "WSL2 Ubuntu",
  fontSize = 22,
  opacity = 1,
}) => (
  <div
    style={{
      position: "absolute",
      left: x,
      top: y,
      width: w,
      opacity,
      borderRadius: 12,
      overflow: "hidden",
      border: `2px solid ${ui.panelBorder}`,
      background: "#060A12",
      boxShadow: "0 12px 40px rgba(0,0,0,0.5)",
    }}
  >
    <div
      style={{
        height: 34,
        background: ui.panel,
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "0 14px",
        fontFamily: fonts.sans,
        fontSize: 18,
        color: ui.textDim,
      }}
    >
      {["#EF4444", "#FACC15", "#4ADE80"].map((c) => (
        <span key={c} style={{ width: 12, height: 12, borderRadius: 6, background: c }} />
      ))}
      <span style={{ marginLeft: 8 }}>{title}</span>
    </div>
    <div style={{ padding: "12px 16px 16px" }}>
      {lines.map((l, i) => {
        if (frame < l.at) return null;
        const text = l.typed ? l.text.slice(0, Math.floor((frame - l.at) * 1.2) + 1) : l.text;
        return (
          <div
            key={i}
            style={{
              fontFamily: fonts.mono,
              fontSize,
              lineHeight: 1.5,
              whiteSpace: "pre",
              color: l.color ?? (l.typed ? "#E2E8F0" : ui.textDim),
              background: l.highlight ? alpha("#3B82F6", 0.25) : "transparent",
            }}
          >
            {l.typed ? "$ " : ""}
            {text}
          </div>
        );
      })}
    </div>
  </div>
);
