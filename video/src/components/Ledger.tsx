import React from "react";
import { fonts } from "../theme/fonts";
import { alpha, ui } from "../theme/palette";

export type LedgerLine = {
  text: string;
  /** Khung hình bắt đầu viết dòng này. */
  at: number;
  /** Đã đọc / đã áp dụng: gạch xám. */
  doneAt?: number;
  color?: string;
};

type Props = {
  x: number;
  y: number;
  w: number;
  title: string;
  lines: LedgerLine[];
  frame: number;
  color: string;
  mono?: boolean;
  fontSize?: number;
  opacity?: number;
};

/** Cuốn sổ có các dòng viết dần từng ký tự: sổ NameNode, edit log, checkpoint. */
export const Ledger: React.FC<Props> = ({
  x,
  y,
  w,
  title,
  lines,
  frame,
  color,
  mono = true,
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
      borderRadius: 14,
      background: alpha("#F8FAFC", 0.06),
      border: `2px solid ${alpha(color, 0.7)}`,
      boxSizing: "border-box",
      padding: "12px 18px 14px",
    }}
  >
    <div style={{ fontFamily: fonts.sans, fontSize: 21, fontWeight: 700, color, marginBottom: 8 }}>
      {title}
    </div>
    {lines.map((l, i) => {
      if (frame < l.at) return null;
      // Viết khoảng 1,5 ký tự mỗi khung hình, như tay đang ghi.
      const shown = Math.min(l.text.length, Math.floor((frame - l.at) * 1.5) + 1);
      const done = l.doneAt !== undefined && frame >= l.doneAt;
      return (
        <div
          key={i}
          style={{
            // Dòng có chữ tiếng Việt dùng phông sans (phông mono không có dấu).
            fontFamily: mono && /^[ -~→…]*$/.test(l.text) ? fonts.mono : fonts.sans,
            fontSize,
            lineHeight: 1.5,
            color: done ? ui.textFaint : l.color ?? ui.text,
            textDecoration: done ? "line-through" : "none",
            borderBottom: `1px solid ${alpha(ui.textFaint, 0.25)}`,
            whiteSpace: "nowrap",
            overflow: "hidden",
          }}
        >
          {l.text.slice(0, shown)}
        </div>
      );
    })}
  </div>
);
