import React from "react";
import { fonts } from "../theme/fonts";
import { alpha, component, state, ui } from "../theme/palette";

export type CalloutVariant = "everyday" | "project" | "illustration" | "inferred" | "warning" | "plain";

const variants: Record<CalloutVariant, { title?: string; color: string; dashed?: boolean }> = {
  everyday: { title: "Đời thường", color: "#FBBF24" },
  project: { title: "Trong đồ án", color: component.hdfs },
  illustration: { title: "Minh họa", color: ui.textDim, dashed: true },
  inferred: { title: "Suy ra", color: ui.textDim, dashed: true },
  warning: { title: "Lưu ý", color: state.running },
  plain: { color: ui.panelBorder },
};

type Props = {
  x: number;
  y: number;
  w?: number;
  variant?: CalloutVariant;
  /** Ghi đè tiêu đề mặc định của kiểu. */
  title?: string;
  color?: string;
  children: React.ReactNode;
  style?: React.CSSProperties;
  fontSize?: number;
};

/** Khung chú thích: ví dụ đời thường, ví dụ đồ án, nhãn "Minh họa", "Suy ra". */
export const Callout: React.FC<Props> = ({
  x,
  y,
  w = 520,
  variant = "plain",
  title,
  color,
  children,
  style,
  fontSize = 26,
}) => {
  const v = variants[variant];
  const c = color ?? v.color;
  const heading = title ?? v.title;
  return (
    <div
      style={{
        position: "absolute",
        left: x,
        top: y,
        width: w,
        boxSizing: "border-box",
        padding: "16px 22px 18px",
        borderRadius: 16,
        background: alpha(ui.panel, 0.94),
        border: `3px ${v.dashed ? "dashed" : "solid"} ${alpha(c, 0.85)}`,
        fontFamily: fonts.sans,
        color: ui.text,
        fontSize,
        lineHeight: 1.4,
        ...style,
      }}
    >
      {heading ? (
        <div
          style={{
            fontSize: 20,
            fontWeight: 800,
            letterSpacing: 1.2,
            textTransform: "uppercase",
            color: c,
            marginBottom: 6,
          }}
        >
          {heading}
        </div>
      ) : null}
      {children}
    </div>
  );
};
