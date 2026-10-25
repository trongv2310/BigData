import React from "react";
import { fonts } from "../theme/fonts";
import { formatNumber } from "../theme/format";
import { ui } from "../theme/palette";

type Props = {
  from?: number;
  to: number;
  /** 0..1, tiến độ chạy số. */
  progress: number;
  decimals?: number;
  prefix?: string;
  suffix?: string;
  color?: string;
  fontSize?: number;
  weight?: number;
  style?: React.CSSProperties;
};

/** Con số chạy từ from tới to, định dạng kiểu Việt Nam. */
export const Counter: React.FC<Props> = ({
  from = 0,
  to,
  progress,
  decimals = 0,
  prefix = "",
  suffix = "",
  color = ui.text,
  fontSize = 40,
  weight = 700,
  style,
}) => {
  const p = Math.min(Math.max(progress, 0), 1);
  const raw = from + (to - from) * p;
  const value = p >= 1 ? to : decimals === 0 ? Math.round(raw) : raw;
  return (
    <span
      style={{
        fontFamily: fonts.sans,
        fontVariantNumeric: "tabular-nums",
        fontSize,
        fontWeight: weight,
        color,
        whiteSpace: "nowrap",
        ...style,
      }}
    >
      {prefix}
      {formatNumber(value, decimals)}
      {suffix}
    </span>
  );
};
