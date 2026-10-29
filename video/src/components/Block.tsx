import React from "react";
import { fonts } from "../theme/fonts";
import { alpha, ui } from "../theme/palette";

type Props = {
  /** Góc trên trái. */
  x: number;
  y: number;
  w: number;
  h?: number;
  color: string;
  /** Mã block, ví dụ "blk_1073741827". */
  id?: string;
  /** Nhãn kích thước, ví dụ "1 048 576 byte". */
  size?: string;
  /** Khung nét đứt có kích thước tối đa của block (để thấy block cuối không chiếm đủ). */
  ghostWidth?: number;
  ghostOpacity?: number;
  opacity?: number;
  scale?: number;
  highlight?: number;
  /** Viên gạch nhỏ gọn khi nằm trong DataNode: chỉ hiện mã ngắn. */
  compact?: boolean;
};

/** Một block HDFS vẽ như viên gạch có mã và kích thước. */
export const Block: React.FC<Props> = ({
  x,
  y,
  w,
  h = 86,
  color,
  id,
  size,
  ghostWidth,
  ghostOpacity = 1,
  opacity = 1,
  scale = 1,
  highlight = 0,
  compact = false,
}) => (
  <>
    {ghostWidth && ghostWidth > w ? (
      <div
        style={{
          position: "absolute",
          left: x,
          top: y,
          width: ghostWidth,
          height: h,
          borderRadius: 10,
          border: `3px dashed ${alpha(color, 0.6)}`,
          opacity: opacity * ghostOpacity,
          boxSizing: "border-box",
        }}
      />
    ) : null}
    <div
      style={{
        position: "absolute",
        left: x,
        top: y,
        width: w,
        height: h,
        opacity,
        transform: `scale(${scale})`,
        transformOrigin: "center",
        borderRadius: 10,
        background: `linear-gradient(180deg, ${alpha(color, 0.95)}, ${alpha(color, 0.7)})`,
        border: `3px solid ${alpha("#FFFFFF", 0.35 + highlight * 0.5)}`,
        boxShadow: highlight > 0 ? `0 0 ${30 * highlight}px ${alpha(color, 0.9)}` : `0 4px 14px rgba(0,0,0,0.35)`,
        boxSizing: "border-box",
        overflow: "hidden",
        color: "#FFFFFF",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        alignItems: "center",
        gap: 2,
      }}
    >
      {id ? (
        <div
          style={{
            fontFamily: /^[ -~]*$/.test(id) ? fonts.mono : fonts.sans,
            fontSize: compact ? 15 : 20,
            fontWeight: 600,
            whiteSpace: "nowrap",
            textShadow: `0 1px 2px ${alpha(ui.bg, 0.6)}`,
          }}
        >
          {id}
        </div>
      ) : null}
      {size && !compact ? (
        <div style={{ fontFamily: fonts.sans, fontSize: 21, fontWeight: 600, whiteSpace: "nowrap" }}>
          {size}
        </div>
      ) : null}
    </div>
  </>
);
