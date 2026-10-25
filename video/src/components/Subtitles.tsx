import React from "react";
import { useCurrentFrame } from "remotion";
import { fonts } from "../theme/fonts";
import { ui } from "../theme/palette";
import { Sentence } from "./timing";

/** Phụ đề ở dải dưới cùng, đọc từ manifest (cùng nguồn với file .srt). */
export const Subtitles: React.FC<{ cues: Sentence[] }> = ({ cues }) => {
  const frame = useCurrentFrame();
  const cue = cues.find((c) => frame >= c.start && frame < c.end);
  if (!cue) return null;
  const fade = Math.min(1, (frame - cue.start) / 4, (cue.end - frame) / 4);
  return (
    <div
      style={{
        position: "absolute",
        left: 0,
        right: 0,
        bottom: 34,
        display: "flex",
        justifyContent: "center",
        opacity: fade,
      }}
    >
      <div
        style={{
          maxWidth: 1560,
          padding: "12px 30px 14px",
          borderRadius: 14,
          background: ui.subtitleBg,
          color: "#FFFFFF",
          fontFamily: fonts.sans,
          fontSize: 38,
          fontWeight: 500,
          lineHeight: 1.38,
          textAlign: "center",
          whiteSpace: "pre-line",
          // Chia đều hai dòng, tránh một chữ rơi lẻ xuống dòng dưới.
          textWrap: "balance",
        }}
      >
        {cue.text}
      </div>
    </div>
  );
};
