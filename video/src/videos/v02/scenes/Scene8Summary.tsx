import React from "react";
import { useCurrentFrame, useVideoConfig } from "remotion";
import { SceneShell, useCues } from "../../../components";
import { pop, progress } from "../../../theme/anim";
import { fonts } from "../../../theme/fonts";
import { alpha, component, ui } from "../../../theme/palette";

const C = component.hdfs;
const POINTS = [
  { k: "NameNode", v: "giữ sổ" },
  { k: "DataNode", v: "giữ block" },
  { k: "File", v: "cắt thành block" },
  { k: "Block", v: "có bản sao" },
  { k: "Nhịp tim", v: "báo sống chết" },
];
const TREE = [
  ["/co/raw/Dataset.csv", "3 179 004 byte"],
  ["/co/processed/aqi_clean/", "1 251 944 byte"],
  ["/co/output/model_best/", ""],
  ["/live/co_stream/", ""],
  ["/live/_checkpoints/", ""],
];

export const Scene8Summary: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const { cue, within } = useCues();

  return (
    <SceneShell index="CẢNH 8" title="Tổng kết" color={C}>
      <div style={{ position: "absolute", left: 120, top: 170, display: "flex", gap: 22 }}>
        {POINTS.map((p, i) => (
          <div
            key={p.k}
            style={{
              width: 314,
              padding: "18px 20px",
              borderRadius: 16,
              background: alpha(C, 0.14),
              border: `3px solid ${C}`,
              transform: `scale(${pop(frame, within(0, 0.08 + i * 0.16), fps)})`,
              boxSizing: "border-box",
            }}
          >
            <div style={{ fontSize: 30, fontWeight: 800 }}>{p.k}</div>
            <div style={{ fontSize: 26, color: ui.textDim }}>{p.v}</div>
          </div>
        ))}
      </div>
      <div style={{ position: "absolute", left: 120, top: 360, width: 900, fontFamily: fonts.mono, fontSize: 26, lineHeight: 1.7 }}>
        <div style={{ fontFamily: fonts.sans, fontSize: 24, fontWeight: 700, color: C }}>Thư mục HDFS của đồ án</div>
        {TREE.map(([path, size], i) => (
          <div key={path} style={{ opacity: progress(frame, cue(0) + 30 + i * 10, 10), display: "flex", gap: 24 }}>
            <span style={{ width: 520 }}>{path}</span>
            <span style={{ fontFamily: fonts.sans, color: ui.textDim }}>{size}</span>
          </div>
        ))}
      </div>
      {/* Câu 1: video sau là Kafka */}
      <div
        style={{
          position: "absolute",
          left: 1180,
          top: 420,
          width: 620,
          padding: "26px 30px",
          borderRadius: 18,
          border: `3px solid ${component.kafka}`,
          background: alpha(component.kafka, 0.12),
          opacity: progress(frame, cue(1), 15),
          transform: `translateX(${(1 - progress(frame, cue(1), 20)) * 60}px)`,
        }}
      >
        <div style={{ fontSize: 22, fontWeight: 800, letterSpacing: 1.2, color: component.kafka }}>VIDEO SAU</div>
        <div style={{ fontSize: 44, fontWeight: 800, marginTop: 4 }}>Kafka</div>
        <div style={{ fontSize: 26, color: ui.textDim, marginTop: 6 }}>cuốn sổ nhật ký chung của dữ liệu luồng</div>
      </div>
    </SceneShell>
  );
};
