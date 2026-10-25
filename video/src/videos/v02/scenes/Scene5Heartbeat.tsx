import React from "react";
import { Easing, interpolate, useCurrentFrame, useVideoConfig } from "remotion";
import { Callout, Node, Packet, SceneShell, useCues } from "../../../components";
import { pop, progress } from "../../../theme/anim";
import { fonts } from "../../../theme/fonts";
import { alpha, component, state, ui } from "../../../theme/palette";
import { DN_PORTS, DN_X, windowOpacity } from "../layout";

const C = component.hdfs;
const NNP = { x: 960, y: 230 };
const ROW_Y = 640;
const PERIOD_SEC = 3; // dfs.heartbeat.interval mặc định
const STOPPED = 2; // DataNode :9884 ngừng gửi nhịp tim
const DEAD_SEC = 630; // khoảng 10 phút 30 giây

const fmtElapsed = (s: number) => {
  const sec = Math.floor(s);
  if (sec < 60) return `${sec} s`;
  const m = Math.floor(sec / 60);
  const r = sec % 60;
  return r ? `${m} phút ${r} giây` : `${m} phút`;
};

export const Scene5Heartbeat: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const { cue, within } = useCues();
  const period = PERIOD_SEC * fps;
  const start = 20;
  const stopAt = cue(3) + 10;
  const deadAt = within(3, 0.7);

  /** Khung hình của nhịp tim gần nhất (trước frame) của DataNode i. */
  const lastBeat = (i: number) => {
    const phase = start + i * 18;
    if (frame < phase) return null;
    let t = phase + Math.floor((frame - phase) / period) * period;
    if (i === STOPPED && t >= stopAt) {
      t = phase + Math.floor((stopAt - phase) / period) * period;
    }
    return t;
  };

  const tableTitle = frame >= cue(5) ? "localhost:9870 · Datanodes" : "Sổ của NameNode";

  return (
    <SceneShell index="CẢNH 5" title="Heartbeat và báo cáo block" color={C}>
      <Node x={NNP.x} y={NNP.y} w={320} h={140} kind="namenode" label="NameNode" sub=":9870" color={C} scale={pop(frame, 0, fps)} />
      {/* Đồng hồ chu kỳ 3 giây */}
      <div style={{ position: "absolute", left: 90, top: 392, display: "flex", alignItems: "center", gap: 10 }}>
        <svg width={56} height={56}>
          <circle cx={28} cy={28} r={22} fill="none" stroke={ui.panelBorder} strokeWidth={6} />
          <circle
            cx={28}
            cy={28}
            r={22}
            fill="none"
            stroke={state.error}
            strokeWidth={6}
            pathLength={1}
            strokeDasharray={`${((frame - start) % period) / period} 1`}
            transform="rotate(-90 28 28)"
          />
        </svg>
        <div style={{ fontSize: 24, fontWeight: 700 }}>
          mỗi 3 s <span style={{ color: ui.textDim, fontWeight: 500, fontSize: 20 }}>(mặc định)</span>
        </div>
      </div>

      {DN_X.map((x, i) => {
        const lb = lastBeat(i);
        const dead = i === STOPPED && frame >= deadAt;
        const glow = lb === null ? 0 : Math.max(0, 1 - (frame - lb) / 14);
        const travel = lb === null ? 0 : progress(frame, lb, 26, Easing.out(Easing.quad));
        const stopped = i === STOPPED && frame >= stopAt;
        return (
          <React.Fragment key={i}>
            <Node x={x} y={ROW_Y} w={250} h={150} kind="datanode" label="DataNode" sub={`:${DN_PORTS[i]}`} color={C} heartbeat={stopped && lb !== null && frame - lb > 14 ? 0 : glow} status={dead ? "dead" : "normal"} scale={pop(frame, 6 + i * 4, fps)} />
            {lb !== null && !(stopped && frame - lb > 26) ? (
              <Packet path={[{ x, y: ROW_Y - 80 }, { x: NNP.x + (i - 1.5) * 60, y: NNP.y + 74 }]} progress={travel} color={state.error} shape="dot" size={20} />
            ) : null}
            {/* Câu 1: NameNode trả lời, đôi khi kèm lệnh */}
            {frame >= cue(1) && lb !== null && !stopped ? (
              <Packet
                path={[{ x: NNP.x + (i - 1.5) * 60 + 20, y: NNP.y + 74 }, { x: x + 30, y: ROW_Y - 80 }]}
                progress={progress(frame, lb + 28, 24)}
                color={i === 3 && frame >= within(1, 0.4) && frame < cue(2) + 30 ? state.running : alpha(ui.text, 0.7)}
                shape={i === 3 && frame >= within(1, 0.4) && frame < cue(2) + 30 ? "envelope" : "dot"}
                size={i === 3 && frame >= within(1, 0.4) && frame < cue(2) + 30 ? 26 : 12}
                label={i === 3 && frame >= within(1, 0.4) && frame < cue(2) + 30 ? "lệnh trong câu trả lời" : undefined}
              />
            ) : null}
          </React.Fragment>
        );
      })}

      {/* Bảng Last contact */}
      <div
        style={{
          position: "absolute",
          left: 90,
          top: 130,
          width: 560,
          borderRadius: 14,
          background: alpha(ui.panel, 0.95),
          border: `2px solid ${frame >= cue(5) ? C : ui.panelBorder}`,
          padding: "12px 18px",
          boxSizing: "border-box",
          opacity: windowOpacity(frame, cue(0) + 60),
        }}
      >
        <div style={{ fontSize: 22, fontWeight: 700, color: C, fontFamily: frame >= cue(5) ? fonts.mono : fonts.sans }}>{tableTitle}</div>
        <div style={{ display: "flex", fontSize: 20, color: ui.textDim, marginTop: 6 }}>
          <span style={{ width: 200 }}>DataNode</span>
          <span style={{ width: 210 }}>Last contact</span>
          <span>Trạng thái</span>
        </div>
        {DN_PORTS.map((p, i) => {
          const lb = lastBeat(i) ?? 0;
          let secs = (frame - lb) / fps;
          if (i === STOPPED && frame >= stopAt) {
            secs = interpolate(frame, [stopAt + 20, deadAt], [(stopAt + 20 - lb) / fps, DEAD_SEC], {
              extrapolateLeft: "clamp",
              extrapolateRight: "clamp",
              easing: Easing.in(Easing.cubic),
            });
          }
          const dead = i === STOPPED && frame >= deadAt;
          return (
            <div key={p} style={{ display: "flex", fontSize: 24, lineHeight: 1.6, color: dead ? state.error : ui.text }}>
              <span style={{ width: 200, fontFamily: fonts.mono }}>:{p}</span>
              <span style={{ width: 210, fontVariantNumeric: "tabular-nums" }}>{fmtElapsed(secs)}</span>
              <span style={{ fontWeight: 700, color: dead ? state.error : state.ok }}>{dead ? "Dead" : "Live"}</span>
            </div>
          );
        })}
        {frame >= stopAt + 20 && frame < deadAt ? (
          <div style={{ fontSize: 19, color: state.running, marginTop: 4 }}>⏩ tua nhanh thời gian</div>
        ) : null}
      </div>
      <div style={{ position: "absolute", left: 90, top: 462, width: 560, fontSize: 22, color: ui.textDim, opacity: windowOpacity(frame, deadAt, cue(4) + 40) }}>
        Quá ≈ 10 phút 30 giây: NameNode coi là chết, ra lệnh chép bù block từ bản sao còn lại.
      </div>

      {/* Câu 2: lớp học trực tuyến */}
      <Callout x={1330} y={120} w={520} variant="everyday" fontSize={23} style={{ opacity: windowOpacity(frame, cue(2), cue(3) + 60) }}>
        Lớp học trực tuyến: bấm “có mặt” theo nhịp
        <div style={{ display: "flex", gap: 10, marginTop: 10 }}>
          {[0, 1, 2, 3].map((i) => {
            const lb = lastBeat(i);
            const on = lb !== null && frame - lb < 14 && !(i === STOPPED && frame >= stopAt);
            const absent = i === STOPPED && frame >= deadAt;
            return (
              <div key={i} style={{ flex: 1, height: 64, borderRadius: 8, border: `2px solid ${absent ? state.error : "#FBBF24"}`, background: on ? alpha("#FBBF24", 0.5) : "transparent", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18, fontWeight: 700, color: absent ? state.error : ui.text }}>
                {absent ? "Vắng" : "Có mặt"}
              </div>
            );
          })}
        </div>
      </Callout>

      {/* Câu 4: báo cáo block */}
      <Packet path={[{ x: DN_X[0] + 60, y: ROW_Y - 80 }, { x: NNP.x - 140, y: NNP.y + 40 }]} curve={60} progress={progress(frame, within(4, 0.2), 110, Easing.inOut(Easing.sin))} color={C} shape="envelope" size={44} label="báo cáo block" />
      <Callout x={1330} y={120} w={520} variant="plain" title="Báo cáo block" color={C} fontSize={23} style={{ opacity: windowOpacity(frame, cue(4), cue(5) - 10) }}>
        Danh sách mọi block DataNode đang giữ: gửi lúc khởi động và mặc định 6 giờ một lần.
      </Callout>

      {/* Câu 5: trong đồ án */}
      <Callout x={1330} y={120} w={520} variant="project" fontSize={23} style={{ opacity: windowOpacity(frame, cue(5)) }}>
        Tắt một DataNode, mở <b>localhost:9870</b>: cột Last contact tăng dần tới khi node chuyển sang <b style={{ color: state.error }}>Dead</b>.
      </Callout>
    </SceneShell>
  );
};
