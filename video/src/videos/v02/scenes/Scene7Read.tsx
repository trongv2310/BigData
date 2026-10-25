import React from "react";
import { Easing, useCurrentFrame, useVideoConfig } from "remotion";
import { Arrow, Block, Callout, Node, Packet, SceneShell, useCues } from "../../../components";
import { mix, pop, progress } from "../../../theme/anim";
import { fonts } from "../../../theme/fonts";
import { alpha, component, state, ui } from "../../../theme/palette";
import { DN_PORTS, DN_X, DN_Y, NN, windowOpacity } from "../layout";

const C = component.hdfs;
const CL = { x: 250, y: 260 };
const ASSEMBLY = { x: 120, y: 400 };

export const Scene7Read: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const { cue, cueEnd, within } = useCues();

  const spark = frame >= cue(2) + 10;
  const ask = progress(frame, cue(0) + 20, 20);
  const reply = progress(frame, cue(0) + 45, 26);
  // Thứ tự đọc: B1, B2 (lần đầu hỏng ở câu 1), B3, B4.
  // Câu 1 ("checksum sai…") chỉ khoảng 2 giây: block đỏ hiện tới gần hết câu,
  // đọc lại bản sao và hai block còn lại kéo sang đầu câu 2.
  const failAt = cue(1) + 2;
  const retryAt = Math.max(failAt + 45, cueEnd(1) - 8);
  const readAt = [within(0, 0.55), failAt, retryAt + 20, retryAt + 32];
  const illusEnd = cue(2) + 40; // khung "Minh họa" và bản sao B2' giữ thêm cho kịp đọc
  const safeOn = frame >= cue(3) && frame < within(3, 0.75);
  const steps = [
    { text: "HDFS: 4/4 DataNode sống", at: cue(4) + 10 },
    { text: "hdfs dfsadmin -safemode wait", at: cue(4) + 40, mono: true },
    { text: "Kafka", at: cue(4) + 80 },
    { text: "Spark Streaming (ghi /live)", at: cue(4) + 110 },
    { text: "Backend", at: cue(4) + 140 },
  ];

  const fetch = (i: number, from: { x: number; y: number }, start: number, slot: number) => {
    const p = progress(frame, start, 36, Easing.inOut(Easing.cubic));
    if (p <= 0) return null;
    const x = mix(from.x - 50, ASSEMBLY.x + slot * 105, p);
    const y = mix(from.y, ASSEMBLY.y, p);
    return <Block key={`${i}-${start}`} x={x} y={y} w={i === 3 ? 30 : 100} h={40} color={spark ? component.spark : C} id={`B${i + 1}`} compact highlight={p >= 1 ? 0 : 0.6} />;
  };

  return (
    <SceneShell index="CẢNH 7" title="Luồng đọc và safe mode" color={C}>
      <Node x={CL.x} y={CL.y} w={260} h={140} kind="client" label={spark ? "Spark" : "Client"} sub={spark ? "đọc aqi_clean" : "đọc file"} color={spark ? component.spark : ui.textDim} scale={pop(frame, 0, fps)} />
      <Node x={NN.x} y={NN.y} w={320} h={140} kind="namenode" label="NameNode" sub={safeOn ? "SAFE MODE · chỉ đọc" : ":9000"} color={safeOn ? state.running : C} status={ask > 0 && reply < 1 ? "active" : "normal"} scale={pop(frame, 6, fps)} />
      {DN_X.map((x, i) => (
        <Node key={i} x={x} y={DN_Y} w={250} h={150} kind="datanode" label="DataNode" sub={`:${DN_PORTS[i]}`} color={C} scale={pop(frame, 10 + i * 4, fps)}>
          <Block x={70} y={92} w={i === 3 ? 30 : 100} h={40} color={C} id={`B${i + 1}`} compact />
          {i === 2 ? <Block x={180} y={92} w={56} h={40} color={C} id="B2'" compact opacity={0.75 * windowOpacity(frame, cue(1), illusEnd)} /> : null}
        </Node>
      ))}

      <Arrow from={{ x: 385, y: 240 }} to={{ x: 795, y: 240 }} progress={ask} color={C} label="getBlockLocations" opacity={windowOpacity(frame, cue(0), cue(3))} />
      <Packet path={[{ x: 795, y: 290 }, { x: 385, y: 290 }]} progress={reply} color={C} shape="envelope" size={30} label="B1…B4 + vị trí" />
      <div style={{ position: "absolute", left: ASSEMBLY.x, top: ASSEMBLY.y - 44, fontSize: 22, color: ui.textDim, opacity: progress(frame, readAt[0], 10) }}>
        đọc thẳng từ DataNode, kiểm checksum từng block
      </div>
      {fetch(0, { x: DN_X[0], y: DN_Y + 17 }, readAt[0], 0)}
      {/* B2: lần đầu checksum sai ở DataNode 2 (minh họa), đọc lại bản sao ở DataNode 3 */}
      {frame >= failAt && frame < retryAt ? (
        <>
          <Block x={DN_X[1] - 50} y={DN_Y + 17} w={100} h={40} color={state.error} id="B2" compact highlight={1} />
          <div style={{ position: "absolute", left: DN_X[1] - 110, top: DN_Y - 140, fontSize: 26, fontWeight: 800, color: state.error }}>✗ checksum sai</div>
        </>
      ) : null}
      {fetch(1, { x: DN_X[2] + 50, y: DN_Y + 17 }, retryAt, 1)}
      {fetch(2, { x: DN_X[2], y: DN_Y + 17 }, readAt[2], 2)}
      {fetch(3, { x: DN_X[3], y: DN_Y + 17 }, readAt[3], 3)}
      <Callout x={1300} y={150} w={520} variant="illustration" fontSize={22} style={{ opacity: windowOpacity(frame, cue(1), illusEnd) }}>
        Checksum sai: chuyển sang bản sao khác và báo NameNode block hỏng (cần replication &gt; 1)
      </Callout>
      <Callout x={1300} y={150} w={520} variant="project" fontSize={23} style={{ opacity: windowOpacity(frame, illusEnd + 12, cue(3) - 6) }}>
        Spark đọc <span style={{ fontFamily: fonts.mono }}>/co/processed/aqi_clean</span> theo đúng đường này.
      </Callout>

      {/* Câu 3: báo cáo block trong safe mode */}
      {DN_X.map((x, i) => (
        <Packet key={i} path={[{ x, y: DN_Y - 80 }, { x: NN.x + (i - 1.5) * 60, y: NN.y + 72 }]} progress={progress(frame, cue(3) + 20 + i * 10, 30)} color={C} shape="envelope" size={26} />
      ))}
      <div style={{ position: "absolute", left: 1135, top: 330, fontSize: 23, fontWeight: 600, color: C, opacity: windowOpacity(frame, cue(3) + 20, cue(4) - 6) }}>
        ← DataNode gửi báo cáo block
      </div>
      <Callout x={1300} y={150} w={520} variant="plain" title="Safe mode" color={state.running} fontSize={23} style={{ opacity: windowOpacity(frame, cue(3), cue(4) - 6) }}>
        Ngay sau khởi động NameNode chỉ đọc, chờ đủ DataNode báo cáo block rồi mới cho ghi.
      </Callout>

      {/* Câu 4: thứ tự của start_all.sh */}
      <div
        style={{
          position: "absolute",
          left: 1300,
          top: 150,
          width: 520,
          borderRadius: 14,
          background: alpha(ui.panel, 0.95),
          border: `2px solid ${C}`,
          padding: "14px 20px",
          boxSizing: "border-box",
          opacity: windowOpacity(frame, cue(4)),
        }}
      >
        <div style={{ fontFamily: fonts.mono, fontSize: 22, color: C, fontWeight: 600, marginBottom: 6 }}>bash infra/start_all.sh</div>
        {steps.map((s, i) => {
          const on = frame >= s.at;
          return (
            <div key={i} style={{ display: "flex", gap: 12, fontSize: 23, lineHeight: 1.7, color: on ? ui.text : ui.textFaint }}>
              <span style={{ color: on ? state.ok : ui.textFaint, width: 22 }}>{on ? "✓" : "·"}</span>
              <span style={{ fontFamily: s.mono ? fonts.mono : fonts.sans, fontSize: s.mono ? 20 : 23 }}>{s.text}</span>
            </div>
          );
        })}
      </div>
    </SceneShell>
  );
};
