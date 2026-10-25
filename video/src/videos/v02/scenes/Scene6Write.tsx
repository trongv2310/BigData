import React from "react";
import { useCurrentFrame, useVideoConfig } from "remotion";
import { Arrow, Callout, Node, Packet, Point, SceneShell, useCues } from "../../../components";
import { pop, progress } from "../../../theme/anim";
import { fonts } from "../../../theme/fonts";
import { alpha, component, state, ui } from "../../../theme/palette";
import { windowOpacity } from "../layout";

const C = component.hdfs;
const STEPS = ["create", "addBlock", "chọn vị trí", "đẩy dữ liệu", "ACK", "blockReceived", "complete", "rename"];
// Nguồn của từng bước trên tab "Sơ đồ luồng" (theo từ điển, mục Luồng ghi file):
// bước 2, 3, 7 quan sát trong log NameNode; 4–6 suy ra; 1 và 8 từ audit log nếu bật, không thì suy ra.
const EVIDENCE: Array<"seen" | "inferred" | "audit"> = ["audit", "seen", "seen", "inferred", "inferred", "inferred", "seen", "audit"];
const CLIENT_P = { x: 210, y: 470 };
const NN_P = { x: 960, y: 310 };
const DN_P = [760, 1160, 1560].map((x) => ({ x, y: 680 }));
const PIPE: Point[] = [{ x: 350, y: 470 }, ...DN_P.map((d) => ({ x: d.x, y: d.y - 75 }))];
const PACKETS = 6;

export const Scene6Write: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const { cue, within } = useCues();

  // Khung hình bắt đầu từng bước.
  const stepAt = [cue(0) + 30, cue(1) + 5, within(1, 0.5), cue(2) + 10, cue(3) + 5, within(3, 0.55), cue(4) + 5, within(4, 0.55)];
  const stepState = (i: number) => (frame < stepAt[i] ? "idle" : i < 7 && frame >= stepAt[i + 1] ? "done" : frame >= stepAt[i] + 40 && i === 7 ? "done" : "running");
  const project = frame >= cue(6);
  const ghostOthers = project ? 1 - 0.75 * progress(frame, cue(6), 20) : 1;
  const renamed = frame >= stepAt[7] + 15;

  // Gói dữ liệu đi theo đường ống client → DN1 → DN2 → DN3.
  const pktStart = (k: number) => stepAt[3] + k * 18;
  const pktDur = 70;
  const passed = (dnIndex: number) => {
    let n = 0;
    for (let k = 0; k < PACKETS; k++) {
      // Vị trí của DataNode trên đường ống, theo tỉ lệ độ dài (xấp xỉ đều nhau).
      const at = [0.42, 0.72, 1][dnIndex];
      if (progress(frame, pktStart(k), pktDur, (t) => t) >= at) n++;
    }
    return n / PACKETS;
  };
  const ackP = progress(frame, stepAt[4], 50, (t) => t);

  return (
    <SceneShell index="CẢNH 6" title="Luồng ghi file" color={C}>
      {/* Thanh 8 bước */}
      <div style={{ position: "absolute", left: 60, top: 110, display: "flex", gap: 8 }}>
        {STEPS.map((s, i) => {
          const st = stepState(i);
          const color = st === "running" ? state.running : st === "done" ? C : ui.panelBorder;
          const ev = EVIDENCE[i];
          const borderStyle = project ? (ev === "seen" ? "solid" : ev === "inferred" ? "dashed" : "dotted") : "solid";
          return (
            <div
              key={s}
              style={{
                width: 216,
                height: 60,
                borderRadius: 10,
                border: `3px ${borderStyle} ${st === "idle" ? ui.panelBorder : color}`,
                background: st === "idle" ? alpha(ui.panel, 0.6) : alpha(color, 0.18),
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                gap: 8,
                fontSize: 22,
                fontWeight: 700,
                color: st === "idle" ? ui.textFaint : ui.text,
                boxSizing: "border-box",
              }}
            >
              <span style={{ color: st === "idle" ? ui.textFaint : color }}>{i + 1}</span>
              <span style={{ whiteSpace: "nowrap" }}>{s}</span>
            </div>
          );
        })}
      </div>
      {/* Chú giải nguồn bằng chứng (câu 6–7) */}
      <div style={{ position: "absolute", left: 60, top: 182, display: "flex", gap: 34, fontSize: 21, color: ui.textDim, opacity: windowOpacity(frame, cue(6) + 20) }}>
        {(
          [
            ["solid", "quan sát thật trong log NameNode (allocate, completeFile)"],
            ["dashed", "suy ra"],
            ["dotted", "audit log nếu bật, không thì suy ra"],
          ] as const
        ).map(([style, text]) => (
          <span key={style} style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ width: 40, height: 22, borderRadius: 5, border: `3px ${style} ${ui.text}`, boxSizing: "border-box" }} />
            {text}
          </span>
        ))}
      </div>

      <Node x={CLIENT_P.x} y={CLIENT_P.y} w={280} h={140} kind="client" label="Client" sub="hdfs dfs -put" color={ui.textDim} scale={pop(frame, 0, fps)} />
      <Node x={NN_P.x} y={NN_P.y} w={400} h={140} kind="namenode" label="NameNode" color={C} status={[0, 1, 2, 5, 6].some((i) => stepState(i) === "running") ? "active" : "normal"} scale={pop(frame, 6, fps)}>
        {frame >= stepAt[0] + 20 ? (
          <div style={{ position: "absolute", left: 76, top: 52, fontFamily: fonts.mono, fontSize: 21, color: renamed ? ui.text : ui.textFaint, whiteSpace: "nowrap" }}>
            Dataset.csv
            <span style={{ opacity: renamed ? 1 - progress(frame, stepAt[7] + 15, 15) : 1 }}>._COPYING_</span>
          </div>
        ) : null}
      </Node>
      {DN_P.map((d, i) => {
        const fill = passed(i);
        return (
          <Node key={i} x={d.x} y={d.y} w={290} h={150} kind="datanode" label={`DataNode ${i + 1}`} color={C} opacity={i === 0 ? 1 : ghostOthers} scale={pop(frame, 12 + i * 4, fps)} status={frame >= stepAt[2] && frame < stepAt[3] ? "active" : "normal"}>
            <div style={{ position: "absolute", left: 20, right: 20, bottom: 18, height: 30, borderRadius: 6, border: `2px solid ${alpha(C, 0.6)}`, overflow: "hidden" }}>
              <div style={{ width: `${fill * 100}%`, height: "100%", background: C }} />
            </div>
          </Node>
        );
      })}
      {project ? (
        <div style={{ position: "absolute", left: 1010, top: 790, fontSize: 23, color: ui.textDim, opacity: progress(frame, cue(6) + 10, 15) }}>
          Đồ án: replication 1, đường ống chỉ có DataNode 1
        </div>
      ) : (
        <Callout x={1280} y={786} w={560} variant="illustration" fontSize={20} style={{ opacity: progress(frame, stepAt[2], 12), padding: "6px 14px 8px" }}>
          3 DataNode trong đường ống (replication 3)
        </Callout>
      )}

      {/* Bước 1: create, bước 7: complete */}
      <Arrow from={{ x: 355, y: 430 }} to={{ x: 755, y: 300 }} progress={progress(frame, stepAt[0], 20)} opacity={windowOpacity(frame, stepAt[0], stepAt[1])} color={C} label="create" />
      <Arrow from={{ x: 355, y: 430 }} to={{ x: 755, y: 300 }} progress={progress(frame, stepAt[6], 20)} opacity={windowOpacity(frame, stepAt[6], stepAt[7] + 40)} color={C} label="complete" />
      {/* Bước 2–3: NameNode trả mã block và danh sách DataNode */}
      <Packet path={[{ x: 755, y: 340 }, { x: 355, y: 470 }]} progress={progress(frame, stepAt[1] + 10, 34)} color={C} shape="envelope" size={32} label="mã block + [DN1, DN2, DN3]" />

      {/* Bước 4: các gói 64 KB */}
      {Array.from({ length: PACKETS }).map((_, k) => (
        <Packet key={k} path={PIPE} progress={progress(frame, pktStart(k), pktDur, (t) => t)} color={C} size={30} label={k === 0 ? "gói ≈ 64 KB" : undefined} />
      ))}
      {/* Bước 5: ACK đi ngược */}
      <Packet path={[...PIPE].reverse()} progress={ackP} color={state.ok} shape="dot" size={30} label="ACK ✓" />
      {/* Bước 6: blockReceived */}
      {DN_P.map((d, i) => (
        <Packet key={i} path={[{ x: d.x, y: d.y - 80 }, { x: NN_P.x + (i - 1) * 70, y: NN_P.y + 72 }]} progress={progress(frame, stepAt[5] + i * 6, 30)} color={C} shape="envelope" size={26} label={i === 2 ? "blockReceived" : undefined} />
      ))}

      {/* Câu 5: chuyền tay hồ sơ */}
      <Callout x={1240} y={240} w={600} variant="everyday" fontSize={23} style={{ opacity: windowOpacity(frame, cue(5), cue(6) - 10) }}>
        Chuyền tay bộ hồ sơ: mỗi người photo cất một bản rồi chuyền tiếp; chữ ký nhận truyền ngược về bạn.
      </Callout>
      <Callout x={1240} y={240} w={600} variant="project" fontSize={23} style={{ opacity: windowOpacity(frame, cue(6)) }}>
        Tab <b>Sơ đồ luồng</b> thắp sáng các bước này khi có lượt ghi thật. Viền đứt là bước suy ra, không quan sát trực tiếp.
      </Callout>
    </SceneShell>
  );
};
