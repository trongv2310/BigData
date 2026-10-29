import React from "react";
import { Easing, useCurrentFrame, useVideoConfig } from "remotion";
import { Block, Callout, Counter, Node, Packet, SceneShell, useCues } from "../../../components";
import { mix, pop, progress } from "../../../theme/anim";
import { fonts } from "../../../theme/fonts";
import { alpha, component, state, ui } from "../../../theme/palette";
import { DN_PORTS, DN_X, windowOpacity } from "../layout";

const C = component.hdfs;
const ROW_Y = 610;
const SRC = { x: 960, y: 250 };
// Phần minh họa replication 3: bản sao đặt ở DataNode 1, 2, 3; DataNode 2 chết,
// NameNode cho chép bù sang DataNode 4.
const COPY_TO = [0, 1, 2];
const DEAD = 1;

export const Scene4Replication: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const { cue, within } = useCues();

  const zoomOp = windowOpacity(frame, 0, cue(1) - 10);
  const scan = progress(frame, within(0, 0.4), 40);
  const rowOp = windowOpacity(frame, cue(1) - 4);

  // Giai đoạn minh họa (câu 1–3) và giai đoạn đồ án (câu 4 trở đi).
  const illus = frame < cue(4);
  const deadAt = within(3, 0.15);
  const isDead = (i: number) => illus && i === DEAD && frame >= deadAt;
  const repairAt = within(3, 0.45);
  const repair = progress(frame, repairAt, 34);

  // Câu 5: ổ đĩa chung hỏng (minh họa giả định)
  const crackAt = within(5, 0.45);
  const crack = frame >= crackAt && frame < cue(6) ? 1 : 0;
  // Câu 7: tách thành bốn máy riêng
  const split = progress(frame, cue(7), 30, Easing.inOut(Easing.cubic));
  const splitBack = progress(frame, cue(8), 24);
  const sep = split * (1 - splitBack);
  const rowY = ROW_Y - sep * 40;
  const xs = DN_X.map((x, i) => x + (i - 1.5) * 26 * sep);

  return (
    <SceneShell index="CẢNH 4" title="DataNode và replication" color={C}>
      {/* Câu 0: bên trong một DataNode */}
      <div style={{ opacity: zoomOp }}>
        <Node x={960} y={420} w={980} h={380} kind="datanode" label="Bên trong một DataNode" sub="đĩa cục bộ" color={C} scale={pop(frame, 0, fps)} />
        {[
          { name: "blk_1073741827", desc: "dữ liệu của block", x: 560 },
          { name: "blk_….meta", desc: "checksum để phát hiện hỏng", x: 1000 },
        ].map((f, i) => (
          <div
            key={i}
            style={{
              position: "absolute",
              left: f.x,
              top: 360,
              width: 380,
              height: 150,
              borderRadius: 12,
              background: alpha(C, i === 0 ? 0.35 : 0.15),
              border: `3px solid ${C}`,
              padding: "18px 22px",
              boxSizing: "border-box",
              opacity: progress(frame, 14 + i * 10, 12),
            }}
          >
            <div style={{ fontFamily: fonts.mono, fontSize: 28, fontWeight: 600 }}>{f.name}</div>
            <div style={{ fontSize: 23, color: ui.textDim, marginTop: 8 }}>{f.desc}</div>
          </div>
        ))}
        {/* Vạch quét checksum */}
        {scan > 0 && scan < 1 ? (
          <div style={{ position: "absolute", left: 560 + scan * 380, top: 350, width: 6, height: 170, background: state.running, boxShadow: `0 0 16px ${state.running}` }} />
        ) : null}
        <div style={{ position: "absolute", left: 560, top: 530, fontSize: 26, fontWeight: 700, color: state.ok, opacity: progress(frame, within(0, 0.4) + 42, 10) }}>
          ✓ checksum khớp: dữ liệu không hỏng
        </div>
      </div>

      <div style={{ opacity: rowOp }}>
        {/* Ổ đĩa chung (câu 5) và các máy riêng (câu 7) */}
        <div
          style={{
            position: "absolute",
            left: 200,
            top: 500,
            width: 1520,
            height: 240,
            borderRadius: 24,
            border: `3px ${crack ? "solid" : "dashed"} ${crack ? state.error : alpha(ui.textDim, 0.6)}`,
            background: crack ? alpha(state.error, 0.08) : "transparent",
            opacity: windowOpacity(frame, cue(5), cue(7)) ,
          }}
        />
        <div style={{ position: "absolute", left: 220, top: 752, fontSize: 24, fontWeight: 600, color: crack ? state.error : ui.textDim, opacity: windowOpacity(frame, cue(5), cue(7)) }}>
          {crack ? "Ổ đĩa hỏng (giả định): mất cả 4 DataNode, kể cả khi để replication 3" : "4 DataNode cùng nằm trên một ổ đĩa vật lý"}
        </div>

        {xs.map((x, i) => (
          <Node
            key={i}
            x={x}
            y={rowY}
            w={250}
            h={150}
            kind="datanode"
            label="DataNode"
            sub={`:${DN_PORTS[i]}`}
            color={C}
            status={isDead(i) || crack ? "dead" : "normal"}
            scale={pop(frame, cue(1) - 4 + i * 4, fps)}
          >
            {/* Câu 7: mỗi máy có ổ riêng */}
            {sep > 0.05 ? (
              <div style={{ position: "absolute", right: 12, bottom: 10, fontSize: 18, color: ui.textDim, opacity: sep }}>ổ riêng</div>
            ) : null}
          </Node>
        ))}

        {/* ---- Minh họa replication 3 ---- */}
        {illus ? (
          <>
            <Block x={SRC.x - 70} y={SRC.y - 25} w={140} h={50} color={C} id="block X" compact opacity={windowOpacity(frame, cue(1), cue(4) - 10)} />
            {COPY_TO.map((d, k) => {
              const p = progress(frame, within(1, 0.35) + k * 10, 36, Easing.inOut(Easing.cubic));
              const x = mix(SRC.x - 50, DN_X[d] - 60, p);
              const y = mix(SRC.y - 20, ROW_Y + 22, p);
              const dead = isDead(d);
              return p > 0 ? (
                <Block key={k} x={x} y={y} w={100} h={40} color={dead ? state.error : C} id={`bản ${k + 1}`} compact opacity={dead ? 0.5 : 1} />
              ) : null;
            })}
            {/* Chép bù từ DataNode 1 sang DataNode 4 */}
            <Packet path={[{ x: DN_X[0], y: ROW_Y + 90 }, { x: DN_X[3] - 10, y: ROW_Y + 42 }]} curve={-160} progress={repair} color={state.ok} size={30} label={repair > 0.1 && repair < 0.9 ? "chép bù" : undefined} />
            {repair >= 1 ? <Block x={DN_X[3] - 60} y={ROW_Y + 22} w={100} h={40} color={C} id="bản 3'" compact highlight={1 - progress(frame, repairAt + 40, 20)} /> : null}
            <Callout x={120} y={140} w={430} variant="illustration" fontSize={23} style={{ opacity: windowOpacity(frame, cue(1), cue(4) - 10) }}>
              replication 3 (mặc định của HDFS): mỗi bản trên một DataNode khác nhau
            </Callout>
            <Callout x={1340} y={140} w={460} variant="everyday" fontSize={23} style={{ opacity: windowOpacity(frame, cue(2), cue(3) + 30) }}>
              Photo hồ sơ cất ở ba tủ:
              <div style={{ display: "flex", gap: 16, marginTop: 10 }}>
                {[0, 1, 2].map((i) => {
                  const burnt = i === 1 && frame >= within(2, 0.55);
                  return (
                    <div key={i} style={{ width: 90, height: 70, borderRadius: 8, border: `3px solid ${burnt ? state.error : "#FBBF24"}`, background: burnt ? alpha(state.error, 0.4) : alpha("#FBBF24", 0.15), display: "flex", alignItems: "center", justifyContent: "center", fontSize: 20, fontWeight: 700 }}>
                      {burnt ? "cháy" : `tủ ${i + 1}`}
                    </div>
                  );
                })}
              </div>
            </Callout>
            <div style={{ position: "absolute", left: 1310, top: 140, width: 520, opacity: windowOpacity(frame, cue(3), cue(4) - 10) }}>
              <div style={{ fontSize: 34, fontWeight: 700 }}>
                1 GB × 3 = <span style={{ color: C }}>3 GB</span> trên cụm
              </div>
              <div style={{ fontSize: 25, color: ui.textDim, marginTop: 8 }}>chịu được 2 DataNode hỏng cùng lúc</div>
            </div>
          </>
        ) : null}

        {/* ---- Đồ án: replication 1 ---- */}
        {!illus ? (
          <>
            {xs.map((x, i) => (
              <Block key={i} x={x - 60} y={rowY + 22} w={i === 3 ? 30 : 100} h={40} color={crack ? state.error : C} id={`B${i + 1}`} compact opacity={progress(frame, cue(4) + i * 6, 10)} />
            ))}
            <Callout x={120} y={140} w={560} variant="project" fontSize={26} style={{ opacity: windowOpacity(frame, cue(4), cue(6) - 10) }}>
              <span style={{ fontFamily: fonts.mono }}>dfs.replication = 1</span>
              <div style={{ fontSize: 23, color: ui.textDim }}>mỗi block chỉ một bản</div>
            </Callout>
            <div style={{ position: "absolute", left: 120, top: 150, opacity: windowOpacity(frame, cue(6), cue(7) - 10) }}>
              <div style={{ fontSize: 26, color: ui.textDim, fontWeight: 600 }}>Có lúc trên cụm</div>
              <div style={{ display: "flex", gap: 40, alignItems: "baseline", marginTop: 6 }}>
                <span><Counter to={24} progress={progress(frame, cue(6) - 4, 16)} fontSize={72} color={C} /> <span style={{ fontSize: 30 }}>block</span></span>
                <span><Counter to={38} progress={progress(frame, cue(6) - 4, 16)} fontSize={72} color={C} /> <span style={{ fontSize: 30 }}>bản sao</span></span>
              </div>
              <div style={{ fontSize: 24, color: ui.textDim, marginTop: 4 }}>vì một số file từng được ghi với replication lớn hơn 1</div>
            </div>
            <Callout x={120} y={140} w={620} variant="plain" title="Cụm thật nhiều máy" color={C} fontSize={26} style={{ opacity: windowOpacity(frame, cue(7), cue(8) - 10) }}>
              mỗi DataNode một máy, một ổ riêng: để <b>replication 3</b>
            </Callout>
            {/* Câu 8: mỗi DataNode báo dung lượng cả ổ */}
            <div style={{ opacity: windowOpacity(frame, cue(8)) }}>
              {DN_X.map((x, i) => {
                const merge = progress(frame, within(8, 0.5), 30);
                return (
                  <div
                    key={i}
                    style={{
                      position: "absolute",
                      left: mix(x - 95, 960 - 95, merge),
                      top: mix(ROW_Y - 150, 300, merge),
                      width: 190,
                      textAlign: "center",
                      padding: "6px 0",
                      borderRadius: 10,
                      border: `2px solid ${C}`,
                      background: alpha(ui.panel, 0.95),
                      fontSize: 22,
                      fontWeight: 600,
                      opacity: merge >= 1 ? 0 : progress(frame, cue(8) + i * 6, 10),
                    }}
                  >
                    dung lượng cả ổ
                  </div>
                );
              })}
              <div style={{ position: "absolute", left: 660, top: 270, width: 600, textAlign: "center", opacity: progress(frame, within(8, 0.5) + 30, 12) }}>
                <div style={{ fontSize: 56, fontWeight: 800, color: C }}>tổng ≈ 4 TB</div>
                <div style={{ fontSize: 26, color: ui.textDim }}>nhưng chỉ có 1 ổ đĩa vật lý</div>
              </div>
            </div>
          </>
        ) : null}
      </div>
    </SceneShell>
  );
};
