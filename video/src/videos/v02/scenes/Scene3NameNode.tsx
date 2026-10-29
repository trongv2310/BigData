import React from "react";
import { useCurrentFrame, useVideoConfig } from "remotion";
import { Arrow, Block, Callout, Ledger, LedgerLine, Node, Packet, SceneShell, useCues } from "../../../components";
import { pop, progress } from "../../../theme/anim";
import { fonts } from "../../../theme/fonts";
import { alpha, component, state, ui } from "../../../theme/palette";
import { DN_PORTS, windowOpacity } from "../layout";

const C = component.hdfs;
const NN_BOX = { x: 520, y: 320, w: 680, h: 340 };
const DN_COL_X = 1640;
const DN_COL_Y = [210, 345, 480, 615];

export const Scene3NameNode: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const { cue, within } = useCues();

  const restart = within(4, 0.15); // màn hình chớp: NameNode khởi động lại
  const flash = windowOpacity(frame, restart, restart + 10, 6);
  const tree: LedgerLine[] = [
    { text: "/raw/Dataset.csv   rw-r--r--   4 block", at: cue(0) + 20 },
    { text: "/co/output/model_best/", at: cue(0) + 45 },
  ];
  const locs = (base: number, step: number): LedgerLine[] =>
    DN_PORTS.map((p, i) => ({ text: `  B${i + 1} → DataNode :${p}`, at: base + i * step, color: C }));

  // Sau khởi động lại: cây thư mục dựng lại từ fsimage + edit log,
  // còn vị trí block chỉ có lại khi báo cáo block tới (câu 5).
  const reportAt = (i: number) => cue(5) + 20 + i * 18;
  const arriveAt = (i: number) => reportAt(i) + 30;
  const rebuilt: LedgerLine[] = [
    { text: "/raw/Dataset.csv   rw-r--r--   4 block", at: within(4, 0.6) },
    { text: "/co/output/model_best/", at: within(4, 0.7) },
    ...DN_PORTS.map((p, i) => ({ text: `  B${i + 1} → DataNode :${p}`, at: arriveAt(i), color: C })),
  ];

  const editLog: LedgerLine[] = [
    { text: "+ mkdir /co/output", at: within(2, 0.55), doneAt: within(4, 0.4) },
    { text: "+ create step_summary.json", at: within(2, 0.65), doneAt: within(4, 0.5) },
    { text: "+ rename …_COPYING_ → tên thật", at: within(2, 0.75), doneAt: within(4, 0.6) },
  ];

  // Câu 1: một block định chui vào NameNode và bị đẩy ra.
  const bounce = progress(frame, within(1, 0.2), 40);
  const bx = 1500 - Math.sin(bounce * Math.PI) * 680;

  return (
    <SceneShell index="CẢNH 3" title="NameNode: người giữ sổ" color={C}>
      <Node
        x={NN_BOX.x}
        y={NN_BOX.y}
        w={NN_BOX.w}
        h={NN_BOX.h}
        kind="namenode"
        label="NameNode"
        sub={frame >= cue(6) ? "RPC :9000 · Web :9870" : "siêu dữ liệu, giữ trong RAM"}
        color={C}
        status={frame >= restart && frame < restart + 40 ? "off" : "normal"}
        scale={pop(frame, 0, fps)}
      />
      <div style={{ opacity: frame < restart ? 1 : 0 }}>
        <Ledger x={NN_BOX.x - 310} y={NN_BOX.y - 92} w={620} title="Sổ trong RAM" frame={frame} color={C} fontSize={20} lines={[...tree, ...locs(cue(0) + 70, 14)]} />
      </div>
      <div style={{ opacity: frame >= restart + 30 ? 1 : 0 }}>
        <Ledger x={NN_BOX.x - 310} y={NN_BOX.y - 92} w={620} title="Sổ trong RAM (dựng lại)" frame={frame} color={C} fontSize={20} lines={rebuilt} />
      </div>

      {/* DataNode giữ block thật */}
      {DN_COL_Y.map((y, i) => (
        <Node key={i} x={DN_COL_X} y={y} w={300} h={118} kind="datanode" label="DataNode" sub={`:${DN_PORTS[i]}`} color={C} scale={pop(frame, 8 + i * 4, fps)}>
          <Block x={196} y={60} w={i === 3 ? 26 : 84} h={38} color={C} id={`B${i + 1}`} compact />
        </Node>
      ))}

      {/* Câu 1: NameNode không giữ dữ liệu thật */}
      {frame >= within(1, 0.2) && frame < within(1, 0.2) + 44 ? (
        <Block x={bx} y={200} w={90} h={40} color={C} id="B1" compact />
      ) : null}
      <div
        style={{
          position: "absolute",
          left: 880,
          top: 120,
          fontSize: 26,
          fontWeight: 700,
          color: state.error,
          opacity: windowOpacity(frame, within(1, 0.45), cue(2)),
        }}
      >
        ✗ dữ liệu thật không vào NameNode
      </div>

      {/* Câu 2: ghi xuống đĩa */}
      <div style={{ opacity: windowOpacity(frame, cue(2)) }}>
        <div style={{ position: "absolute", left: 180, top: 520, fontSize: 24, fontWeight: 700, color: ui.textDim }}>
          Ổ đĩa của NameNode
        </div>
        <div
          style={{
            position: "absolute",
            left: 180,
            top: 560,
            width: 300,
            height: 190,
            borderRadius: 12,
            background: alpha(C, 0.18),
            border: `3px solid ${C}`,
            transform: `translateY(${(1 - progress(frame, within(2, 0.25), 18)) * -120}px)`,
            opacity: progress(frame, within(2, 0.25), 18),
            boxShadow: frame >= within(4, 0.25) && frame < within(4, 0.45) ? `0 0 30px ${C}` : undefined,
            padding: 18,
            boxSizing: "border-box",
          }}
        >
          <div style={{ fontFamily: fonts.mono, fontSize: 28, fontWeight: 600, color: C }}>fsimage</div>
          <div style={{ fontSize: 23, marginTop: 8 }}>ảnh chụp toàn bộ cây thư mục</div>
          <div style={{ fontSize: 20, marginTop: 10, color: ui.textDim }}>như sổ hộ khẩu chép sạch đầu năm</div>
        </div>
        <Ledger
          x={510}
          y={560}
          w={470}
          title="edit log (minh họa)"
          frame={frame}
          color={C}
          lines={editLog}
          fontSize={20}
          opacity={progress(frame, within(2, 0.5), 12)}
        />
      </div>

      {/* Câu 3: ví dụ đời thường */}
      <Callout x={1010} y={520} w={450} variant="everyday" fontSize={24} style={{ opacity: windowOpacity(frame, cue(3), cue(5) - 10) }}>
        Sổ hộ khẩu chép sạch đầu năm + tờ ghi thay đổi:
        <div style={{ marginTop: 6, fontSize: 22, color: ui.textDim }}>
          <div style={{ opacity: progress(frame, within(3, 0.55), 8) }}>• ngày 3: nhập thêm A</div>
          <div style={{ opacity: progress(frame, within(3, 0.7), 8) }}>• ngày 5: chuyển B đi</div>
        </div>
      </Callout>

      {/* Câu 4: khởi động lại */}
      <div
        style={{
          position: "absolute",
          left: 1010,
          top: 780,
          width: 560,
          fontSize: 24,
          fontWeight: 600,
          color: state.running,
          opacity: windowOpacity(frame, restart, cue(5) - 10),
        }}
      >
        Khởi động lại: đọc fsimage, rồi áp từng dòng edit log
      </div>
      <div style={{ position: "absolute", inset: 0, background: "#FFFFFF", opacity: flash * 0.35 }} />

      {/* Câu 5: báo cáo block dựng lại vị trí */}
      {DN_COL_Y.map((y, i) => (
        <Packet
          key={i}
          path={[{ x: DN_COL_X - 150, y }, { x: NN_BOX.x + NN_BOX.w / 2 + 10, y: NN_BOX.y - 20 + i * 28 }]}
          progress={progress(frame, reportAt(i), 30)}
          color={C}
          shape="envelope"
          size={30}
          label={i === 0 ? "báo cáo block" : undefined}
        />
      ))}
      <div
        style={{
          position: "absolute",
          left: 1010,
          top: 560,
          width: 450,
          fontSize: 24,
          color: ui.textDim,
          opacity: windowOpacity(frame, cue(5), cue(6) - 10),
        }}
      >
        Vị trí block không ghi xuống đĩa: NameNode dựng lại từ báo cáo của DataNode.
      </div>

      {/* Câu 6: trong đồ án, backend chỉ đọc */}
      <div style={{ opacity: windowOpacity(frame, cue(6)) }}>
        <Node x={1230} y={815} w={330} h={110} kind="service" label="Backend" sub=":8000" color={component.serving} />
        <Arrow from={{ x: NN_BOX.x + NN_BOX.w / 2 + 6, y: NN_BOX.y - 40 }} to={{ x: 1230, y: 752 }} dashed color={component.serving} progress={progress(frame, cue(6) + 30, 25)} label="chỉ đọc: JMX, WebHDFS" labelOffset={-14} />
      </div>
    </SceneShell>
  );
};
