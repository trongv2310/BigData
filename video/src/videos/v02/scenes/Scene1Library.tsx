import React from "react";
import { useCurrentFrame, useVideoConfig } from "remotion";
import { Arrow, Callout, Node, Packet, SceneShell, useCues } from "../../../components";
import { pop, progress } from "../../../theme/anim";
import { fonts } from "../../../theme/fonts";
import { alpha, component, state, ui } from "../../../theme/palette";
import { CLIENT, DN_PORTS, DN_X, DN_Y, NN, windowOpacity } from "../layout";

const C = component.hdfs;

/** Nhãn đổi dần từ cách gọi đời thường sang tên thật. */
const morph = (frame: number, at: number, a: string, b: string) => (frame < at + 8 ? a : b);

export const Scene1Library: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const { cue, within } = useCues();

  // Câu 0–1: nhiều máy nhưng một cây thư mục.
  const treeOp = windowOpacity(frame, cue(0) + 20, cue(2) - 6);
  // Câu 2 trở đi: thư viện.
  const libOp = windowOpacity(frame, cue(2));
  const renameAt = cue(4);
  const nnLabel = morph(frame, renameAt, "Quầy thủ thư", "NameNode");
  const dnLabel = (i: number) => morph(frame, renameAt + 10 * i, `Kệ ${"ABCD"[i]}`, "DataNode");
  const clientLabel = morph(frame, renameAt + 40, "Bạn", "Client");
  const real = frame >= cue(5);

  // Câu 3: hỏi, nhận trả lời, rồi tự đi lấy sách ở kệ A và C.
  const ask = progress(frame, cue(3), 24);
  const answer = progress(frame, cue(3) + 26, 24);
  const walkA = progress(frame, cue(3) + 54, 30);
  const walkC = progress(frame, cue(3) + 90, 34);
  const shelfA = { x: DN_X[0], y: DN_Y - 90 };
  const shelfC = { x: DN_X[2], y: DN_Y - 90 };

  return (
    <SceneShell index="CẢNH 1" title="HDFS là một thư viện" color={C}>
      {/* Câu 0–1: bốn máy, một cây thư mục */}
      <div style={{ opacity: treeOp }}>
        {DN_X.map((x, i) => (
          <Node
            key={i}
            x={x}
            y={DN_Y + 20}
            kind="datanode"
            label={`Máy ${i + 1}`}
            color={C}
            scale={pop(frame, cue(0) + 10 + i * 6, fps)}
          />
        ))}
        <div
          style={{
            position: "absolute",
            left: 660,
            top: 170,
            width: 600,
            padding: "20px 28px",
            borderRadius: 18,
            background: alpha(ui.panel, 0.95),
            border: `3px solid ${C}`,
            fontFamily: fonts.mono,
            fontSize: 28,
            lineHeight: 1.6,
            opacity: progress(frame, cue(0) + 40, 15),
          }}
        >
          <div style={{ fontFamily: fonts.sans, fontWeight: 700, color: C, fontSize: 24 }}>
            Người dùng chỉ thấy một cây thư mục
          </div>
          <div>/</div>
          <div>├─ co/raw/Dataset.csv</div>
          <div>└─ live/co_stream/</div>
        </div>
        {DN_X.map((x, i) => (
          <Arrow
            key={i}
            from={{ x: 960, y: 420 }}
            to={{ x, y: DN_Y - 60 }}
            dashed
            color={alpha(C, 0.8)}
            width={3}
            head={false}
            progress={progress(frame, cue(0) + 60 + i * 5, 20)}
          />
        ))}
        <Callout x={1340} y={190} w={460} variant="plain" fontSize={25} style={{ opacity: progress(frame, cue(1), 14) }}>
          Sinh ra cho <b>file lớn</b>: ghi một lần, đọc nhiều lần.
        </Callout>
      </div>

      {/* Câu 2 trở đi: thư viện */}
      <div style={{ opacity: libOp }}>
        <Node x={NN.x} y={NN.y} w={300} kind="namenode" label={nnLabel} sub={real ? ":9000 · UI :9870" : "thẻ mục lục"} color={C} scale={pop(frame, cue(2), fps)} status={ask > 0 && answer < 1 ? "active" : "normal"} />
        {DN_X.map((x, i) => (
          <Node
            key={i}
            x={x}
            y={DN_Y}
            kind="datanode"
            label={dnLabel(i)}
            sub={real ? `:${DN_PORTS[i]}` : "kệ sách"}
            color={C}
            scale={pop(frame, cue(2) + 12 + i * 5, fps)}
          />
        ))}
        <Node x={CLIENT.x} y={CLIENT.y} w={220} kind="client" label={clientLabel} sub={real ? "hdfs dfs, Spark" : "người đọc"} color={ui.textDim} scale={pop(frame, cue(3) - 10, fps)} />

        <Arrow from={{ x: 365, y: 230 }} to={{ x: 805, y: 230 }} progress={ask} color={C} label="Cuốn X ở đâu?" />
        <Arrow from={{ x: 805, y: 280 }} to={{ x: 365, y: 280 }} progress={answer} color={state.ok} label="Tập 1: kệ A · Tập 2: kệ C" labelOffset={44} />
        <Packet path={[{ x: CLIENT.x, y: 330 }, shelfA]} progress={walkA} color={ui.text} shape="dot" size={26} />
        <Packet path={[shelfA, shelfC]} curve={-80} progress={walkC} color={ui.text} shape="dot" size={26} />
        {/* Thủ thư không bê sách: đường quầy → kệ bị gạch */}
        {frame > cue(3) + 130 ? (
          <div style={{ opacity: windowOpacity(frame, cue(3) + 130, cue(5) - 10) }}>
            <Arrow from={{ x: NN.x, y: NN.y + 80 }} to={{ x: DN_X[1] + 40, y: DN_Y - 80 }} dashed color={state.error} width={3} />
            <div style={{ position: "absolute", left: 870, top: 400, fontSize: 52, fontWeight: 800, color: state.error }}>✗</div>
            <div style={{ position: "absolute", left: 930, top: 410, fontSize: 24, fontWeight: 600, color: state.error, width: 360 }}>
              thủ thư không chạm vào sách
            </div>
          </div>
        ) : null}

        {/* Câu 5: đây là một máy WSL2 */}
        {real ? (
          <>
            <div
              style={{
                position: "absolute",
                left: 170,
                top: 470,
                width: 1580,
                height: 290,
                borderRadius: 22,
                border: `3px dashed ${alpha(ui.textDim, 0.7)}`,
                opacity: progress(frame, within(5, 0.3), 15),
              }}
            />
            <div
              style={{
                position: "absolute",
                left: 190,
                top: 774,
                fontSize: 24,
                fontWeight: 600,
                color: ui.textDim,
                opacity: progress(frame, within(5, 0.3), 15),
              }}
            >
              4 DataNode trên một máy WSL2 Ubuntu, khác cổng
            </div>
            <Callout x={1300} y={150} w={500} variant="project" style={{ opacity: progress(frame, cue(5), 15) }}>
              <div style={{ fontFamily: fonts.mono, fontSize: 25 }}>hdfs://localhost:9000</div>
              <div style={{ fontSize: 24, color: ui.textDim }}>Giao diện NameNode: cổng 9870</div>
            </Callout>
          </>
        ) : null}
      </div>
    </SceneShell>
  );
};
