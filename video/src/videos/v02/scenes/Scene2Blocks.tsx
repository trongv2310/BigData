import React from "react";
import { Easing, useCurrentFrame, useVideoConfig } from "remotion";
import { Block, Callout, Counter, Node, SceneShell, Terminal, useCues } from "../../../components";
import { mix, pop, progress } from "../../../theme/anim";
import { fonts } from "../../../theme/fonts";
import { alpha, component, state, ui } from "../../../theme/palette";
import { DN_PORTS, DN_X, windowOpacity } from "../layout";

const C = component.hdfs;
const MIB = 1_048_576;
const FILE_BYTES = 3_179_004;
const LAST_BYTES = FILE_BYTES - 3 * MIB; // 33 276

// Phần A: file minh họa 300 MiB, 1 200 px.
const A_X = 200;
const A_Y = 190;
const A_PX = 1200 / 300;
const A_SIZES = [128, 128, 44];

// Phần B: file thật, thanh dài 1 400 px.
const B_X = 260;
const B_Y = 210;
const B_W = 1400;
const DN_ROW_Y = 690;

export const Scene2Blocks: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const { cue, within, cueEnd } = useCues();

  // ---------- Phần A (câu 0–4) ----------
  const aOp = windowOpacity(frame, 0, cue(5) - 12);
  const cut = progress(frame, within(0, 0.55), 22);
  const gap = cut * 24;
  const showSizes = frame >= cue(1);
  const ghostOp = progress(frame, within(1, 0.5), 15);

  let ax = A_X;
  const aBlocks = A_SIZES.map((mb) => {
    const b = { x: ax, w: mb * A_PX, mb };
    ax += mb * A_PX + gap;
    return b;
  });

  // ---------- Phần B (câu 5–8) ----------
  const bOp = windowOpacity(frame, cue(5));
  // Câu 5: block 128 MiB, file chỉ là một vệt nhỏ. Câu 6: đổi thước sang 1 MiB.
  const zoom = progress(frame, within(6, 0.35), 40, Easing.inOut(Easing.quad));
  const pxPerByte = mix(B_W / (128 * MIB), B_W / (FILE_BYTES / MIB) / MIB, zoom);
  const fileW = FILE_BYTES * pxPerByte;
  const rulerW = (zoom < 0.5 ? 128 * MIB : MIB) * pxPerByte;
  const rulerLabel = zoom < 0.5 ? "block 128 MiB" : "block 1 MiB";
  // Câu 7: cắt thành 4 block, rồi từng block bay vào một DataNode.
  const bCut = progress(frame, cue(7), 20);
  const bGap = bCut * 18;
  const flyStart = within(7, 0.55);
  const sizes = [MIB, MIB, MIB, LAST_BYTES];
  const fsckAt = cue(8) + 10;

  return (
    <SceneShell index="CẢNH 2" title="Block: cắt file thành viên gạch" color={C}>
      {/* ===== Phần A ===== */}
      <div style={{ opacity: aOp }}>
        <div style={{ position: "absolute", left: A_X, top: A_Y - 46, fontSize: 28, fontWeight: 700 }}>
          Một file lớn <span style={{ color: ui.textDim, fontWeight: 500 }}>(minh họa: 300 MiB)</span>
        </div>
        {aBlocks.map((b, i) => (
          <Block
            key={i}
            x={b.x}
            y={A_Y}
            w={b.w}
            h={92}
            color={C}
            size={showSizes ? `${b.mb} MiB` : undefined}
            ghostWidth={i === 2 ? 128 * A_PX : undefined}
            ghostOpacity={ghostOp}
            opacity={progress(frame, 6, 14)}
          />
        ))}
        {/* Vết cắt */}
        {[0, 1].map((i) => (
          <div
            key={i}
            style={{
              position: "absolute",
              left: A_X + (i + 1) * 128 * A_PX + i * gap - 2 + gap / 2,
              top: A_Y - 22,
              width: 4,
              height: 136,
              background: state.running,
              opacity: frame < within(0, 0.45) ? 0 : 1 - cut * 0.7,
            }}
          />
        ))}
        {showSizes ? (
          <div
            style={{
              position: "absolute",
              left: A_X + 2 * 128 * A_PX + 2 * gap + 44 * A_PX + 30,
              top: A_Y + 110,
              fontSize: 23,
              color: ui.textDim,
              opacity: ghostOp,
              width: 330,
            }}
          >
            block cuối chỉ chiếm 44 MiB, không chiếm đủ 128 MiB
          </div>
        ) : null}
        <div
          style={{
            position: "absolute",
            left: A_X,
            top: A_Y + 128,
            fontSize: 30,
            fontWeight: 600,
            opacity: progress(frame, cue(3), 15),
          }}
        >
          300 ÷ 128 ≈ 2,34 → làm tròn lên: <span style={{ color: C }}>3 block</span> = 128 + 128 + 44 MiB
        </div>

        {/* Câu 2: cuốn từ điển tách thành tập */}
        <Callout x={A_X} y={420} w={700} variant="everyday" style={{ opacity: windowOpacity(frame, cue(2), cue(4) + 40) }}>
          Cuốn từ điển quá dày được tách thành nhiều tập để cất ở nhiều kệ.
          <div style={{ position: "relative", height: 110, marginTop: 10 }}>
            {[0, 1, 2].map((i) => {
              const sp = progress(frame, within(2, 0.4), 24);
              return (
                <div
                  key={i}
                  style={{
                    position: "absolute",
                    left: 40 + i * mix(70, 180, sp),
                    top: 4,
                    width: 64,
                    height: 96,
                    borderRadius: 6,
                    background: alpha("#FBBF24", 0.25 + i * 0.1),
                    border: "3px solid #FBBF24",
                    fontSize: 20,
                    fontWeight: 700,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                  }}
                >
                  {sp > 0.5 ? `Tập ${i + 1}` : ""}
                </div>
              );
            })}
          </div>
        </Callout>

        {/* Câu 4: NameNode nhớ mọi block trong RAM */}
        <div style={{ opacity: windowOpacity(frame, cue(4), cue(5) - 12) }}>
          <Callout x={980} y={420} w={760} variant="plain" title="Vì sao block to?" color={C}>
            NameNode giữ thông tin <b>mọi block trong RAM</b>.
            <div style={{ display: "flex", gap: 30, marginTop: 14, alignItems: "flex-end" }}>
              <div style={{ textAlign: "center" }}>
                <div style={{ display: "flex", flexDirection: "column", gap: 6, height: 120, justifyContent: "flex-end" }}>
                  {[0, 1, 2].map((i) => (
                    <div key={i} style={{ width: 200, height: 22, borderRadius: 4, background: alpha(C, 0.8), opacity: progress(frame, within(4, 0.5) + i * 4, 8) }} />
                  ))}
                </div>
                <div style={{ fontSize: 22, marginTop: 8 }}>block to: ít dòng sổ</div>
              </div>
              <div style={{ textAlign: "center" }}>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 3, width: 260, height: 120, alignContent: "flex-end" }}>
                  {Array.from({ length: 96 }).map((_, i) => (
                    <div key={i} style={{ width: 18, height: 8, borderRadius: 2, background: alpha(state.error, 0.75), opacity: progress(frame, within(4, 0.5) + i * 0.4, 4) }} />
                  ))}
                </div>
                <div style={{ fontSize: 22, marginTop: 8 }}>block nhỏ: RAM đầy dòng sổ</div>
              </div>
            </div>
          </Callout>
        </div>
      </div>

      {/* ===== Phần B ===== */}
      <div style={{ opacity: bOp }}>
        <div style={{ position: "absolute", left: B_X, top: B_Y - 56, fontSize: 28, fontWeight: 700, opacity: 1 - progress(frame, cue(8) - 6, 10) }}>
          <span style={{ fontFamily: fonts.mono, color: C }}>/raw/Dataset.csv</span>{" "}
          <Counter to={FILE_BYTES} progress={progress(frame, cue(5), 30)} fontSize={28} suffix=" byte" />
        </div>
        {/* Thước block */}
        {frame < cue(7) + 10 ? (
          <div
            style={{
              position: "absolute",
              left: B_X,
              top: B_Y,
              width: rulerW,
              height: 92,
              borderRadius: 10,
              border: `3px dashed ${alpha(C, 0.75)}`,
              boxSizing: "border-box",
              opacity: 1 - progress(frame, cue(7), 10),
            }}
          >
            <div style={{ position: "absolute", left: 12, top: -2, fontSize: 22, color: C, fontWeight: 600, whiteSpace: "nowrap", transform: "translateY(100px)" }}>
              {rulerLabel}
            </div>
          </div>
        ) : null}
        {/* File trước khi cắt */}
        {frame < cue(7) ? (
          <div
            style={{
              position: "absolute",
              left: B_X,
              top: B_Y + 8,
              width: Math.max(4, fileW),
              height: 76,
              borderRadius: 8,
              background: `linear-gradient(180deg, ${alpha(C, 0.95)}, ${alpha(C, 0.7)})`,
            }}
          />
        ) : null}
        {frame < within(6, 0.35) ? (
          <div style={{ position: "absolute", left: B_X + 50, top: B_Y + 18, fontSize: 25, opacity: progress(frame, within(5, 0.4), 12) }}>
            ← cả file chỉ là một vệt nhỏ trong <b>1 block</b> 128 MiB
          </div>
        ) : null}

        {/* Bốn block sau khi cắt, rồi bay xuống DataNode */}
        {frame >= cue(7)
          ? sizes.map((bytes, i) => {
              const w0 = bytes * pxPerByte;
              const x0 = B_X + i * (MIB * pxPerByte + bGap);
              const fly = progress(frame, flyStart + i * 14, 34, Easing.inOut(Easing.cubic));
              const inDnW = i === 3 ? 30 : 150;
              const x1 = DN_X[i] - 95;
              const y1 = DN_ROW_Y + 18;
              const x = mix(x0, x1, fly);
              const y = mix(B_Y, y1, fly);
              const w = mix(Math.max(w0, 6), inDnW, fly);
              const h = mix(92, 44, fly);
              const lit = frame >= fsckAt + 30 + i * 12 && frame < fsckAt + 30 + i * 12 + 40 ? 1 : 0;
              return (
                <React.Fragment key={i}>
                  <Block
                    x={x}
                    y={y}
                    w={w}
                    h={h}
                    color={C}
                    id={fly > 0.8 ? `B${i + 1}` : undefined}
                    compact={fly > 0.8}
                    size={fly < 0.2 && i < 3 ? "1 048 576 byte" : undefined}
                    ghostWidth={i === 3 && fly > 0.9 ? 150 : undefined}
                    highlight={lit}
                  />
                  {i === 3 && fly < 0.2 ? (
                    <div style={{ position: "absolute", left: x0 - 330, width: 340, textAlign: "right", top: B_Y - 40, fontSize: 22, fontWeight: 600, color: state.running, whiteSpace: "nowrap" }}>
                      block cuối 33 276 byte ↓
                    </div>
                  ) : null}
                </React.Fragment>
              );
            })
          : null}
        {frame >= cue(7) ? (
          <div style={{ position: "absolute", left: B_X, top: B_Y + 116, fontSize: 30, fontWeight: 600, opacity: windowOpacity(frame, cue(7) + 15, fsckAt - 12) }}>
            3 × 1 048 576 + 33 276 = <span style={{ color: C }}>3 179 004 byte</span> → <span style={{ color: C }}>4 block</span>
          </div>
        ) : null}

        {/* DataNode nhận block */}
        <div style={{ opacity: windowOpacity(frame, within(7, 0.35)) }}>
          {DN_X.map((x, i) => (
            <Node key={i} x={x} y={DN_ROW_Y} w={240} h={150} kind="datanode" label="DataNode" sub={`:${DN_PORTS[i]}`} color={C} scale={pop(frame, within(7, 0.35) + i * 4, fps)} />
          ))}
          <Callout x={1400} y={790} w={470} variant="illustration" fontSize={20} style={{ opacity: progress(frame, cueEnd(7) - 30, 12), padding: "8px 14px 10px" }}>
            Block nào nằm ở DataNode nào: chỉ để minh họa
          </Callout>
        </div>

        {/* Câu 8: hdfs fsck */}
        <Terminal
          x={B_X}
          y={150}
          w={1400}
          frame={frame}
          opacity={progress(frame, cue(8) - 6, 10)}
          title="WSL2 Ubuntu · trích, rút gọn"
          lines={[
            { text: "hdfs fsck /raw/Dataset.csv -files -blocks -locations", at: fsckAt - 50, typed: true },
            { text: "/raw/Dataset.csv 3179004 bytes, 4 block(s)", at: fsckAt + 20 },
            { text: "0. blk_1073741827   len=1048576", at: fsckAt + 30, highlight: frame >= fsckAt + 30 && frame < fsckAt + 70 },
            { text: "1. blk_…            len=1048576", at: fsckAt + 42, highlight: frame >= fsckAt + 42 && frame < fsckAt + 82 },
            { text: "2. blk_…            len=1048576", at: fsckAt + 54, highlight: frame >= fsckAt + 54 && frame < fsckAt + 94 },
            { text: "3. blk_…            len=33276", at: fsckAt + 66, highlight: frame >= fsckAt + 66 && frame < fsckAt + 106 },
          ]}
        />
      </div>
    </SceneShell>
  );
};
