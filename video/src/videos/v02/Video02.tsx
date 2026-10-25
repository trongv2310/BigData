import React from "react";
import { AbsoluteFill, Html5Audio, Sequence, staticFile } from "remotion";
import { Manifest, SceneTimingProvider, Subtitles } from "../../components";
import { ui } from "../../theme/palette";
import manifestJson from "./manifest.json";
import { Scene1Library } from "./scenes/Scene1Library";
import { Scene2Blocks } from "./scenes/Scene2Blocks";
import { Scene3NameNode } from "./scenes/Scene3NameNode";
import { Scene4Replication } from "./scenes/Scene4Replication";
import { Scene5Heartbeat } from "./scenes/Scene5Heartbeat";
import { Scene6Write } from "./scenes/Scene6Write";
import { Scene7Read } from "./scenes/Scene7Read";
import { Scene8Summary } from "./scenes/Scene8Summary";

export const manifest = manifestJson as Manifest;

const scenes: Record<string, React.FC> = {
  s1: Scene1Library,
  s2: Scene2Blocks,
  s3: Scene3NameNode,
  s4: Scene4Replication,
  s5: Scene5Heartbeat,
  s6: Scene6Write,
  s7: Scene7Read,
  s8: Scene8Summary,
};

/** Video 02 — HDFS. Thời lượng từng cảnh lấy từ manifest do tools/tts.py sinh. */
export const Video02: React.FC = () => (
  <AbsoluteFill style={{ background: ui.bg }}>
    {manifest.scenes.map((s) => {
      const Scene = scenes[s.id];
      return (
        <Sequence key={s.id} from={s.from} durationInFrames={s.durationInFrames} name={s.title}>
          <SceneTimingProvider value={s}>
            <Scene />
          </SceneTimingProvider>
          <Html5Audio src={staticFile(s.audio)} />
        </Sequence>
      );
    })}
    <Subtitles cues={manifest.cues} />
  </AbsoluteFill>
);
