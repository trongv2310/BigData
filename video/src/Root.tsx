import React from "react";
import { Composition } from "remotion";
import { manifest as m02, Video02 } from "./videos/v02/Video02";
import "./theme/fonts";

export const RemotionRoot: React.FC = () => (
  <>
    <Composition
      id="Video02Hdfs"
      component={Video02}
      durationInFrames={m02.totalFrames}
      fps={m02.fps}
      width={m02.width}
      height={m02.height}
    />
  </>
);
