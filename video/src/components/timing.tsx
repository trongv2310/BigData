import React, { createContext, useContext } from "react";

export type Sentence = { start: number; end: number; text: string };

export type SceneTiming = {
  id: string;
  title: string;
  from: number;
  durationInFrames: number;
  audio: string;
  /** Các câu của lời đọc, khung hình tính từ đầu cảnh. */
  sentences: Sentence[];
};

export type Manifest = {
  video: string;
  fps: number;
  width: number;
  height: number;
  totalFrames: number;
  scenes: SceneTiming[];
  /** Phụ đề đã chia dòng, khung hình tính từ đầu video. */
  cues: Sentence[];
};

const Ctx = createContext<SceneTiming | null>(null);

export const SceneTimingProvider: React.FC<{ value: SceneTiming; children: React.ReactNode }> = ({
  value,
  children,
}) => <Ctx.Provider value={value}>{children}</Ctx.Provider>;

/**
 * Mốc thời gian của lời đọc trong cảnh hiện tại, để hoạt hình bám theo câu nói.
 * cue(i) là khung hình bắt đầu câu i, cueEnd(i) là khung hình câu i đọc xong.
 */
export const useCues = () => {
  const t = useContext(Ctx);
  if (!t) throw new Error("useCues phải nằm trong SceneTimingProvider");
  const s = t.sentences;
  const at = (i: number) => s[Math.min(Math.max(i, 0), s.length - 1)];
  return {
    cue: (i: number) => at(i).start,
    cueEnd: (i: number) => at(i).end,
    /** Khung hình nằm giữa câu i, cộng thêm phần trăm k (0..1) của độ dài câu. */
    within: (i: number, k: number) => Math.round(at(i).start + (at(i).end - at(i).start) * k),
    duration: t.durationInFrames,
  };
};
