import { createContext } from 'react';
import type {
  StreamEvent,
  AqiReading,
  KafkaStatsEvent,
  SparkProgressEvent,
  HdfsOpEvent,
  ServiceStateEvent,
  FlowStepEvent,
} from '../types';
import type { ConnectionStatus } from '../services/stream';

export interface StreamContextType {
  status: ConnectionStatus;
  events: StreamEvent[];
  latestReading: AqiReading | null;
  hasStreamReading: boolean; // true khi đã nhận ít nhất 1 sự kiện reading từ luồng thời gian thực
  latestReplayReading: AqiReading | null;
  latestLiveReading: AqiReading | null;
  replayHistory: AqiReading[];
  liveHistory: AqiReading[];
  hasReplayReading: boolean;
  hasLiveReading: boolean;
  latestKafkaStats: KafkaStatsEvent['data'] | null;
  latestSparkProgress: SparkProgressEvent['data'] | null;
  recentHdfsOps: HdfsOpEvent['data'][];
  serviceOverrides: Record<string, ServiceStateEvent['data']>;
  flowStepsByFlow: Record<string, Record<string, FlowStepEvent['data'][]>>; // [flowId][runId] -> list steps
  latestRunIdByFlow: Record<string, string>; // [flowId] -> latest runId
  flowStepsByRunId: Record<string, FlowStepEvent['data'][]>; // Tương thích ngược
  latestFlowRunId: string | null;
  clearEvents: () => void;
}

export const StreamContext = createContext<StreamContextType>({
  status: 'disconnected',
  events: [],
  latestReading: null,
  hasStreamReading: false,
  latestReplayReading: null,
  latestLiveReading: null,
  replayHistory: [],
  liveHistory: [],
  hasReplayReading: false,
  hasLiveReading: false,
  latestKafkaStats: null,
  latestSparkProgress: null,
  recentHdfsOps: [],
  serviceOverrides: {},
  flowStepsByFlow: {},
  latestRunIdByFlow: {},
  flowStepsByRunId: {},
  latestFlowRunId: null,
  clearEvents: () => {},
});
