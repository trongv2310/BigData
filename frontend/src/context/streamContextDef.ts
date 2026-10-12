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
  latestKafkaStats: KafkaStatsEvent['data'] | null;
  latestSparkProgress: SparkProgressEvent['data'] | null;
  recentHdfsOps: HdfsOpEvent['data'][];
  serviceOverrides: Record<string, ServiceStateEvent['data']>;
  flowStepsByRunId: Record<string, FlowStepEvent['data'][]>;
  latestFlowRunId: string | null;
  clearEvents: () => void;
}

export const StreamContext = createContext<StreamContextType>({
  status: 'disconnected',
  events: [],
  latestReading: null,
  latestKafkaStats: null,
  latestSparkProgress: null,
  recentHdfsOps: [],
  serviceOverrides: {},
  flowStepsByRunId: {},
  latestFlowRunId: null,
  clearEvents: () => {},
});
