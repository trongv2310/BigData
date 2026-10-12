import React, { useEffect, useState } from 'react';
import type {
  StreamEvent,
  AqiReading,
  KafkaStatsEvent,
  SparkProgressEvent,
  HdfsOpEvent,
  ServiceStateEvent,
  FlowStepEvent,
} from '../types';
import { getStreamSource, type ConnectionStatus } from '../services/stream';
import { StreamContext } from './streamContextDef';

export const StreamProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [status, setStatus] = useState<ConnectionStatus>('disconnected');
  const [events, setEvents] = useState<StreamEvent[]>([]);
  const [latestReading, setLatestReading] = useState<AqiReading | null>(null);
  const [latestKafkaStats, setLatestKafkaStats] = useState<KafkaStatsEvent['data'] | null>(null);
  const [latestSparkProgress, setLatestSparkProgress] = useState<SparkProgressEvent['data'] | null>(null);
  const [recentHdfsOps, setRecentHdfsOps] = useState<HdfsOpEvent['data'][]>([]);
  const [serviceOverrides, setServiceOverrides] = useState<Record<string, ServiceStateEvent['data']>>({});
  const [flowStepsByRunId, setFlowStepsByRunId] = useState<Record<string, FlowStepEvent['data'][]>>({});
  const [latestFlowRunId, setLatestFlowRunId] = useState<string | null>(null);

  useEffect(() => {
    const source = getStreamSource();
    source.start();

    const unsubStatus = source.onStatusChange((s) => {
      setStatus(s);
    });

    const unsubEvents = source.subscribe((event) => {
      // Deduplicate theo seq để tránh sự kiện bị nhân đôi khi re-mount hoặc StrictMode
      setEvents((prev) => {
        if (prev.some((e) => e.seq === event.seq)) {
          return prev;
        }
        return [event, ...prev.slice(0, 49)];
      });

      if (event.type === 'reading') {
        setLatestReading(event.data);
      } else if (event.type === 'kafka.stats') {
        setLatestKafkaStats(event.data);
      } else if (event.type === 'spark.progress') {
        setLatestSparkProgress(event.data);
      } else if (event.type === 'hdfs.op') {
        setRecentHdfsOps((prev) => [event.data, ...prev.slice(0, 19)]);
      } else if (event.type === 'service.state') {
        setServiceOverrides((prev) => ({
          ...prev,
          [event.data.name]: event.data,
        }));
      } else if (event.type === 'flow.step') {
        setFlowStepsByRunId((prev) => {
          const runId = event.data.runId;
          const currentList = prev[runId] || [];
          if (currentList.some((s) => s.step === event.data.step)) {
            return prev;
          }
          return {
            ...prev,
            [runId]: [...currentList, event.data].sort((a, b) => a.step - b.step),
          };
        });
        setLatestFlowRunId(event.data.runId);
      }
    });

    return () => {
      unsubStatus();
      unsubEvents();
      // Ngắt nguồn khi cleanup để tránh giữ duplicate socket
      source.stop();
    };
  }, []);

  const clearEvents = () => setEvents([]);

  return (
    <StreamContext.Provider
      value={{
        status,
        events,
        latestReading,
        latestKafkaStats,
        latestSparkProgress,
        recentHdfsOps,
        serviceOverrides,
        flowStepsByRunId,
        latestFlowRunId,
        clearEvents,
      }}
    >
      {children}
    </StreamContext.Provider>
  );
};
