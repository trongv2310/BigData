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
  const [hasStreamReading, setHasStreamReading] = useState<boolean>(false);
  const [latestKafkaStats, setLatestKafkaStats] = useState<KafkaStatsEvent['data'] | null>(null);
  const [latestSparkProgress, setLatestSparkProgress] = useState<SparkProgressEvent['data'] | null>(null);
  const [recentHdfsOps, setRecentHdfsOps] = useState<HdfsOpEvent['data'][]>([]);
  const [serviceOverrides, setServiceOverrides] = useState<Record<string, ServiceStateEvent['data']>>({});
  const [flowStepsByFlow, setFlowStepsByFlow] = useState<Record<string, Record<string, FlowStepEvent['data'][]>>>({});
  const [latestRunIdByFlow, setLatestRunIdByFlow] = useState<Record<string, string>>({});
  const [flowStepsByRunId, setFlowStepsByRunId] = useState<Record<string, FlowStepEvent['data'][]>>({});
  const [latestFlowRunId, setLatestFlowRunId] = useState<string | null>(null);

  useEffect(() => {
    const source = getStreamSource();
    source.start();

    const unsubStatus = source.onStatusChange((s) => {
      setStatus(s);
    });

    const unsubEvents = source.subscribe((event) => {
      // Luu su kien moi vao nhat ky (giu toi da 50 su kien gan nhat)
      setEvents((prev) => [event, ...prev.slice(0, 49)]);

      if (event.type === 'reading') {
        setLatestReading(event.data);
        setHasStreamReading(true);
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
        const { flowId, runId, step } = event.data;

        // Lưu flow.step gom nhóm theo flowId và runId
        setFlowStepsByFlow((prev) => {
          const currentFlowRuns = prev[flowId] || {};
          const currentRunSteps = currentFlowRuns[runId] || [];
          if (currentRunSteps.some((s) => s.step === step)) {
            return prev;
          }
          const updatedRunSteps = [...currentRunSteps, event.data].sort((a, b) => a.step - b.step);
          const updatedFlowRuns = {
            ...currentFlowRuns,
            [runId]: updatedRunSteps,
          };
          // Giới hạn 20 run gần nhất cho mỗi flowId
          const keys = Object.keys(updatedFlowRuns);
          if (keys.length > 20) {
            const trimmed: Record<string, FlowStepEvent['data'][]> = {};
            for (const k of keys.slice(keys.length - 20)) {
              trimmed[k] = updatedFlowRuns[k];
            }
            return {
              ...prev,
              [flowId]: trimmed,
            };
          }
          return {
            ...prev,
            [flowId]: updatedFlowRuns,
          };
        });

        setLatestRunIdByFlow((prev) => ({
          ...prev,
          [flowId]: runId,
        }));

        // Đồng thời cập nhật flowStepsByRunId cho tương thích ngược
        setFlowStepsByRunId((prev) => {
          const currentList = prev[runId] || [];
          if (currentList.some((s) => s.step === step)) {
            return prev;
          }
          const updated = {
            ...prev,
            [runId]: [...currentList, event.data].sort((a, b) => a.step - b.step),
          };
          const keys = Object.keys(updated);
          if (keys.length > 20) {
            const trimmed: Record<string, FlowStepEvent['data'][]> = {};
            for (const k of keys.slice(keys.length - 20)) {
              trimmed[k] = updated[k];
            }
            return trimmed;
          }
          return updated;
        });
        setLatestFlowRunId(runId);
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
        hasStreamReading,
        latestKafkaStats,
        latestSparkProgress,
        recentHdfsOps,
        serviceOverrides,
        flowStepsByFlow,
        latestRunIdByFlow,
        flowStepsByRunId,
        latestFlowRunId,
        clearEvents,
      }}
    >
      {children}
    </StreamContext.Provider>
  );
};
