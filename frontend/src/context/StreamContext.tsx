import React, { createContext, useContext, useEffect, useState } from 'react';
import type {
  StreamEvent,
  AqiReading,
  KafkaStatsEvent,
  SparkProgressEvent,
  HdfsOpEvent,
  ServiceStateEvent,
} from '../types';
import { getStreamSource, type ConnectionStatus } from '../services/stream';

interface StreamContextType {
  status: ConnectionStatus;
  events: StreamEvent[];
  latestReading: AqiReading | null;
  latestKafkaStats: KafkaStatsEvent['data'] | null;
  latestSparkProgress: SparkProgressEvent['data'] | null;
  recentHdfsOps: HdfsOpEvent['data'][];
  serviceOverrides: Record<string, ServiceStateEvent['data']>;
  clearEvents: () => void;
}

const StreamContext = createContext<StreamContextType>({
  status: 'disconnected',
  events: [],
  latestReading: null,
  latestKafkaStats: null,
  latestSparkProgress: null,
  recentHdfsOps: [],
  serviceOverrides: {},
  clearEvents: () => {},
});

export const StreamProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [status, setStatus] = useState<ConnectionStatus>('disconnected');
  const [events, setEvents] = useState<StreamEvent[]>([]);
  const [latestReading, setLatestReading] = useState<AqiReading | null>(null);
  const [latestKafkaStats, setLatestKafkaStats] = useState<KafkaStatsEvent['data'] | null>(null);
  const [latestSparkProgress, setLatestSparkProgress] = useState<SparkProgressEvent['data'] | null>(null);
  const [recentHdfsOps, setRecentHdfsOps] = useState<HdfsOpEvent['data'][]>([]);
  const [serviceOverrides, setServiceOverrides] = useState<Record<string, ServiceStateEvent['data']>>({});

  useEffect(() => {
    const source = getStreamSource();
    source.start();

    const unsubStatus = source.onStatusChange((s) => {
      setStatus(s);
    });

    const unsubEvents = source.subscribe((event) => {
      // Giữ tối đa 50 sự kiện gần nhất cho ticker / log
      setEvents((prev) => [event, ...prev.slice(0, 49)]);

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
      }
    });

    return () => {
      unsubStatus();
      unsubEvents();
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
        clearEvents,
      }}
    >
      {children}
    </StreamContext.Provider>
  );
};

export const useStream = () => useContext(StreamContext);
