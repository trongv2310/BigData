import type {
  ServiceList,
  HdfsSummary,
  HdfsDataNodes,
  HdfsListing,
  HdfsFileBlocks,
  KafkaTopics,
  SparkApplications,
  AqiReading,
  AqiSeries,
  HourlyProfile,
} from '../types';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8010';

async function fetchJson<T>(path: string, options?: RequestInit): Promise<T> {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!res.ok) {
    let errorDetail = `Lỗi HTTP ${res.status}`;
    try {
      const errJson = await res.json();
      if (errJson?.error?.message) {
        errorDetail = errJson.error.message;
      }
    } catch {
      // Bỏ qua lỗi parse
    }
    throw new Error(errorDetail);
  }

  return (await res.json()) as T;
}

export const api = {
  // P1 & P2: Dịch vụ và Hạ tầng
  getServices: () => fetchJson<ServiceList>('/api/services'),
  getHdfsSummary: () => fetchJson<HdfsSummary>('/api/hdfs/summary'),
  getHdfsDataNodes: () => fetchJson<HdfsDataNodes>('/api/hdfs/datanodes'),
  getHdfsFiles: (path: string = '/') =>
    fetchJson<HdfsListing>(`/api/hdfs/files?path=${encodeURIComponent(path)}`),
  getHdfsBlocks: (path: string) =>
    fetchJson<HdfsFileBlocks>(`/api/hdfs/blocks?path=${encodeURIComponent(path)}`),
  getKafkaTopics: () => fetchJson<KafkaTopics>('/api/kafka/topics'),
  getSparkApplications: () => fetchJson<SparkApplications>('/api/spark/applications'),

  // P3: Dữ liệu CO & AQI
  getAqiLatest: () => fetchJson<AqiReading>('/api/aqi/latest'),
  getAqiSeries: (params?: {
    metric?: string;
    granularity?: 'hour' | 'day' | 'month';
    from?: string;
    to?: string;
  }) => {
    const q = new URLSearchParams();
    if (params?.metric) q.set('metric', params.metric);
    if (params?.granularity) q.set('granularity', params.granularity);
    if (params?.from) q.set('from', params.from);
    if (params?.to) q.set('to', params.to);
    const queryString = q.toString() ? `?${q.toString()}` : '';
    return fetchJson<AqiSeries>(`/api/aqi/series${queryString}`);
  },
  getAqiHourlyProfile: () => fetchJson<HourlyProfile>('/api/aqi/hourly-profile'),
};
