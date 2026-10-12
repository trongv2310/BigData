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
  FlowList,
  FlowDefinition,
  SqlRequest,
  SqlResponse,
  PredictRequest,
  PredictResponse,
} from '../types';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8010';

export class ApiError extends Error {
  code?: string;
  status: number;

  constructor(message: string, status: number, code?: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
  }
}

async function fetchJson<T>(
  path: string,
  options?: RequestInit,
  timeoutMs: number = 15000
): Promise<T> {
  const url = `${API_BASE}${path}`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
    });

    if (!res.ok) {
      let errorDetail = `Lỗi HTTP ${res.status}`;
      let errorCode: string | undefined = undefined;
      try {
        const errJson = await res.json();
        if (errJson?.error) {
          if (errJson.error.message) errorDetail = errJson.error.message;
          if (errJson.error.code) errorCode = errJson.error.code;
        }
      } catch {
        // Bỏ qua lỗi parse JSON body
      }
      throw new ApiError(errorDetail, res.status, errorCode);
    }

    return (await res.json()) as T;
  } catch (err: unknown) {
    if (err instanceof ApiError) {
      throw err;
    }
    if (err instanceof Error && err.name === 'AbortError') {
      throw new ApiError(
        `Quá thời gian chờ phản hồi từ máy chủ (${Math.round(timeoutMs / 1000)} giây).`,
        504,
        'TIMEOUT'
      );
    }
    const msg = err instanceof Error ? err.message : String(err);
    throw new ApiError(msg, 0);
  } finally {
    clearTimeout(timeoutId);
  }
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

  // P4: Sơ đồ luồng
  listFlows: () => fetchJson<FlowList>('/api/flows'),
  getFlow: (id: string) => fetchJson<FlowDefinition>(`/api/flows/${encodeURIComponent(id)}`),

  // P5: SparkSQL & Dự đoán Heuristic
  runSql: (req: SqlRequest) =>
    fetchJson<SqlResponse>(
      '/api/sql',
      {
        method: 'POST',
        body: JSON.stringify(req),
      },
      65000 // Khởi động Spark lần đầu có thể mất 8-10 giây, timeout >= 60 giây
    ),
  predictCo: (req: PredictRequest) =>
    fetchJson<PredictResponse>('/api/predict', {
      method: 'POST',
      body: JSON.stringify(req),
    }),
};
