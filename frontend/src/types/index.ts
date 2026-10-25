import type { components, paths } from './api';

export type Schemas = components['schemas'];
export type ServiceStatus = Schemas['ServiceStatus'];
export type ServiceList = Schemas['ServiceList'];
export type HdfsSummary = Schemas['HdfsSummary'];
export type DataNodeInfo = Schemas['DataNodeInfo'];
export type HdfsDataNodes = Schemas['HdfsDataNodes'];
export type HdfsEntry = Schemas['HdfsEntry'];
export type HdfsListing = Schemas['HdfsListing'];
export type HdfsBlock = Schemas['HdfsBlock'];
export type HdfsFileBlocks = Schemas['HdfsFileBlocks'];
export type KafkaTopic = Schemas['KafkaTopic'];
export type KafkaTopics = Schemas['KafkaTopics'];
export type SparkApplication = Schemas['SparkApplication'];
export type SparkApplications = Schemas['SparkApplications'];
export type CoStatus = Schemas['CoStatus'];
export type ReadingSource = Schemas['ReadingSource'];
export type AqiReading = Schemas['AqiReading'];
export type AqiSeries = Schemas['AqiSeries'];
export type HourlyProfile = Schemas['HourlyProfile'];
export type StreamEvent = Schemas['StreamEvent'];
export type ReadingEvent = Schemas['ReadingEvent'];
export type HdfsOpEvent = Schemas['HdfsOpEvent'];
export type FlowStepEvent = Schemas['FlowStepEvent'];
export type SparkProgressEvent = Schemas['SparkProgressEvent'];
export type KafkaStatsEvent = Schemas['KafkaStatsEvent'];
export type ServiceStateEvent = Schemas['ServiceStateEvent'];

// P4: Sơ đồ luồng
export type FlowSummary = Schemas['FlowSummary'];
export type FlowList = Schemas['FlowList'];
export type FlowDefinition = Schemas['FlowDefinition'];

// P5: SQL, Predict
export type SqlRequest = Schemas['SqlRequest'];
export type SqlResponse = Schemas['SqlResponse'];
export type PredictRequest = Schemas['PredictRequest'];
export type PredictResponse = Schemas['PredictResponse'];

// F1 & F2: Replay, Pipeline, Model Metrics
export type ReplayControlRequest = Schemas['ReplayControlRequest'];
export type ReplayStatus = Schemas['ReplayStatus'];
export type PipelineStepState = Schemas['PipelineStepState'];
export type PipelineStep = Schemas['PipelineStep'];
export type PipelineSteps = Schemas['PipelineSteps'];
export type ErrorMetrics = Schemas['ErrorMetrics'];
export type DataSplitInfo = Schemas['DataSplitInfo'];
export type ModelMetrics = Schemas['ModelMetrics'];

// P19: Phân tích SQL và Phân cụm ngày
export type SqlAnalysis = Schemas['SqlAnalysis'];
export type DayClusters = Schemas['DayClusters'];
export type DayClusterInfo = Schemas['DayClusterInfo'];
export type DayClusterStability = Schemas['DayClusterStability'];

export type { paths, components };
