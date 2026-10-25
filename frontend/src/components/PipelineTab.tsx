import React, { useEffect, useState, useCallback } from 'react';
import {
  Layers,
  CheckCircle2,
  Clock,
  XCircle,
  AlertTriangle,
  RefreshCw,
  GitMerge,
  BarChart3,
  Cpu,
  Info,
  ChevronRight,
  TrendingDown,
  Calendar,
  Database,
  Sparkles,
} from 'lucide-react';
import { api, isMockMode, onMockChange, ApiError } from '../services/api';
import type {
  PipelineSteps,
  PipelineStep,
  PipelineStepState,
  ModelMetrics,
} from '../types';
import { formatDateTime } from '../utils/format';

export const PipelineTab: React.FC = () => {
  const [pipelineData, setPipelineData] = useState<PipelineSteps | null>(null);
  const [modelMetrics, setModelMetrics] = useState<ModelMetrics | null>(null);
  const [modelUntrained, setModelUntrained] = useState<boolean>(false);
  const [modelErrorMsg, setModelErrorMsg] = useState<string | null>(null);
  const [pipelineUnavailableMsg, setPipelineUnavailableMsg] = useState<string | null>(null);

  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedStepId, setSelectedStepId] = useState<string>('prepare-hdfs');

  // Variant test cho mock
  const [mockVariant, setMockVariant] = useState<string>('');

  const MOCK_VARIANTS = [
    { id: '', label: 'Mặc định (Có dữ liệu)' },
    { id: 'unavailable', label: '503 Unavailable' },
    { id: 'untrained', label: '404 Chưa huấn luyện' },
    { id: 'notready', label: '404 Chưa sẵn sàng' },
    { id: 'nostability', label: 'Không có Stability' },
  ];

  // Phát hiện mock server
  const [isMock, setIsMock] = useState<boolean>(() => isMockMode());
  useEffect(() => {
    return onMockChange(setIsMock);
  }, []);

  const loadData = useCallback(async (variant?: string) => {
    try {
      setLoading(true);
      setError(null);
      setModelErrorMsg(null);
      setPipelineUnavailableMsg(null);

      // 1. Tải 9 bước pipeline (hỗ trợ variant mock)
      const stepsPromise = api.getPipelineSteps(variant || undefined);

      // 2. Tải chỉ số mô hình
      const metricsPromise = api.getModelMetrics(variant || undefined);

      const [stepsRes, metricsRes] = await Promise.allSettled([
        stepsPromise,
        metricsPromise,
      ]);

      if (stepsRes.status === 'fulfilled') {
        setPipelineData(stepsRes.value);
        setPipelineUnavailableMsg(null);
        // Mặc định chọn bước đang chạy hoặc bước đầu tiên
        const runningStep = stepsRes.value.steps.find((s) => s.state === 'running');
        if (runningStep) {
          setSelectedStepId(runningStep.id);
        }
      } else {
        const reason = stepsRes.reason;
        if (reason instanceof ApiError && (reason.code === 'PIPELINE_UNAVAILABLE' || reason.status === 503)) {
          setPipelineData(null);
          setPipelineUnavailableMsg(reason.message);
        } else {
          const msg = reason instanceof Error ? reason.message : String(reason);
          setError(`Không thể tải 9 bước pipeline: ${msg}`);
          setPipelineData(null);
        }
      }

      if (metricsRes.status === 'fulfilled') {
        setModelMetrics(metricsRes.value);
        setModelUntrained(false);
      } else {
        const reason = metricsRes.reason;
        if (reason instanceof ApiError && reason.status === 404) {
          setModelMetrics(null);
          setModelUntrained(true);
          setModelErrorMsg(reason.message);
        } else {
          const msg = reason instanceof Error ? reason.message : String(reason);
          setModelMetrics(null);
          setModelUntrained(false);
          setModelErrorMsg(msg);
        }
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData(mockVariant);
  }, [loadData, mockVariant]);

  const handleVariantToggle = () => {
    const next = mockVariant === 'untrained' ? '' : 'untrained';
    setMockVariant(next);
  };

  const getStepStateBadge = (state: PipelineStepState) => {
    switch (state) {
      case 'done':
        return {
          label: 'Đã hoàn thành',
          badgeClass: 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/30',
          icon: CheckCircle2,
          iconColor: 'text-emerald-400',
        };
      case 'running':
        return {
          label: 'Đang chạy',
          badgeClass: 'bg-sky-500/10 text-sky-400 border border-sky-500/30 animate-pulse',
          icon: RefreshCw,
          iconColor: 'text-sky-400 animate-spin',
        };
      case 'partial':
        return {
          label: 'Mới xong một phần',
          badgeClass: 'bg-amber-500/10 text-amber-400 border border-amber-500/30',
          icon: AlertTriangle,
          iconColor: 'text-amber-400',
        };
      case 'failed':
        return {
          label: 'Thất bại',
          badgeClass: 'bg-rose-500/10 text-rose-400 border border-rose-500/30',
          icon: XCircle,
          iconColor: 'text-rose-400',
        };
      case 'notStarted':
      default:
        return {
          label: 'Chưa chạy',
          badgeClass: 'bg-slate-800 text-slate-400 border border-slate-700',
          icon: Clock,
          iconColor: 'text-slate-500',
        };
    }
  };

  const selectedStep: PipelineStep | undefined = pipelineData?.steps.find(
    (s) => s.id === selectedStepId
  ) || pipelineData?.steps[0];

  const completedStepsCount = pipelineData?.steps.filter((s) => s.state === 'done').length || 0;
  const runningStepsCount = pipelineData?.steps.filter((s) => s.state === 'running').length || 0;
  const partialStepsCount = pipelineData?.steps.filter((s) => s.state === 'partial').length || 0;
  const failedStepsCount = pipelineData?.steps.filter((s) => s.state === 'failed').length || 0;

  return (
    <div className="space-y-8 min-w-0 max-w-full">
      {/* Banner Tiêu đề & Điều khiển */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
            <GitMerge className="w-6 h-6 text-sky-400" />
            Quy trình Big Data 9 Bước & Chỉ số Mô hình
          </h2>
          <p className="text-sm text-slate-400 mt-1">
            Theo dõi tuần tự 9 bước xử lý dữ liệu và đánh giá thực nghiệm mô hình dự đoán khí CO
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2 self-start sm:self-auto">
          {isMock && (
            <div className="flex flex-wrap items-center gap-1.5 p-1 rounded-xl bg-slate-900 border border-slate-800">
              <span className="text-[11px] text-slate-400 px-2 font-mono">Mock:</span>
              {MOCK_VARIANTS.map((v) => (
                <button
                  key={v.id}
                  onClick={() => setMockVariant(v.id)}
                  className={`px-2.5 py-1 rounded-lg text-xs font-medium transition ${
                    mockVariant === v.id
                      ? 'bg-sky-500/20 text-sky-300 border border-sky-500/40 shadow-sm'
                      : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800 border border-transparent'
                  }`}
                  title={`Kiểm thử chế độ mock variant ?mock=${v.id || 'default'}`}
                >
                  {v.label}
                </button>
              ))}
            </div>
          )}

          <button
            onClick={() => loadData(mockVariant)}
            disabled={loading}
            className="inline-flex items-center gap-2 px-3.5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs sm:text-sm font-medium rounded-lg border border-slate-700 transition disabled:opacity-50"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            Tải lại
          </button>
        </div>
      </div>

      {/* Thông báo Mock server nếu có */}
      {isMock && (
        <div className="p-3.5 rounded-xl bg-indigo-500/10 border border-indigo-500/30 text-indigo-200 text-xs sm:text-sm flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Info className="w-4 h-4 text-indigo-400 shrink-0" />
            <span>
              <strong className="font-semibold text-indigo-300">[DỮ LIỆU MẪU MOCK]</strong> Các số liệu pipeline và chỉ số mô hình là kết quả minh họa phục vụ kiểm thử giao diện theo hợp đồng OpenAPI 0.2.0.
            </span>
          </div>
          <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-indigo-500/20 text-indigo-300 shrink-0">
            X-Mock: true
          </span>
        </div>
      )}

      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm flex items-start gap-3">
          <AlertTriangle className="w-5 h-5 shrink-0 text-rose-400 mt-0.5" />
          <div>
            <div className="font-semibold">Lỗi nạp dữ liệu</div>
            <div>{error}</div>
          </div>
        </div>
      )}

      {pipelineUnavailableMsg && (
        <div className="p-5 rounded-2xl bg-amber-500/10 border border-amber-500/30 text-amber-200 text-sm space-y-2">
          <div className="flex items-center gap-2 text-amber-300 font-bold">
            <AlertTriangle className="w-5 h-5 text-amber-400 shrink-0" />
            Dịch vụ Pipeline Chưa Khả Dụng (503 PIPELINE_UNAVAILABLE)
          </div>
          <p className="text-xs text-amber-200/90 leading-relaxed font-mono">
            {pipelineUnavailableMsg}
          </p>
          <div className="text-[11px] text-slate-400 pt-1.5 border-t border-amber-500/20 leading-relaxed">
            💡 <strong>Nguyên nhân thường gặp:</strong> Cụm HDFS chưa được khởi động (NameNode/DataNode đóng) hoặc tệp <code>/co/output/step_summary.json</code> chưa được các script pipeline sinh ra. Kiểm tra trạng thái NameNode/DataNode trên tab HDFS hoặc chạy <code>bash ~/start-4nodes.sh</code> trong WSL Ubuntu.
          </div>
        </div>
      )}

      {/* ========================================================================= */}
      {/* PHẦN 1: 9 BƯỚC CỦA PIPELINE                                              */}
      {/* ========================================================================= */}
      <section className="space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800 pb-3">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-sky-500/10 border border-sky-500/20 flex items-center justify-center text-sky-400">
              <Layers className="w-4 h-4" />
            </div>
            <div>
              <h3 className="text-base sm:text-lg font-bold text-white tracking-tight">
                Tiến độ Thực thi Pipeline 9 Bước
              </h3>
              <p className="text-xs text-slate-400 font-mono">
                GET /api/pipeline/steps • Cập nhật lúc:{' '}
                {pipelineData ? formatDateTime(pipelineData.generatedAt) : '—'}
              </p>
            </div>
          </div>

          {/* Thanh tổng quan trạng thái */}
          <div className="flex items-center gap-3 text-xs font-mono">
            <span className="text-emerald-400 font-semibold">
              Hoàn thành: {completedStepsCount}/9
            </span>
            {runningStepsCount > 0 && (
              <span className="text-sky-400 font-semibold animate-pulse">
                • Đang chạy: {runningStepsCount}
              </span>
            )}
            {partialStepsCount > 0 && (
              <span className="text-amber-400 font-semibold">
                • Mới xong một phần: {partialStepsCount}
              </span>
            )}
            {failedStepsCount > 0 && (
              <span className="text-rose-400 font-semibold">
                • Lỗi: {failedStepsCount}
              </span>
            )}
          </div>
        </div>

        {/* Lưới 9 bước pipeline (Có thể click để xem chi tiết) */}
        {!pipelineData && !loading ? (
          <div className="p-8 text-center bg-slate-900/60 rounded-xl border border-slate-800 text-slate-400">
            <Layers className="w-10 h-10 text-slate-600 mx-auto mb-3" />
            <p className="font-medium text-slate-300">Chưa có dữ liệu tiến độ pipeline 9 bước</p>
            <p className="text-xs text-slate-500 mt-1 max-w-md mx-auto">
              Backend chưa khởi tạo hoặc trả về mã 404 (GET /api/pipeline/steps). Bảng dữ liệu sẽ tự động hiển thị khi tiến trình pipeline hoàn tất.
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3.5">
            {pipelineData?.steps.map((step) => {
              const isSelected = step.id === selectedStepId;
              const stateInfo = getStepStateBadge(step.state);
              const StateIcon = stateInfo.icon;

              return (
                <div
                  key={step.id}
                  onClick={() => setSelectedStepId(step.id)}
                  className={`p-4 rounded-xl border transition cursor-pointer flex flex-col justify-between space-y-3 relative overflow-hidden ${
                    isSelected
                      ? 'bg-slate-800/90 border-sky-500 shadow-md shadow-sky-500/10 ring-1 ring-sky-500/30'
                      : 'bg-slate-900/80 border-slate-800 hover:border-slate-700 hover:bg-slate-850'
                  }`}
                >
                  {/* Header bước */}
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex items-center gap-2">
                      <span className="w-6 h-6 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center text-xs font-bold font-mono text-slate-300 shrink-0">
                        {step.order}
                      </span>
                      <h4 className="text-sm font-bold text-white tracking-tight leading-snug">
                        {step.title}
                      </h4>
                    </div>
                    <span
                      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-medium shrink-0 ${stateInfo.badgeClass}`}
                    >
                      <StateIcon className={`w-3 h-3 ${stateInfo.iconColor}`} />
                      <span>{stateInfo.label}</span>
                    </span>
                  </div>

                  {/* Số liệu chính (Summary) */}
                  <div className="space-y-1.5 pt-1">
                    {step.state === 'partial' && step.detail && (
                      <div className="text-[11px] text-amber-300 bg-amber-500/10 rounded-lg px-2.5 py-1.5 border border-amber-500/20 leading-relaxed">
                        <span className="font-semibold text-amber-400">Một phần: </span>
                        {step.detail}
                      </div>
                    )}
                    {step.summary && step.summary.length > 0 ? (
                      <div className="flex flex-wrap gap-1.5">
                        {step.summary.map((item, idx) => (
                          <span
                            key={idx}
                            className="px-2 py-0.5 rounded text-[11px] bg-slate-800/80 text-slate-300 font-mono border border-slate-700/60"
                          >
                            <span className="text-slate-400">{item.label}: </span>
                            <strong className="text-white">
                              {typeof item.value === 'number'
                                ? item.value.toLocaleString()
                                : item.value ?? '—'}
                            </strong>
                            {item.unit ? ` ${item.unit}` : ''}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <div className="text-[11px] text-slate-500 italic">
                        {step.state === 'notStarted'
                          ? 'Chưa có số liệu (bước chưa bắt đầu)'
                          : 'Không có số liệu tóm tắt'}
                      </div>
                    )}
                  </div>

                  {/* Footer bước: Thời điểm cập nhật */}
                  <div className="flex items-center justify-between text-[11px] text-slate-400 font-mono pt-2 border-t border-slate-800/60">
                    <span className="flex items-center gap-1">
                      <Clock className="w-3 h-3 text-slate-500" />
                      {step.updatedAt ? formatDateTime(step.updatedAt) : 'Chưa cập nhật'}
                    </span>
                    <span className="text-sky-400 text-[10px] font-semibold flex items-center gap-0.5">
                      {isSelected ? 'Đang xem' : 'Chi tiết'}
                      <ChevronRight className="w-3 h-3" />
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Khung Chi tiết Bước Được Chọn */}
        {selectedStep && (
          <div className="p-5 sm:p-6 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-4">
            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 pb-3">
              <div className="flex items-center gap-3">
                <span className="w-8 h-8 rounded-lg bg-sky-500/20 text-sky-400 border border-sky-500/30 flex items-center justify-center text-sm font-bold font-mono">
                  #{selectedStep.order}
                </span>
                <div>
                  <h4 className="text-base sm:text-lg font-bold text-white">
                    Chi tiết Bước {selectedStep.order}: {selectedStep.title}
                  </h4>
                  <div className="text-xs text-slate-400 font-mono">
                    ID: <code className="text-sky-400">{selectedStep.id}</code>
                  </div>
                </div>
              </div>

              <div className="flex items-center gap-2">
                <span
                  className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold ${
                    getStepStateBadge(selectedStep.state).badgeClass
                  }`}
                >
                  <span>{getStepStateBadge(selectedStep.state).label}</span>
                </span>
                <span className="text-xs text-slate-400 font-mono">
                  {selectedStep.updatedAt ? formatDateTime(selectedStep.updatedAt) : 'Chưa chạy'}
                </span>
              </div>
            </div>

            {/* Cảnh báo đặc biệt cho trạng thái partial */}
            {selectedStep.state === 'partial' && (
              <div className="p-3.5 rounded-xl bg-amber-500/10 border border-amber-500/30 text-amber-200 text-xs flex items-start gap-2.5">
                <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0 mt-0.5" />
                <div>
                  <strong className="font-semibold text-amber-300">Trạng thái mới xong một phần (partial):</strong>{' '}
                  {selectedStep.detail || 'Một phần của bước đã hoàn tất (như 6a phân cụm ngày), nhưng chưa hoàn thành toàn bộ bước (chờ 6b huấn luyện mô hình). Hiện không có tiến trình nào đang chạy.'}
                </div>
              </div>
            )}

            {/* Mô tả chi tiết (detail) */}
            <div className="p-4 rounded-xl bg-slate-950/60 border border-slate-800/80 space-y-1.5">
              <div className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                <Info className="w-4 h-4 text-sky-400" />
                Mô tả & Ghi chú thực thi:
              </div>
              <p className="text-xs text-slate-300 leading-relaxed font-mono">
                {selectedStep.detail || 'Không có mô tả chi tiết cho bước này.'}
              </p>
            </div>

            {/* Danh sách số liệu mở rộng (summary) */}
            {selectedStep.summary && selectedStep.summary.length > 0 && (
              <div className="space-y-2">
                <span className="text-xs font-semibold text-slate-300">
                  Các chỉ số đo lường do bước này ghi nhận:
                </span>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  {selectedStep.summary.map((m, idx) => (
                    <div
                      key={idx}
                      className="p-3 rounded-xl bg-slate-800/40 border border-slate-800 space-y-0.5"
                    >
                      <div className="text-[11px] text-slate-400">{m.label}</div>
                      <div className="text-base font-bold text-white font-mono">
                        {typeof m.value === 'number' ? m.value.toLocaleString() : m.value ?? '—'}
                        {m.unit && <span className="text-xs text-slate-400 ml-1 font-normal">{m.unit}</span>}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </section>

      {/* ========================================================================= */}
      {/* PHẦN 2: CHỈ SỐ MÔ HÌNH DỰ ĐOÁN CO VÀ MỐC SO SÁNH                          */}
      {/* ========================================================================= */}
      <section className="space-y-6 pt-4 border-t border-slate-800/80">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800 pb-3">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-purple-500/10 border border-purple-500/20 flex items-center justify-center text-purple-400">
              <BarChart3 className="w-4 h-4" />
            </div>
            <div>
              <h3 className="text-base sm:text-lg font-bold text-white tracking-tight">
                Chỉ số Đánh giá Mô hình Dự đoán Khí CO (Test Set)
              </h3>
              <p className="text-xs text-slate-400 font-mono">
                GET /api/model/metrics • So sánh thực nghiệm với 2 mốc cơ sở (Baselines)
              </p>
            </div>
          </div>
        </div>

        {/* Trạng thái 404: MODEL_NOT_TRAINED */}
        {modelUntrained ? (
          <div className="p-8 sm:p-10 rounded-2xl bg-slate-900 border border-dashed border-amber-500/30 text-center space-y-3">
            <AlertTriangle className="w-10 h-10 text-amber-400 mx-auto" />
            <h4 className="text-base font-bold text-white">
              Chưa có mô hình nào được huấn luyện (404 MODEL_NOT_TRAINED)
            </h4>
            <p className="text-xs text-slate-400 max-w-lg mx-auto">
              {modelErrorMsg || 'Pipeline đang ở các bước đầu. Hãy hoàn tất bước 6b (huấn luyện mô hình) để ghi nhận chỉ số đánh giá RMSE, MAE, R².'}
            </p>
            {isMock && (
              <button
                onClick={handleVariantToggle}
                className="mt-2 inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-semibold bg-sky-600 hover:bg-sky-500 text-white transition"
              >
                Chuyển sang Mock có dữ liệu mô hình mẫu
              </button>
            )}
          </div>
        ) : modelMetrics ? (
          <div className="space-y-6">
            {/* 1. BẢNG SO SÁNH TRUNG THỰC: CÁC MÔ HÌNH VÀ 2 MỐC CƠ SỞ */}
            <div className="rounded-2xl bg-slate-900 border border-slate-800 shadow-xl overflow-hidden">
              <div className="p-4 sm:p-5 border-b border-slate-800 flex flex-wrap items-center justify-between gap-2">
                <div>
                  <h4 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
                    <Sparkles className="w-4 h-4 text-purple-400" />
                    Bảng Sai số Kiểm tra (RMSE, MAE, R²) so với Mốc Cơ sở
                  </h4>
                  <p className="text-xs text-slate-400 mt-0.5">
                    Được đánh giá độc lập trên cùng tập kiểm tra ({modelMetrics.testSet.rows.toLocaleString()} dòng). Nguyên tắc: Thấp hơn là tốt hơn cho RMSE/MAE, Cao hơn cho R².
                  </p>
                </div>
                <span className="px-2.5 py-0.5 rounded text-[11px] font-mono bg-purple-500/10 text-purple-300 border border-purple-500/20">
                  Dự báo trước {modelMetrics.horizonHours}h
                </span>
              </div>

              {/* Bảng so sánh */}
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs border-collapse">
                  <thead>
                    <tr className="border-b border-slate-800 bg-slate-950/60 font-mono text-slate-400 text-[11px]">
                      <th className="py-3 px-4">Tên Mô hình / Mốc so sánh</th>
                      <th className="py-3 px-4">Phân loại</th>
                      <th className="py-3 px-4 text-right">RMSE (µg/m³)</th>
                      <th className="py-3 px-4 text-right">MAE (µg/m³)</th>
                      <th className="py-3 px-4 text-right">R²</th>
                      <th className="py-3 px-4 text-center">Thắng mốc "CO giờ trước"?</th>
                      <th className="py-3 px-4 text-center">Thắng mốc "Trung bình giờ"?</th>
                      <th className="py-3 px-4">Tham số / Ghi chú</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/60 font-mono">
                    {/* Danh sách các Mô hình huấn luyện */}
                    {modelMetrics.models.map((m) => (
                      <tr
                        key={m.version}
                        className={`transition ${
                          m.isBest
                            ? 'bg-purple-950/20 hover:bg-purple-950/30 font-semibold'
                            : 'hover:bg-slate-800/40'
                        }`}
                      >
                        <td className="py-3 px-4 font-sans text-white flex items-center gap-2">
                          <span className="font-bold">{m.name}</span>
                          {m.isBest && (
                            <span className="px-2 py-0.5 rounded text-[10px] font-mono bg-emerald-500/20 text-emerald-300 border border-emerald-500/40 uppercase font-bold shrink-0">
                              Mô hình tốt nhất
                            </span>
                          )}
                        </td>
                        <td className="py-3 px-4 text-slate-400 text-[11px]">
                          <code>{m.kind}</code> ({m.version})
                        </td>
                        <td className="py-3 px-4 text-right font-bold text-slate-100">
                          {m.test.rmse.toFixed(1)}
                        </td>
                        <td className="py-3 px-4 text-right text-slate-300">
                          {m.test.mae.toFixed(1)}
                        </td>
                        <td className="py-3 px-4 text-right text-slate-300">
                          {m.test.r2.toFixed(2)}
                        </td>

                        {/* Thắng mốc CO giờ trước (beatsPreviousHour) */}
                        <td className="py-3 px-4 text-center">
                          {m.beatsPreviousHour ? (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
                              <CheckCircle2 className="w-3 h-3" /> Thắng
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] bg-rose-500/20 text-rose-400 border border-rose-500/30">
                              <XCircle className="w-3 h-3" /> Không thắng
                            </span>
                          )}
                        </td>

                        {/* Thắng mốc Trung bình giờ (beatsHourlyMean) */}
                        <td className="py-3 px-4 text-center">
                          {m.beatsHourlyMean ? (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
                              <CheckCircle2 className="w-3 h-3" /> Thắng
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] bg-rose-500/20 text-rose-400 border border-rose-500/30">
                              <XCircle className="w-3 h-3" /> Không thắng
                            </span>
                          )}
                        </td>

                        <td className="py-3 px-4 text-slate-400 text-[11px] font-sans">
                          {m.params || 'Mặc định'}
                        </td>
                      </tr>
                    ))}

                    {/* Danh sách 2 Mốc Cơ Sở (Baselines) */}
                    {modelMetrics.baselines.map((b) => (
                      <tr key={b.id} className="bg-slate-950/40 text-slate-400 hover:bg-slate-900/60">
                        <td className="py-3 px-4 font-sans text-amber-300 font-medium flex items-center gap-2">
                          <span>{b.name}</span>
                          <span className="px-1.5 py-0.2 rounded text-[10px] bg-amber-500/10 text-amber-400 border border-amber-500/30 font-mono">
                            Mốc cơ sở
                          </span>
                        </td>
                        <td className="py-3 px-4 text-[11px]">
                          <code>baseline:{b.id}</code>
                        </td>
                        <td className="py-3 px-4 text-right font-bold text-amber-200">
                          {b.test.rmse.toFixed(1)}
                        </td>
                        <td className="py-3 px-4 text-right text-slate-300">
                          {b.test.mae.toFixed(1)}
                        </td>
                        <td className="py-3 px-4 text-right text-slate-300">
                          {b.test.r2.toFixed(2)}
                        </td>
                        <td className="py-3 px-4 text-center text-slate-500 text-[11px]">—</td>
                        <td className="py-3 px-4 text-center text-slate-500 text-[11px]">—</td>
                        <td className="py-3 px-4 text-[11px] font-sans text-slate-400">
                          {b.description}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* 2. KẾT LUẬN TRUNG THỰC TỪ PIPELINE (Không làm đẹp, hiển thị đúng thực tế) */}
            <div className="p-4 sm:p-5 rounded-2xl bg-amber-500/10 border border-amber-500/30 space-y-2">
              <div className="flex items-center gap-2 font-bold text-amber-300 text-sm">
                <AlertTriangle className="w-5 h-5 text-amber-400 shrink-0" />
                <span>Kết luận Thực nghiệm từ Pipeline:</span>
              </div>
              <p className="text-xs sm:text-sm text-slate-200 leading-relaxed font-sans">
                {modelMetrics.conclusion}
              </p>
              <p className="text-[11px] text-amber-400/90 italic">
                * Tuân thủ quy tắc trung thực: Nếu mô hình không đánh bại được mốc cơ sở (như việc lặp lại CO giờ trước), dashboard hiển thị nguyên văn kết luận này mà không che giấu.
              </p>
            </div>

            {/* 3. THÔNG TIN TẬP DỮ LIỆU & PHƯƠNG PHÁP CHIA (Train / Test Split) */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="p-4 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
                <span className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                  <Database className="w-4 h-4 text-sky-400" />
                  Tập Huấn luyện (Train Set)
                </span>
                <div className="text-xl font-bold text-white font-mono">
                  {modelMetrics.trainSet.rows.toLocaleString()}{' '}
                  <span className="text-xs font-normal text-slate-400">dòng giờ</span>
                </div>
                <div className="text-[11px] text-slate-400 font-mono">
                  Từ: {formatDateTime(modelMetrics.trainSet.from)}
                  <br />
                  Đến: {formatDateTime(modelMetrics.trainSet.to)}
                </div>
              </div>

              <div className="p-4 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
                <span className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                  <Database className="w-4 h-4 text-purple-400" />
                  Tập Kiểm tra (Test Set)
                </span>
                <div className="text-xl font-bold text-white font-mono">
                  {modelMetrics.testSet.rows.toLocaleString()}{' '}
                  <span className="text-xs font-normal text-slate-400">dòng giờ</span>
                </div>
                <div className="text-[11px] text-slate-400 font-mono">
                  Từ: {formatDateTime(modelMetrics.testSet.from)}
                  <br />
                  Đến: {formatDateTime(modelMetrics.testSet.to)}
                </div>
              </div>

              <div className="p-4 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
                <span className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                  <Calendar className="w-4 h-4 text-emerald-400" />
                  Phương pháp phân chia
                </span>
                <div className="text-xs text-slate-300 leading-relaxed font-sans">
                  {modelMetrics.splitMethod}
                </div>
                <div className="text-[10px] text-slate-500 italic">
                  Không xáo ngẫu nhiên nhằm bảo toàn cấu trúc chuỗi thời gian
                </div>
              </div>
            </div>

            {/* 4. CĂN CỨ DỮ LIỆU & ĐẶC TRƯNG ĐẦU VÀO (Features) */}
            <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-3">
              <div className="space-y-1">
                <h4 className="text-sm font-bold text-white flex items-center gap-2">
                  <Cpu className="w-4 h-4 text-sky-400" />
                  Dữ liệu Huấn luyện & Các Đặc trưng Đầu vào (Features)
                </h4>
                <p className="text-xs text-slate-400 leading-relaxed">
                  {modelMetrics.trainedOn}
                </p>
              </div>

              <div className="pt-2 border-t border-slate-800">
                <span className="text-[11px] font-semibold text-slate-300 block mb-2">
                  15 Đặc trưng đầu vào của mô hình:
                </span>
                <div className="flex flex-wrap gap-1.5">
                  {modelMetrics.features.map((feat) => (
                    <span
                      key={feat}
                      className="px-2.5 py-1 rounded-md text-xs font-mono bg-slate-800 text-sky-300 border border-slate-700"
                    >
                      {feat}
                    </span>
                  ))}
                </div>
              </div>
            </div>

            {/* 5. CÁCH XỬ LÝ ĐIỂM GÃY 01/07/2025 (Breakpoint Strategy) */}
            {modelMetrics.breakpoint && (
              <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 shadow-xl space-y-3">
                <div className="flex items-center justify-between border-b border-slate-800 pb-3">
                  <div className="flex items-center gap-2">
                    <TrendingDown className="w-5 h-5 text-rose-400" />
                    <div>
                      <h4 className="text-sm font-bold text-white">
                        Cách xử lý Điểm gãy Dữ liệu ({formatDateTime(modelMetrics.breakpoint.date)})
                      </h4>
                      <p className="text-xs text-slate-400">
                        Chiến lược: <code className="text-rose-300">{modelMetrics.breakpoint.strategy}</code>
                      </p>
                    </div>
                  </div>
                </div>

                <p className="text-xs text-slate-300 leading-relaxed">
                  {modelMetrics.breakpoint.description}
                </p>

                {modelMetrics.breakpoint.effect && (
                  <div className="p-3 bg-slate-950/60 rounded-xl text-xs text-slate-400 border border-slate-800">
                    <strong className="text-slate-200">Hiệu ứng đo được: </strong>
                    {modelMetrics.breakpoint.effect}
                  </div>
                )}
              </div>
            )}
          </div>
        ) : null}
      </section>
    </div>
  );
};

export default PipelineTab;
