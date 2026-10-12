import React, { useEffect, useState } from 'react';
import {
  GitCommit,
  Layers,
  CheckCircle2,
  Clock,
  ArrowRight,
  HardDrive,
  Cpu,
  Server,
  Terminal,
  Activity,
  AlertCircle,
  RefreshCw,
  Info,
  Radio,
  Zap,
  Database,
} from 'lucide-react';
import { api } from '../services/api';
import { useStream } from '../context/useStream';
import type { FlowList, FlowDefinition, FlowSummary } from '../types';

export const FlowsTab: React.FC = () => {
  const { flowStepsByFlow, latestRunIdByFlow, recentHdfsOps } = useStream();
  const [flows, setFlows] = useState<FlowSummary[]>([]);
  const [selectedFlowId, setSelectedFlowId] = useState<string>('hdfs-write');
  const [currentFlow, setCurrentFlow] = useState<FlowDefinition | null>(null);
  const [userSelectedRunId, setUserSelectedRunId] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Lấy danh sách luồng
  const loadFlowList = async () => {
    try {
      setLoading(true);
      setError(null);
      const res: FlowList = await api.listFlows();
      setFlows(res.flows);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(`Không thể tải danh sách luồng: ${msg}`);
    } finally {
      setLoading(false);
    }
  };

  // Tải chi tiết luồng được chọn
  const loadFlowDetail = async (id: string) => {
    try {
      setError(null);
      const detail = await api.getFlow(id);
      setCurrentFlow(detail);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(`Không thể tải chi tiết luồng ${id}: ${msg}`);
      setCurrentFlow(null);
    }
  };

  useEffect(() => {
    let ignore = false;
    api.listFlows()
      .then((res) => {
        if (!ignore) {
          setFlows(res.flows);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!ignore) {
          const msg = err instanceof Error ? err.message : String(err);
          setError(`Không thể tải danh sách luồng: ${msg}`);
          setLoading(false);
        }
      });

    return () => {
      ignore = true;
    };
  }, []);

  useEffect(() => {
    if (!selectedFlowId) return;
    let ignore = false;
    api.getFlow(selectedFlowId)
      .then((detail) => {
        if (!ignore) setCurrentFlow(detail);
      })
      .catch((err) => {
        if (!ignore) {
          const msg = err instanceof Error ? err.message : String(err);
          setError(`Không thể tải chi tiết luồng ${selectedFlowId}: ${msg}`);
          setCurrentFlow(null);
        }
      });

    return () => {
      ignore = true;
    };
  }, [selectedFlowId]);

  // Đổi luồng và reset run đã chọn thủ công
  const handleSelectFlow = (flowId: string) => {
    setSelectedFlowId(flowId);
    setUserSelectedRunId(null);
  };

  // QUAN TRỌNG: Lọc flow.step nghiêm ngặt theo selectedFlowId
  const runsForFlow = flowStepsByFlow[selectedFlowId] || {};
  const runIds = Object.keys(runsForFlow);
  const latestRunForFlow = latestRunIdByFlow[selectedFlowId] || null;
  const activeRun =
    userSelectedRunId && runsForFlow[userSelectedRunId]
      ? userSelectedRunId
      : latestRunForFlow || (runIds.length > 0 ? runIds[runIds.length - 1] : null);
  const activeStepsForRun = activeRun ? runsForFlow[activeRun] || [] : [];
  const completedStepOrders = new Set(activeStepsForRun.map((s) => s.step));

  // Kiểm tra bước là Quan sát thật
  const isStepReal = (flowId: string, order: number): boolean => {
    if (flowId === 'hdfs-write') return order === 2 || order === 7 || order === 8;
    if (flowId === 'kafka-produce') return order === 2; // ACK kèm offset là quan sát thật
    if (flowId === 'spark-streaming') return false; // Cả 4 bước đều là suy ra từ micro-batch thành công
    return false;
  };

  // Kiểm tra bước là Tác vụ nền
  const isStepBackground = (flowId: string, order: number): boolean => {
    if (flowId === 'hdfs-write') return order === 9;
    return false;
  };

  const getComponentIcon = (kind: string) => {
    switch (kind) {
      case 'client':
        return <Terminal className="w-5 h-5 text-sky-400" />;
      case 'namenode':
      case 'secondary-namenode':
        return <Server className="w-5 h-5 text-indigo-400" />;
      case 'datanode':
        return <HardDrive className="w-5 h-5 text-emerald-400" />;
      case 'kafka-broker':
        return <Radio className="w-5 h-5 text-purple-400" />;
      case 'spark':
        return <Zap className="w-5 h-5 text-amber-400" />;
      case 'storage':
        return <Database className="w-5 h-5 text-emerald-400" />;
      case 'yarn':
        return <Cpu className="w-5 h-5 text-amber-400" />;
      default:
        return <Layers className="w-5 h-5 text-slate-400" />;
    }
  };

  const renderFlowGuidance = () => {
    if (selectedFlowId === 'hdfs-write') {
      return (
        <div className="p-4 rounded-xl bg-sky-500/10 border border-sky-500/20 text-xs text-sky-300 flex items-start gap-3">
          <Info className="w-5 h-5 shrink-0 text-sky-400 mt-0.5" />
          <div className="space-y-1">
            <p className="font-semibold text-sky-200">
              Quan sát các bước sáng lên theo thời gian thực (HDFS Write):
            </p>
            <p className="text-slate-300">
              Chạy lệnh sau trong terminal WSL Ubuntu để phát bản ghi qua HDFS:
            </p>
            <code className="block bg-slate-950/80 px-2.5 py-1.5 rounded font-mono text-amber-300 border border-slate-800">
              python3 infra/replay_producer.py --count 20 --interval 3
            </code>
            <p className="text-slate-400 text-[11px]">
              * Lưu ý: Bước 2, 7, 8 quan sát thật từ NameNode log; Bước 1, 3, 4, 5, 6 suy ra. Sự kiện <code>reading</code> có thể tới trước các bước 4-6 vì backend đọc file ngay khi đổi tên.
            </p>
          </div>
        </div>
      );
    }
    if (selectedFlowId === 'kafka-produce') {
      return (
        <div className="p-4 rounded-xl bg-purple-500/10 border border-purple-500/20 text-xs text-purple-300 flex items-start gap-3">
          <Info className="w-5 h-5 shrink-0 text-purple-400 mt-0.5" />
          <div className="space-y-1">
            <p className="font-semibold text-purple-200">
              Luồng Kafka Producer (Open-Meteo → Kafka topic hcmc_co_stream):
            </p>
            <p className="text-slate-300">
              <code>live_producer.py</code> lấy CO và thời tiết từ Open-Meteo và chỉ gửi vào topic khi có quan sát mới.
            </p>
            <p className="text-slate-400 text-[11px]">
              * <strong>Bước 2 (ACK kèm offset)</strong> là quan sát thật sau khi bản ghi đã nằm trong log trên đĩa; <strong>Bước 1 (send)</strong> là suy ra. Run ID có dạng <code>kafka-p&#123;partition&#125;-o&#123;offset&#125;</code> (ví dụ: <code>kafka-p0-o7</code>).
            </p>
          </div>
        </div>
      );
    }
    if (selectedFlowId === 'spark-streaming') {
      return (
        <div className="p-4 rounded-xl bg-amber-500/10 border border-amber-500/20 text-xs text-amber-300 flex items-start gap-3">
          <Info className="w-5 h-5 shrink-0 text-amber-400 mt-0.5" />
          <div className="space-y-1">
            <p className="font-semibold text-amber-200">
              Luồng Spark Structured Streaming (Kafka → Spark → HDFS /live):
            </p>
            <p className="text-slate-300">
              Ứng dụng Spark Structured Streaming đọc từ Kafka theo offset, tính tích lũy CO và ghi Parquet kèm checkpoint vào HDFS.
            </p>
            <p className="text-slate-400 text-[11px]">
              * <strong>Tất cả 4 bước đều là SUY RA</strong> từ việc micro-batch (job Spark) thành công (do Spark REST không lộ chi tiết từng bước con). Run ID có dạng <code>spark-batch-&#123;batchId&#125;</code> (ví dụ: <code>spark-batch-12</code>).
            </p>
          </div>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
            <GitCommit className="w-6 h-6 text-sky-400" />
            Sơ đồ Luồng Kỹ thuật & Theo dõi Bước Trực tiếp
          </h2>
          <p className="text-sm text-slate-400">
            Giám sát quy trình truyền nhận, ghi dữ liệu HDFS, Kafka và Spark Streaming theo thời gian thực
          </p>
        </div>
        <button
          onClick={() => {
            loadFlowList();
            if (selectedFlowId) loadFlowDetail(selectedFlowId);
          }}
          disabled={loading}
          className="inline-flex items-center gap-2 px-3.5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs sm:text-sm font-medium rounded-lg border border-slate-700 transition disabled:opacity-50 self-start sm:self-auto"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Làm mới
        </button>
      </div>

      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm flex items-start gap-3">
          <AlertCircle className="w-5 h-5 shrink-0 text-rose-400 mt-0.5" />
          <div>{error}</div>
        </div>
      )}

      {/* Tabs chọn Luồng */}
      <div className="flex flex-wrap gap-2 border-b border-slate-800 pb-3">
        {flows.map((f) => {
          const isSelected = f.id === selectedFlowId;
          const isAvailable = f.status === 'available';
          return (
            <button
              key={f.id}
              onClick={() => handleSelectFlow(f.id)}
              className={`px-3.5 py-2 rounded-xl text-xs sm:text-sm font-medium transition flex items-center gap-2 border ${
                isSelected
                  ? 'bg-sky-500/10 text-sky-400 border-sky-500/40 shadow-sm'
                  : 'bg-slate-900/60 text-slate-400 border-slate-800 hover:bg-slate-800/60 hover:text-slate-200'
              }`}
            >
              <span>{f.title}</span>
              {isAvailable ? (
                <span className="px-1.5 py-0.5 rounded text-[10px] font-mono uppercase bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
                  Sẵn sàng
                </span>
              ) : (
                <span className="px-1.5 py-0.5 rounded text-[10px] font-mono uppercase bg-slate-800 text-slate-400 border border-slate-700">
                  Chưa có
                </span>
              )}
            </button>
          );
        })}
      </div>

      {/* Nội dung luồng */}
      {currentFlow ? (
        <div className="space-y-6">
          {/* Card Thông tin luồng & Chọn RunId */}
          <div className="p-5 rounded-2xl bg-slate-900 border border-slate-800 flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div className="space-y-1">
              <div className="flex items-center gap-2">
                <h3 className="text-lg font-bold text-white">{currentFlow.title}</h3>
                <span className="text-xs font-mono px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
                  id: {currentFlow.id}
                </span>
              </div>
              <p className="text-xs text-slate-400">
                {flows.find((f) => f.id === currentFlow.id)?.description}
              </p>
            </div>

            {/* RunId selector - Chỉ hiện runId của flow hiện tại */}
            <div className="flex flex-col sm:flex-row sm:items-center gap-2">
              <span className="text-xs text-slate-400">Phiên chạy (Run ID):</span>
              {runIds.length > 0 ? (
                <select
                  value={activeRun || ''}
                  onChange={(e) => setUserSelectedRunId(e.target.value)}
                  className="px-3 py-1.5 bg-slate-800 border border-slate-700 rounded-lg text-xs font-mono text-slate-200 focus:outline-none focus:border-sky-500"
                >
                  {runIds.map((rid) => (
                    <option key={rid} value={rid}>
                      {rid} {rid === latestRunForFlow ? '(Mới nhất)' : ''}
                    </option>
                  ))}
                </select>
              ) : (
                <span className="text-xs font-mono text-slate-500 bg-slate-800/60 px-3 py-1.5 rounded-lg border border-slate-800">
                  Chưa có lượt chạy nào cho luồng này
                </span>
              )}
            </div>
          </div>

          {/* Hướng dẫn thử nghiệm / chi tiết kỹ thuật của luồng */}
          {renderFlowGuidance()}

          {/* Thành phần tham gia (Components) */}
          <div className="space-y-3">
            <h4 className="text-sm font-semibold text-slate-300 flex items-center gap-2">
              <Layers className="w-4 h-4 text-sky-400" />
              Các thành phần tham gia luồng ({currentFlow.components.length})
            </h4>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
              {currentFlow.components.map((comp) => (
                <div
                  key={comp.id}
                  className="p-4 rounded-xl bg-slate-900/80 border border-slate-800/80 space-y-2"
                >
                  <div className="flex items-center gap-2.5">
                    <div className="p-2 rounded-lg bg-slate-800 border border-slate-700/60">
                      {getComponentIcon(comp.kind)}
                    </div>
                    <div>
                      <h5 className="text-sm font-bold text-white">{comp.label}</h5>
                      <span className="text-[11px] font-mono text-slate-400">id: {comp.id}</span>
                    </div>
                  </div>
                  <p className="text-xs text-slate-400">{comp.note}</p>
                </div>
              ))}
            </div>
          </div>

          {/* Các bước thực thi (Steps) */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <h4 className="text-sm font-semibold text-slate-300 flex items-center gap-2">
                <Activity className="w-4 h-4 text-emerald-400" />
                Các bước thực thi & Trạng thái quan sát ({currentFlow.steps.length} bước)
              </h4>
              <div className="flex items-center gap-3 text-xs">
                <span className="flex items-center gap-1.5 text-emerald-400">
                  <span className="w-2 h-2 rounded-full bg-emerald-400" /> Quan sát thật
                </span>
                <span className="flex items-center gap-1.5 text-amber-400">
                  <span className="w-2 h-2 rounded-full bg-amber-400" /> Suy ra từ log
                </span>
                <span className="flex items-center gap-1.5 text-purple-400">
                  <span className="w-2 h-2 rounded-full bg-purple-400" /> Tác vụ nền
                </span>
              </div>
            </div>

            <div className="space-y-2">
              {currentFlow.steps.map((st) => {
                const isReal = isStepReal(currentFlow.id, st.order);
                const isBg = isStepBackground(currentFlow.id, st.order);
                const isCompleted = completedStepOrders.has(st.order);

                return (
                  <div
                    key={st.order}
                    className={`p-4 rounded-xl border transition flex flex-col md:flex-row md:items-center justify-between gap-3 ${
                      isCompleted
                        ? 'bg-emerald-500/10 border-emerald-500/40'
                        : 'bg-slate-900/60 border-slate-800/80 hover:border-slate-700'
                    }`}
                  >
                    <div className="flex items-start md:items-center gap-3.5">
                      {/* Step order number */}
                      <div
                        className={`w-7 h-7 shrink-0 rounded-full flex items-center justify-center text-xs font-mono font-bold ${
                          isCompleted
                            ? 'bg-emerald-500 text-slate-950 shadow-md shadow-emerald-500/30'
                            : 'bg-slate-800 text-slate-300 border border-slate-700'
                        }`}
                      >
                        {st.order}
                      </div>

                      <div className="space-y-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="text-sm font-semibold text-white">{st.label}</span>

                          {/* Phân loại Thật / Suy ra / Nền */}
                          {isReal ? (
                            <span className="px-2 py-0.5 rounded text-[10px] font-medium bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
                              Quan sát thật
                            </span>
                          ) : isBg ? (
                            <span className="px-2 py-0.5 rounded text-[10px] font-medium bg-purple-500/20 text-purple-400 border border-purple-500/30">
                              Tác vụ nền (không theo file)
                            </span>
                          ) : (
                            <span className="px-2 py-0.5 rounded text-[10px] font-medium bg-amber-500/20 text-amber-400 border border-amber-500/30">
                              Suy ra từ log
                            </span>
                          )}

                          {/* Kênh truyền */}
                          <span
                            className={`px-1.5 py-0.2 rounded text-[10px] font-mono ${
                              st.channel === 'data'
                                ? 'bg-sky-500/10 text-sky-400 border border-sky-500/20'
                                : st.channel === 'control'
                                ? 'bg-indigo-500/10 text-indigo-400 border border-indigo-500/20'
                                : 'bg-slate-800 text-slate-400 border border-slate-700'
                            }`}
                          >
                            kênh: {st.channel}
                          </span>

                          {st.repeats && (
                            <span className="text-[10px] font-mono text-slate-400 bg-slate-800/80 px-1.5 py-0.2 rounded">
                              lặp: {st.repeats}
                            </span>
                          )}
                        </div>

                        {/* From -> To */}
                        <div className="flex items-center gap-1.5 text-xs text-slate-400 font-mono">
                          <span className="text-slate-300 font-medium">{st.from}</span>
                          <ArrowRight className="w-3.5 h-3.5 text-slate-500" />
                          <span className="text-slate-300 font-medium">{st.to}</span>
                        </div>
                      </div>
                    </div>

                    {/* Step live status */}
                    <div className="flex items-center gap-2 self-start md:self-auto pl-10 md:pl-0">
                      {isCompleted ? (
                        <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
                          <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                          Đã hoàn thành (done)
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium bg-slate-800 text-slate-400 border border-slate-700/60">
                          <Clock className="w-3.5 h-3.5 text-slate-500" />
                          Chưa kích hoạt
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Lịch sử thao tác HDFS gần đây */}
          <div className="space-y-3 pt-2">
            <h4 className="text-sm font-semibold text-slate-300 flex items-center gap-2">
              <HardDrive className="w-4 h-4 text-emerald-400" />
              Sự kiện Thao tác HDFS gần đây (hdfs.op từ luồng trực tiếp)
            </h4>
            {recentHdfsOps.length > 0 ? (
              <div className="overflow-x-auto rounded-xl border border-slate-800">
                <table className="w-full text-left text-xs">
                  <thead className="bg-slate-900 text-slate-400 font-mono uppercase text-[11px] border-b border-slate-800">
                    <tr>
                      <th className="px-4 py-2.5">Thao tác (op)</th>
                      <th className="px-4 py-2.5">Đường dẫn (path)</th>
                      <th className="px-4 py-2.5">Block ID</th>
                      <th className="px-4 py-2.5">DataNode</th>
                      <th className="px-4 py-2.5 text-right">Dung lượng</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/60 bg-slate-900/40 font-mono text-slate-300">
                    {recentHdfsOps.slice(0, 8).map((op, idx) => (
                      <tr key={idx} className="hover:bg-slate-800/30">
                        <td className="px-4 py-2 font-bold text-sky-400">{op.op}</td>
                        <td className="px-4 py-2 text-slate-200 truncate max-w-xs">{op.path}</td>
                        <td className="px-4 py-2 text-amber-400">{op.blockId || '—'}</td>
                        <td className="px-4 py-2 text-slate-400">{op.datanode || '—'}</td>
                        <td className="px-4 py-2 text-right">
                          {op.bytes !== null ? `${op.bytes} B` : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="p-4 rounded-xl bg-slate-900/60 border border-dashed border-slate-800 text-center text-xs text-slate-500">
                Chưa nhận được sự kiện hdfs.op nào trong phiên này
              </div>
            )}
          </div>
        </div>
      ) : (
        /* Trường hợp luồng chưa có sẵn (status: planned) */
        <div className="p-12 rounded-2xl bg-slate-900/40 border border-dashed border-slate-800 text-center space-y-3">
          <Layers className="w-10 h-10 text-slate-600 mx-auto" />
          <h3 className="text-base font-bold text-white">
            Luồng "{flows.find((f) => f.id === selectedFlowId)?.title || selectedFlowId}" chưa có sẵn
          </h3>
          <p className="text-xs text-slate-400 max-w-md mx-auto">
            Luồng này hiện đang trong trạng thái kế hoạch (planned). Hãy chọn luồng{' '}
            <strong className="text-sky-400">"HDFS ghi file"</strong> (hoặc luồng khả dụng khác khi bật FLOWS_EXTRA) để xem sơ đồ thành phần và các bước thực thi trực tiếp.
          </p>
        </div>
      )}
    </div>
  );
};
