import React, { useEffect, useState } from 'react';
import {
  Database,
  HardDrive,
  Folder,
  File,
  ChevronRight,
  RefreshCw,
  AlertTriangle,
  Server,
  Layers,
  X,
  FileText,
} from 'lucide-react';
import { api } from '../services/api';
import { useStream } from '../context/StreamContext';
import type {
  HdfsSummary,
  HdfsDataNodes,
  HdfsListing,
  HdfsFileBlocks,
} from '../types';
import { formatBytes, formatDateTime } from '../utils/format';

export const HdfsTab: React.FC = () => {
  const { recentHdfsOps } = useStream();

  const [summary, setSummary] = useState<HdfsSummary | null>(null);
  const [dataNodes, setDataNodes] = useState<HdfsDataNodes | null>(null);
  const [currentPath, setCurrentPath] = useState<string>('/');
  const [listing, setListing] = useState<HdfsListing | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Xem block của 1 file cụ thể
  const [selectedFileBlocks, setSelectedFileBlocks] = useState<HdfsFileBlocks | null>(null);
  const [loadingBlocks, setLoadingBlocks] = useState<boolean>(false);
  const [blockError, setBlockError] = useState<string | null>(null);


  const loadHdfsOverview = async () => {
    try {
      setLoading(true);
      setError(null);
      const [sumRes, dnRes] = await Promise.all([
        api.getHdfsSummary(),
        api.getHdfsDataNodes(),
      ]);
      setSummary(sumRes);
      setDataNodes(dnRes);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(`Không thể tải dữ liệu HDFS: ${msg}`);
    } finally {
      setLoading(false);
    }
  };

  const loadPath = async (path: string) => {
    try {
      const res = await api.getHdfsFiles(path);
      setListing(res);
      setCurrentPath(path);
    } catch (err: unknown) {
      console.error(`Lỗi duyệt thư mục ${path}:`, err);
    }
  };

  useEffect(() => {
    loadHdfsOverview();
    loadPath('/');
  }, []);

  const handleOpenFileBlocks = async (filePath: string) => {
    try {
      setLoadingBlocks(true);
      setBlockError(null);
      const blocks = await api.getHdfsBlocks(filePath);
      setSelectedFileBlocks(blocks);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setBlockError(`Không thể lấy danh sách block: ${msg}`);
    } finally {
      setLoadingBlocks(false);
    }
  };

  // Tính tỷ lệ % dung lượng đã dùng
  const usedPercent =
    summary && summary.capacityTotalBytes > 0
      ? ((summary.capacityUsedBytes / summary.capacityTotalBytes) * 100).toFixed(1)
      : '0';

  // Điều hướng breadcrumbs
  const pathParts = currentPath.split('/').filter(Boolean);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
            <Database className="w-6 h-6 text-sky-400" />
            Hadoop HDFS — Cụm Lưu trữ Phân tán
          </h2>
          <p className="text-sm text-slate-400">
            Giám sát NameNode JMX, DataNodes và cấu trúc phân mảnh Block
          </p>
        </div>
        <button
          onClick={() => {
            loadHdfsOverview();
            loadPath(currentPath);
          }}
          disabled={loading}
          className="inline-flex items-center gap-2 px-3.5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs sm:text-sm font-medium rounded-lg border border-slate-700 transition disabled:opacity-50 self-start sm:self-auto"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Làm mới HDFS
        </button>
      </div>

      {error && (
        <div className="p-4 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-300 text-sm flex items-start gap-3">
          <AlertTriangle className="w-5 h-5 shrink-0 text-rose-400 mt-0.5" />
          <div>{error}</div>
        </div>
      )}

      {/* Tóm tắt Cụm HDFS (Summary Cards) */}
      {summary && (
        <div className="space-y-4">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
            <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800">
              <span className="text-[11px] text-slate-500 uppercase tracking-wider block">
                Cluster ID / Phiên bản
              </span>
              <span className="text-sm font-bold text-slate-100 font-mono truncate block mt-0.5">
                {summary.clusterId}
              </span>
              <span className="text-[10px] text-sky-400 font-mono">v{summary.version}</span>
            </div>

            <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800">
              <span className="text-[11px] text-slate-500 uppercase tracking-wider block">
                Trạng thái SafeMode
              </span>
              <span
                className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-bold font-mono mt-1 ${
                  summary.safeMode
                    ? 'bg-amber-500/20 text-amber-400'
                    : 'bg-emerald-500/20 text-emerald-400'
                }`}
              >
                {summary.safeMode ? 'SAFE MODE BẬT' : 'BÌNH THƯỜNG'}
              </span>
            </div>

            <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800">
              <span className="text-[11px] text-slate-500 uppercase tracking-wider block">
                DataNodes (Sống / Chết)
              </span>
              <div className="text-base font-bold text-white font-mono mt-0.5">
                <span className="text-emerald-400">{summary.liveDataNodes} sống</span>
                <span className="text-slate-500"> / </span>
                <span className="text-rose-400">{summary.deadDataNodes} chết</span>
              </div>
            </div>

            <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800">
              <span className="text-[11px] text-slate-500 uppercase tracking-wider block">
                Tổng số Files
              </span>
              <span className="text-base font-bold text-white font-mono block mt-0.5">
                {summary.filesTotal.toLocaleString()}
              </span>
            </div>

            <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800">
              <span className="text-[11px] text-slate-500 uppercase tracking-wider block">
                Tổng số Blocks
              </span>
              <span className="text-base font-bold text-white font-mono block mt-0.5">
                {summary.blocksTotal.toLocaleString()}
              </span>
              <span className="text-[10px] text-slate-500">
                (Thiếu: {summary.missingBlocks})
              </span>
            </div>

            <div className="p-3.5 rounded-xl bg-slate-900 border border-slate-800">
              <span className="text-[11px] text-slate-500 uppercase tracking-wider block">
                Kích thước Block chuẩn
              </span>
              <span className="text-sm font-bold text-slate-200 font-mono block mt-0.5">
                {formatBytes(summary.defaultBlockSizeBytes)}
              </span>
              <span className="text-[10px] text-slate-500">
                Replication: {summary.defaultReplication}x
              </span>
            </div>
          </div>

          {/* Thanh Tiến trình Dung lượng */}
          <div className="p-4 rounded-xl bg-slate-900 border border-slate-800">
            <div className="flex flex-wrap items-center justify-between text-xs text-slate-400 mb-2 gap-2">
              <span className="font-semibold text-slate-200">
                Dung lượng Lưu trữ Cụm: {formatBytes(summary.capacityUsedBytes)} /{' '}
                {formatBytes(summary.capacityTotalBytes)} ({usedPercent}%)
              </span>
              <span>Còn trống: {formatBytes(summary.capacityRemainingBytes)}</span>
            </div>
            <div className="w-full bg-slate-800 h-2.5 rounded-full overflow-hidden">
              <div
                className="bg-gradient-to-r from-sky-500 to-indigo-500 h-full rounded-full transition-all duration-500"
                style={{ width: `${Math.min(Number(usedPercent), 100)}%` }}
              />
            </div>
          </div>
        </div>
      )}

      {/* Danh sách DataNodes */}
      <div className="rounded-2xl bg-slate-900 border border-slate-800 p-5">
        <div className="flex items-center gap-2 mb-4">
          <HardDrive className="w-5 h-5 text-indigo-400" />
          <h3 className="text-base font-bold text-white">
            Danh sách DataNodes ({dataNodes?.dataNodes.length || 0})
          </h3>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs font-mono">
            <thead className="bg-slate-800/80 text-slate-400">
              <tr>
                <th className="py-2.5 px-3">Tên Node</th>
                <th className="py-2.5 px-3">Địa chỉ kết nối</th>
                <th className="py-2.5 px-3">Trạng thái</th>
                <th className="py-2.5 px-3">Liên lạc gần nhất</th>
                <th className="py-2.5 px-3">Dung lượng sử dụng</th>
                <th className="py-2.5 px-3">Số Blocks</th>
                <th className="py-2.5 px-3">Phiên bản</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 text-slate-300">
              {dataNodes?.dataNodes.map((dn) => (
                <tr key={dn.name} className="hover:bg-slate-800/40">
                  <td className="py-2.5 px-3 font-bold text-white flex items-center gap-1.5">
                    <Server className="w-3.5 h-3.5 text-slate-500" />
                    {dn.name}
                  </td>
                  <td className="py-2.5 px-3 text-slate-400">{dn.address}</td>
                  <td className="py-2.5 px-3">
                    <span
                      className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${
                        dn.state === 'live'
                          ? 'bg-emerald-500/20 text-emerald-400'
                          : 'bg-rose-500/20 text-rose-400'
                      }`}
                    >
                      {dn.state}
                    </span>
                  </td>
                  <td className="py-2.5 px-3">{dn.lastContactSeconds} giây trước</td>
                  <td className="py-2.5 px-3">
                    {formatBytes(dn.usedBytes)} / {formatBytes(dn.capacityBytes)}
                  </td>
                  <td className="py-2.5 px-3">{dn.blockCount}</td>
                  <td className="py-2.5 px-3 text-slate-500">{dn.version}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Trình duyệt Tệp tin HDFS (File Browser) */}
      <div className="rounded-2xl bg-slate-900 border border-slate-800 p-5">
        <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
          <div className="flex items-center gap-2">
            <Folder className="w-5 h-5 text-amber-400" />
            <h3 className="text-base font-bold text-white">Trình duyệt Tệp tin WebHDFS</h3>
          </div>
          <span className="text-xs text-slate-500">
            Click vào thư mục để duyệt, click vào tệp để xem phân rã Blocks
          </span>
        </div>

        {/* Breadcrumb Navigation */}
        <div className="flex items-center gap-1.5 p-2 rounded-lg bg-slate-800/60 text-xs font-mono text-slate-300 mb-3 overflow-x-auto">
          <button
            onClick={() => loadPath('/')}
            className="hover:text-sky-400 px-1 py-0.5 rounded"
          >
            /
          </button>
          {pathParts.map((part, index) => {
            const pathUpTo = '/' + pathParts.slice(0, index + 1).join('/');
            return (
              <React.Fragment key={pathUpTo}>
                <ChevronRight className="w-3.5 h-3.5 text-slate-600" />
                <button
                  onClick={() => loadPath(pathUpTo)}
                  className={`hover:text-sky-400 px-1 py-0.5 rounded ${
                    index === pathParts.length - 1 ? 'font-bold text-sky-400' : ''
                  }`}
                >
                  {part.replace(/\r/g, '\\r').replace(/\n/g, '\\n')}
                </button>
              </React.Fragment>
            );
          })}
        </div>

        {/* File Table */}
        <div className="overflow-x-auto border border-slate-800 rounded-lg">
          <table className="w-full text-left text-xs font-mono">
            <thead className="bg-slate-800/80 text-slate-400">
              <tr>
                <th className="py-2 px-3">Tên</th>
                <th className="py-2 px-3">Loại</th>
                <th className="py-2 px-3">Kích thước</th>
                <th className="py-2 px-3">Replication</th>
                <th className="py-2 px-3">Kích thước Block</th>
                <th className="py-2 px-3">Người sở hữu</th>
                <th className="py-2 px-3">Thời gian sửa đổi</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/60 text-slate-300">
              {currentPath !== '/' && (
                <tr
                  onClick={() => {
                    const parent = '/' + pathParts.slice(0, -1).join('/');
                    loadPath(parent === '//' ? '/' : parent);
                  }}
                  className="hover:bg-slate-800/40 cursor-pointer text-slate-400"
                >
                  <td colSpan={7} className="py-2 px-3 flex items-center gap-2">
                    <Folder className="w-4 h-4 text-amber-500" />
                    <span>.. (Lên thư mục cha)</span>
                  </td>
                </tr>
              )}
              {listing?.entries.map((entry) => {
                const fullPath =
                  currentPath === '/' ? `/${entry.name}` : `${currentPath}/${entry.name}`;
                const isDir = entry.type === 'directory';
                const hasControlChar = entry.name.includes('\n') || entry.name.includes('\r');
                const safeName = entry.name.replace(/\r/g, '\\r').replace(/\n/g, '\\n');

                return (
                  <tr
                    key={entry.name}
                    onClick={() => {
                      if (isDir) {
                        loadPath(fullPath);
                      } else {
                        handleOpenFileBlocks(fullPath);
                      }
                    }}
                    className="hover:bg-slate-800/50 cursor-pointer"
                  >
                    <td className="py-2 px-3 font-semibold text-white flex items-center gap-2">
                      {isDir ? (
                        <Folder className="w-4 h-4 text-amber-400 shrink-0" />
                      ) : (
                        <File className="w-4 h-4 text-sky-400 shrink-0" />
                      )}
                      <span className={isDir ? 'text-amber-200' : 'text-slate-100'}>
                        {safeName}
                      </span>
                      {hasControlChar && (
                        <span className="px-1.5 py-0.2 rounded text-[10px] bg-rose-500/20 text-rose-300 border border-rose-500/30 font-mono">
                          [\\n]
                        </span>
                      )}
                    </td>
                    <td className="py-2 px-3 text-slate-400">{entry.type}</td>
                    <td className="py-2 px-3">
                      {isDir ? '—' : formatBytes(entry.lengthBytes)}
                    </td>
                    <td className="py-2 px-3">
                      {isDir ? '—' : `${entry.replication}x`}
                    </td>
                    <td className="py-2 px-3">
                      {isDir ? '—' : formatBytes(entry.blockSizeBytes)}
                    </td>

                    <td className="py-2 px-3 text-slate-400">{entry.owner}</td>
                    <td className="py-2 px-3 text-slate-500">
                      {formatDateTime(entry.modifiedAt)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Modal / Panel Xem Block của Tệp tin */}
      {selectedFileBlocks && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-slate-900 border border-slate-800 rounded-2xl max-w-3xl w-full p-6 shadow-2xl space-y-4 max-h-[90vh] overflow-y-auto">
            <div className="flex items-center justify-between border-b border-slate-800 pb-3">
              <div className="flex items-center gap-2">
                <Layers className="w-5 h-5 text-sky-400" />
                <h3 className="text-base font-bold text-white">
                  Phân rã Block của Tệp tin HDFS
                </h3>
                {loadingBlocks && <RefreshCw className="w-4 h-4 text-sky-400 animate-spin" />}
              </div>
              <button
                onClick={() => setSelectedFileBlocks(null)}
                className="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {blockError && (
              <div className="p-3 bg-rose-500/10 border border-rose-500/30 rounded-xl text-xs text-rose-300">
                {blockError}
              </div>
            )}


            <div className="p-3 bg-slate-800/40 rounded-xl text-xs font-mono space-y-1">
              <div>
                <span className="text-slate-500">Đường dẫn: </span>
                <span className="text-sky-300 font-bold">
                  {selectedFileBlocks.path.replace(/\r/g, '\\r').replace(/\n/g, '\\n')}
                </span>

              </div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 pt-2">
                <div>
                  <span className="text-slate-500">Kích thước: </span>
                  <span className="text-slate-200">
                    {formatBytes(selectedFileBlocks.lengthBytes)}
                  </span>
                </div>
                <div>
                  <span className="text-slate-500">Block Size: </span>
                  <span className="text-slate-200">
                    {formatBytes(selectedFileBlocks.blockSizeBytes)}
                  </span>
                </div>
                <div>
                  <span className="text-slate-500">Replication: </span>
                  <span className="text-slate-200">{selectedFileBlocks.replication}x</span>
                </div>
                <div>
                  <span className="text-slate-500">Tổng Blocks: </span>
                  <span className="text-slate-200">{selectedFileBlocks.blockCount}</span>
                </div>
              </div>
            </div>

            <div>
              <h4 className="text-xs uppercase font-bold text-slate-400 mb-2">
                Danh sách các Blocks ({selectedFileBlocks.blocks.length})
              </h4>
              <div className="space-y-2 max-h-60 overflow-y-auto">
                {selectedFileBlocks.blocks.map((blk) => (
                  <div
                    key={blk.blockId}
                    className="p-3 rounded-lg bg-slate-800/60 border border-slate-800 flex flex-col sm:flex-row sm:items-center justify-between text-xs font-mono gap-2"
                  >
                    <div>
                      <div className="font-bold text-amber-300 flex items-center gap-1.5">
                        <FileText className="w-3.5 h-3.5" />
                        Block #{blk.index}: {blk.blockId}
                      </div>
                      <div className="text-slate-400 text-[11px] mt-0.5">
                        Kích thước: {formatBytes(blk.lengthBytes)}
                      </div>
                    </div>
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <span className="text-slate-500 text-[11px]">Vị trí DataNodes:</span>
                      {blk.locations.map((loc) => (
                        <span
                          key={loc}
                          className="px-2 py-0.5 rounded bg-sky-500/10 text-sky-300 border border-sky-500/20 text-[11px]"
                        >
                          {loc}
                        </span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="flex justify-end pt-2 border-t border-slate-800">
              <button
                onClick={() => setSelectedFileBlocks(null)}
                className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-xs font-medium"
              >
                Đóng
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Nhật ký thao tác HDFS thời gian thực (từ hdfs.op stream) */}
      <div className="rounded-2xl bg-slate-900 border border-slate-800 p-5">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-bold text-white flex items-center gap-2">
            <Layers className="w-4 h-4 text-amber-400" />
            Thao tác HDFS Gần nhất (Nhận từ sự kiện Stream)
          </h3>
          <span className="text-xs text-slate-500 font-mono">
            {recentHdfsOps.length} sự kiện
          </span>
        </div>

        <div className="space-y-2">
          {recentHdfsOps.length === 0 ? (
            <div className="p-4 text-center text-xs text-slate-500">
              Chưa có thao tác HDFS mới trong luồng stream...
            </div>
          ) : (
            recentHdfsOps.map((op, i) => (
              <div
                key={i}
                className="p-2.5 rounded-lg bg-slate-800/40 text-xs font-mono flex flex-wrap items-center justify-between gap-2 border border-slate-800/50"
              >
                <div className="flex items-center gap-2">
                  <span className="px-1.5 py-0.5 rounded text-[10px] font-bold uppercase bg-amber-500/20 text-amber-400 border border-amber-500/30">
                    {op.op}
                  </span>
                  <span className="text-slate-200 font-semibold">{op.path}</span>
                </div>
                <div className="flex items-center gap-3 text-slate-400 text-[11px]">
                  {op.blockId && <span>Block: {op.blockId}</span>}
                  {op.datanode && <span>Node: {op.datanode}</span>}
                  {op.bytes !== null && op.bytes !== undefined && (
                    <span>Size: {formatBytes(op.bytes)}</span>
                  )}
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
};
