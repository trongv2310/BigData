import React, { useEffect, useRef, useState, useCallback } from 'react';
import { Terminal as XTerm } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import '@xterm/xterm/css/xterm.css';
import {
  Terminal as TerminalIcon,
  Key,
  Play,
  RotateCcw,
  ShieldAlert,
  Info,
  CheckCircle2,
  XCircle,
} from 'lucide-react';

const CLOSE_CODE_EXPLANATIONS: Record<number, { title: string; desc: string; isWarning?: boolean }> = {
  1006: {
    title: 'Lỗi Bắt tay WebSocket (HTTP 403 Forbidden)',
    desc: 'Backend từ chối kết nối do Origin không hợp lệ hoặc client không kết nối qua loopback (127.0.0.1/localhost:5173).',
    isWarning: true,
  },
  4401: {
    title: 'Mã 4401 — Sai hoặc Thiếu Token Xác thực',
    desc: 'Token cửa sổ lệnh không chính xác hoặc chưa được cung cấp. Hãy kiểm tra console lúc backend khởi động để lấy token ngẫu nhiên.',
    isWarning: true,
  },
  4403: {
    title: 'Mã 4403 — Cửa sổ lệnh đang TẮT mặc định',
    desc: 'Để bảo vệ HDFS/Kafka/Spark không bị can thiệp ngoài ý muốn, cửa sổ lệnh mặc định bị TẮT. Để bật, hãy chạy backend với biến môi trường BACKEND_ALLOW_WRITES=1.',
    isWarning: true,
  },
  4408: {
    title: 'Mã 4408 — Hết hạn do Không hoạt động (Timeout)',
    desc: 'Phiên làm việc tự động đóng sau 30 phút không có hoạt động để đảm bảo an toàn tài nguyên.',
  },
  4409: {
    title: 'Mã 4409 — Xung đột Phiên (Đã có phiên đang mở)',
    desc: 'Chỉ cho phép duy nhất một phiên cửa sổ lệnh hoạt động tại một thời điểm. Vui lòng đóng phiên khác trước.',
  },
};

export const TerminalTab: React.FC = () => {
  const containerRef = useRef<HTMLDivElement>(null);
  const termRef = useRef<XTerm | null>(null);
  const fitAddonRef = useRef<FitAddon | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  const [token, setToken] = useState<string>(() => {
    return localStorage.getItem('terminal_token') || '';
  });
  const [tokenInput, setTokenInput] = useState<string>(token);
  const [status, setStatus] = useState<'idle' | 'connecting' | 'connected' | 'disconnected'>('idle');
  const [closeInfo, setCloseInfo] = useState<{ code: number; reason: string } | null>(null);

  // Lưu token vào localStorage khi thay đổi
  const saveToken = (newToken: string) => {
    setToken(newToken);
    localStorage.setItem('terminal_token', newToken);
  };

  const disconnect = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    setStatus('disconnected');
  }, []);

  const connect = useCallback(() => {
    if (!token.trim()) {
      setCloseInfo({
        code: 4401,
        reason: 'Vui lòng nhập Token xác thực của backend trước khi kết nối.',
      });
      return;
    }

    disconnect();
    setStatus('connecting');
    setCloseInfo(null);

    // Xác định URL WebSocket terminal từ VITE_API_BASE
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8000';
    const wsBase = apiBase.replace(/^http/, 'ws');
    const wsUrl = `${wsBase}/ws/terminal?token=${encodeURIComponent(token.trim())}`;

    try {
      const socket = new WebSocket(wsUrl);
      wsRef.current = socket;

      socket.onopen = () => {
        setStatus('connected');
        termRef.current?.write('\r\n\x1b[32m[✓ Kết nối Cửa sổ lệnh thành công]\x1b[0m\r\n\r\n');

        // Gửi kích thước ban đầu nếu fit addon sẵn sàng
        if (termRef.current && fitAddonRef.current) {
          fitAddonRef.current.fit();
          socket.send(
            JSON.stringify({
              type: 'resize',
              cols: termRef.current.cols,
              rows: termRef.current.rows,
            })
          );
        }
      };

      socket.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === 'output' && typeof msg.data === 'string') {
            termRef.current?.write(msg.data);
          } else if (msg.type === 'exit') {
            termRef.current?.write(`\r\n\x1b[33m[Phiên làm việc kết thúc với mã ${msg.code}]\x1b[0m\r\n`);
          }
        } catch {
          // Trường hợp tin nhắn dạng text thô
          termRef.current?.write(event.data);
        }
      };

      socket.onerror = () => {
        // Lỗi kết nối
      };

      socket.onclose = (event) => {
        setStatus('disconnected');
        setCloseInfo({
          code: event.code,
          reason: event.reason || '',
        });
        termRef.current?.write(
          `\r\n\x1b[31m[Đã ngắt kết nối — Mã: ${event.code} ${
            event.reason ? `(${event.reason})` : ''
          }]\x1b[0m\r\n`
        );
      };
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setStatus('disconnected');
      setCloseInfo({
        code: 0,
        reason: `Không thể khởi tạo WebSocket: ${msg}`,
      });
    }
  }, [token, disconnect]);

  // Khởi tạo terminal instance
  useEffect(() => {
    if (!containerRef.current) return;

    const term = new XTerm({
      cursorBlink: true,
      fontFamily: 'Fira Code, Menlo, Monaco, Consolas, monospace',
      fontSize: 13,
      lineHeight: 1.2,
      theme: {
        background: '#090d16',
        foreground: '#e2e8f0',
        cursor: '#38bdf8',
        selectionBackground: '#1e293b',
        black: '#0f172a',
        red: '#f43f5e',
        green: '#10b981',
        yellow: '#f59e0b',
        blue: '#3b82f6',
        magenta: '#a855f7',
        cyan: '#06b6d4',
        white: '#f8fafc',
        brightBlack: '#64748b',
        brightRed: '#fb7185',
        brightGreen: '#34d399',
        brightYellow: '#fbbf24',
        brightBlue: '#60a5fa',
        brightMagenta: '#c084fc',
        brightCyan: '#22d3ee',
        brightWhite: '#ffffff',
      },
    });

    const fitAddon = new FitAddon();
    term.loadAddon(fitAddon);

    term.open(containerRef.current);
    fitAddon.fit();

    termRef.current = term;
    fitAddonRef.current = fitAddon;

    term.write(
      '\x1b[1;36m=== Hệ thống Quản trị Big Data CO HCMC — Terminal Shell ===\x1b[0m\r\n' +
        '\x1b[90mNhập token từ backend console và nhấn "Kết nối Terminal" để bắt đầu.\x1b[0m\r\n\r\n'
    );

    // Xử lý gửi dữ liệu khi gõ
    const onDataDisposable = term.onData((data) => {
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        wsRef.current.send(
          JSON.stringify({
            type: 'input',
            data,
          })
        );
      }
    });

    // Resize observer để tự động fit
    const resizeObserver = new ResizeObserver(() => {
      try {
        fitAddon.fit();
        if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
          wsRef.current.send(
            JSON.stringify({
              type: 'resize',
              cols: term.cols,
              rows: term.rows,
            })
          );
        }
      } catch {
        // Bỏ qua lỗi resize khi chưa mount
      }
    });

    if (containerRef.current) {
      resizeObserver.observe(containerRef.current);
    }

    return () => {
      onDataDisposable.dispose();
      resizeObserver.disconnect();
      disconnect();
      term.dispose();
    };
  }, [disconnect]);

  const explanation = closeInfo ? CLOSE_CODE_EXPLANATIONS[closeInfo.code] : null;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-white tracking-tight flex items-center gap-2">
            <TerminalIcon className="w-6 h-6 text-sky-400" />
            Cửa sổ Lệnh Trực tiếp (Terminal PTY)
          </h2>
          <p className="text-sm text-slate-400">
            Mở phiên bash trực tiếp trên môi trường máy chủ cụm Big Data (chỉ qua loopback 127.0.0.1)
          </p>
        </div>

        {/* Trạng thái kết nối */}
        <div className="flex items-center gap-2">
          {status === 'connected' ? (
            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">
              <CheckCircle2 className="w-4 h-4 text-emerald-400" />
              Đang kết nối
            </span>
          ) : status === 'connecting' ? (
            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold bg-amber-500/20 text-amber-400 border border-amber-500/30">
              <RotateCcw className="w-4 h-4 animate-spin text-amber-400" />
              Đang kết nối...
            </span>
          ) : (
            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold bg-slate-800 text-slate-400 border border-slate-700">
              <XCircle className="w-4 h-4 text-slate-500" />
              Chưa kết nối
            </span>
          )}
        </div>
      </div>

      {/* Thanh Token & Điều khiển */}
      <div className="p-4 rounded-2xl bg-slate-900 border border-slate-800 space-y-3 shadow-xl">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-3">
          <div className="flex-1 flex flex-col sm:flex-row sm:items-center gap-3">
            <div className="flex items-center gap-2 text-xs font-semibold text-slate-300">
              <Key className="w-4 h-4 text-amber-400" />
              <span>Token xác thực:</span>
            </div>
            <div className="flex-1 flex items-center gap-2">
              <input
                type="password"
                value={tokenInput}
                onChange={(e) => setTokenInput(e.target.value)}
                placeholder="Dán token ngẫu nhiên in từ backend console..."
                className="w-full max-w-md px-3.5 py-2 bg-slate-950 border border-slate-700 rounded-xl text-xs font-mono text-slate-100 placeholder-slate-500 focus:outline-none focus:border-sky-500"
              />
              <button
                onClick={() => saveToken(tokenInput)}
                className="px-3 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-medium rounded-xl border border-slate-700 transition"
              >
                Lưu Token
              </button>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {status === 'connected' ? (
              <button
                onClick={disconnect}
                className="px-4 py-2 bg-rose-600 hover:bg-rose-500 text-white text-xs font-bold rounded-xl transition"
              >
                Ngắt kết nối
              </button>
            ) : (
              <button
                onClick={() => {
                  saveToken(tokenInput);
                  connect();
                }}
                disabled={status === 'connecting'}
                className="inline-flex items-center gap-2 px-5 py-2 bg-sky-500 hover:bg-sky-400 text-slate-950 text-xs font-bold rounded-xl transition shadow-lg shadow-sky-500/20 disabled:opacity-50"
              >
                <Play className="w-3.5 h-3.5 fill-slate-950" />
                Kết nối Terminal
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Thông báo giải thích mã đóng kết nối (Đặc biệt 4403 tắt mặc định) */}
      {closeInfo && (
        <div
          className={`p-4 rounded-xl border space-y-2 text-xs sm:text-sm ${
            explanation?.isWarning
              ? 'bg-amber-500/10 border-amber-500/30 text-amber-300'
              : 'bg-slate-900 border-slate-800 text-slate-300'
          }`}
        >
          <div className="flex items-center gap-2 font-bold">
            <ShieldAlert className="w-4 h-4 shrink-0 text-amber-400" />
            <span>
              {explanation?.title || `Đóng kết nối với mã ${closeInfo.code}`}
            </span>
          </div>
          <p className="text-slate-300 pl-6 leading-relaxed">
            {explanation?.desc || closeInfo.reason || 'Kết nối cửa sổ lệnh đã bị đóng.'}
          </p>
          {closeInfo.code === 4403 && (
            <div className="pl-6 pt-1">
              <span className="text-[11px] text-slate-400 block mb-1">
                Lệnh chạy backend với quyền ghi terminal trong WSL Ubuntu:
              </span>
              <code className="inline-block px-3 py-1.5 bg-slate-950 rounded font-mono text-emerald-400 text-xs border border-slate-800">
                BACKEND_ALLOW_WRITES=1 python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000
              </code>
            </div>
          )}
        </div>
      )}

      {/* Terminal Viewport */}
      <div className="rounded-2xl border border-slate-800 bg-[#090d16] p-3 shadow-2xl overflow-hidden">
        <div className="flex items-center justify-between pb-2 mb-2 border-b border-slate-800/80 px-2 text-xs font-mono text-slate-500">
          <div className="flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full bg-rose-500/80 inline-block" />
            <span className="w-2.5 h-2.5 rounded-full bg-amber-500/80 inline-block" />
            <span className="w-2.5 h-2.5 rounded-full bg-emerald-500/80 inline-block" />
            <span className="ml-2 text-slate-400">bash @ co-hcmc-cluster</span>
          </div>
          <span>Origin: http://localhost:5173</span>
        </div>
        <div ref={containerRef} className="h-[480px] w-full" />
      </div>

      {/* Hướng dẫn an toàn bảo mật */}
      <div className="p-4 rounded-xl bg-slate-900/40 border border-slate-800 text-xs text-slate-400 flex items-start gap-3">
        <Info className="w-4 h-4 text-sky-400 shrink-0 mt-0.5" />
        <div className="space-y-1">
          <p className="font-semibold text-slate-300">Quy định Bảo mật Cửa sổ lệnh:</p>
          <ul className="list-disc pl-4 space-y-0.5 text-slate-400 text-[11px]">
            <li>Cửa sổ lệnh chỉ kết nối tới <code>127.0.0.1:8000</code> và chỉ chấp nhận Origin từ <code>localhost:5173</code>.</li>
            <li>Phiên làm việc tự động khóa sau 30 phút không hoạt động và chỉ cho phép 1 phiên duy nhất.</li>
            <li>Backend không lưu nhật ký lịch sử dòng lệnh (<code>HISTFILE=/dev/null</code>) để bảo mật.</li>
          </ul>
        </div>
      </div>
    </div>
  );
};
