import type { StreamEvent } from '../types';

export type ConnectionStatus = 'connected' | 'connecting' | 'disconnected';
export type StreamEventListener = (event: StreamEvent) => void;
export type StatusChangeListener = (status: ConnectionStatus) => void;

export interface StreamSource {
  start(): void;
  stop(): void;
  subscribe(listener: StreamEventListener): () => void;
  onStatusChange(listener: StatusChangeListener): () => void;
}

/**
 * ReplaySource đọc file mock/stream-events.ndjson và phát lại tuần tự từng sự kiện.
 */
export class ReplaySource implements StreamSource {
  private listeners = new Set<StreamEventListener>();
  private statusListeners = new Set<StatusChangeListener>();
  private status: ConnectionStatus = 'disconnected';
  private timer: number | null = null;
  private events: StreamEvent[] = [];
  private currentIndex = 0;
  private intervalMs: number;

  constructor(intervalMs: number = 1000) {
    this.intervalMs = intervalMs;
  }

  private setStatus(newStatus: ConnectionStatus) {
    this.status = newStatus;
    this.statusListeners.forEach((l) => l(newStatus));
  }

  async start() {
    if (this.status === 'connected' || this.status === 'connecting') return;
    this.setStatus('connecting');

    try {
      const res = await fetch('/mock/stream-events.ndjson');
      if (!res.ok) {
        throw new Error(`Không tải được stream-events.ndjson: ${res.statusText}`);
      }
      const text = await res.text();
      const lines = text
        .split('\n')
        .map((l) => l.trim())
        .filter((l) => l.length > 0);

      this.events = lines
        .map((line) => {
          try {
            return JSON.parse(line) as StreamEvent;
          } catch {
            return null;
          }
        })
        .filter((e): e is StreamEvent => e !== null);

      if (this.events.length === 0) {
        this.setStatus('disconnected');
        return;
      }

      this.setStatus('connected');
      this.currentIndex = 0;
      this.scheduleNext();
    } catch (err) {
      console.warn('[ReplaySource] Lỗi nạp mock events:', err);
      this.setStatus('disconnected');
    }
  }

  private scheduleNext() {
    if (this.status !== 'connected' || this.events.length === 0) return;

    this.timer = window.setTimeout(() => {
      const event = this.events[this.currentIndex];
      this.currentIndex = (this.currentIndex + 1) % this.events.length;

      this.listeners.forEach((l) => {
        try {
          l(event);
        } catch (e) {
          console.error('[ReplaySource] Lỗi trong listener:', e);
        }
      });

      this.scheduleNext();
    }, this.intervalMs);
  }

  stop() {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.setStatus('disconnected');
  }

  subscribe(listener: StreamEventListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  onStatusChange(listener: StatusChangeListener): () => void {
    this.statusListeners.add(listener);
    listener(this.status);
    return () => {
      this.statusListeners.delete(listener);
    };
  }
}

/**
 * WebSocketSource kết nối trực tiếp đến ws://localhost:8000/ws/stream
 * Có cơ chế tự động kết nối lại 1s, 2s, 4s... tối đa 15s.
 */
export class WebSocketSource implements StreamSource {
  private wsUrl: string;
  private ws: WebSocket | null = null;
  private listeners = new Set<StreamEventListener>();
  private statusListeners = new Set<StatusChangeListener>();
  private status: ConnectionStatus = 'disconnected';
  private reconnectAttempt = 0;
  private reconnectTimer: number | null = null;
  private lastSeq: number | null = null;
  private isExplicitlyStopped = false;

  constructor(wsUrl?: string) {
    this.wsUrl =
      wsUrl ||
      import.meta.env.VITE_WS_URL ||
      'ws://localhost:8000/ws/stream';
  }

  private setStatus(newStatus: ConnectionStatus) {
    this.status = newStatus;
    this.statusListeners.forEach((l) => l(newStatus));
  }

  start() {
    this.isExplicitlyStopped = false;
    this.connect();
  }

  private connect() {
    if (this.isExplicitlyStopped) return;
    this.setStatus('connecting');

    try {
      this.ws = new WebSocket(this.wsUrl);

      this.ws.onopen = () => {
        this.setStatus('connected');
        this.reconnectAttempt = 0;
      };

      this.ws.onmessage = (evt) => {
        try {
          const parsed = JSON.parse(evt.data);
          // Kiểm tra cấu trúc StreamEnvelope tối thiểu
          if (parsed && typeof parsed.type === 'string' && typeof parsed.seq === 'number') {
            if (this.lastSeq !== null && parsed.seq > this.lastSeq + 1) {
              console.warn(
                `[WebSocketSource] Nhảy chuỗi seq từ ${this.lastSeq} sang ${parsed.seq}. Có thể đã mất tin.`
              );
            }
            this.lastSeq = parsed.seq;

            this.listeners.forEach((l) => {
              try {
                l(parsed as StreamEvent);
              } catch (e) {
                console.error('[WebSocketSource] Lỗi trong listener:', e);
              }
            });
          }
        } catch {
          // Bỏ qua tin không hợp lệ, không gây sập
        }
      };

      this.ws.onclose = () => {
        this.setStatus('disconnected');
        this.scheduleReconnect();
      };

      this.ws.onerror = () => {
        this.setStatus('disconnected');
        // onclose sẽ được gọi tiếp theo
      };
    } catch {
      this.setStatus('disconnected');
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect() {
    if (this.isExplicitlyStopped) return;

    // Chờ 1s, 2s, 4s, 8s, tối đa 15s
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempt), 15000);
    this.reconnectAttempt++;

    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
    }
    this.reconnectTimer = window.setTimeout(() => {
      this.connect();
    }, delay);
  }

  stop() {
    this.isExplicitlyStopped = true;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.setStatus('disconnected');
  }

  subscribe(listener: StreamEventListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  onStatusChange(listener: StatusChangeListener): () => void {
    this.statusListeners.add(listener);
    listener(this.status);
    return () => {
      this.statusListeners.delete(listener);
    };
  }
}

let singletonSource: StreamSource | null = null;

export function getStreamSource(): StreamSource {
  if (!singletonSource) {
    const streamMode = import.meta.env.VITE_STREAM || 'replay';
    if (streamMode === 'ws') {
      singletonSource = new WebSocketSource();
    } else {
      singletonSource = new ReplaySource(1200);
    }
  }
  return singletonSource;
}
