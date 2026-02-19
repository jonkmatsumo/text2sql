import React, { createContext, useCallback, useState, useRef, useEffect } from "react";

export type ToastType = "success" | "error" | "info" | "warning";

export interface Toast {
  id: string;
  message: string;
  type: ToastType;
  duration: number;
  dedupeKey?: string;
}

export interface ToastOptions {
  duration?: number;
  dedupeKey?: string;
}

export interface ToastContextValue {
  toasts: Toast[];
  show: (message: string, type: ToastType, options?: number | ToastOptions) => string;
  dismiss: (id: string) => void;
}

export const ToastContext = createContext<ToastContextValue | null>(null);

interface ToastProviderProps {
  children: React.ReactNode;
}

export function ToastProvider({ children }: ToastProviderProps) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  // Map of dedupeKey -> timestamp (ms)
  const dedupeCache = useRef<Map<string, number>>(new Map());
  const TTL_MS = 5 * 60 * 1000; // 5 minutes
  const MAX_CACHE_SIZE = 50;

  // Cleanup expired entries periodically
  useEffect(() => {
    const interval = setInterval(() => {
      const now = Date.now();
      const cache = dedupeCache.current;
      for (const [key, timestamp] of cache.entries()) {
        if (now - timestamp > TTL_MS) {
          cache.delete(key);
        }
      }
    }, 60000); // Check every minute
    return () => clearInterval(interval);
  }, []);

  const dismiss = useCallback((id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const show = useCallback(
    (message: string, type: ToastType, options?: number | ToastOptions): string => {
      const duration = typeof options === "number" ? options : options?.duration ?? 4000;
      const dedupeKey = typeof options === "object" ? options?.dedupeKey : undefined;

      if (dedupeKey) {
        const now = Date.now();
        const lastShown = dedupeCache.current.get(dedupeKey);

        if (lastShown && now - lastShown < TTL_MS) {
          return "";
        }

        // Bounding logic: if cache is full, remove oldest entry
        if (dedupeCache.current.size >= MAX_CACHE_SIZE) {
          const oldestKey = dedupeCache.current.keys().next().value;
          if (oldestKey) dedupeCache.current.delete(oldestKey);
        }

        dedupeCache.current.set(dedupeKey, now);
      }

      const id = crypto.randomUUID();
      const toast: Toast = { id, message, type, duration, dedupeKey };

      setToasts((prev) => [...prev, toast]);

      if (duration > 0) {
        setTimeout(() => dismiss(id), duration);
      }

      return id;
    },
    [dismiss]
  );

  return (
    <ToastContext.Provider value={{ toasts, show, dismiss }}>
      {children}
      <ToastContainer toasts={toasts} onDismiss={dismiss} />
    </ToastContext.Provider>
  );
}

interface ToastContainerProps {
  toasts: Toast[];
  onDismiss: (id: string) => void;
}

function ToastContainer({ toasts, onDismiss }: ToastContainerProps) {
  if (toasts.length === 0) return null;

  return (
    <div className="toast-container">
      {toasts.map((toast) => (
        <div
          key={toast.id}
          className={`toast toast--${toast.type}`}
          role="alert"
        >
          <span className="toast__icon">{getIcon(toast.type)}</span>
          <span className="toast__message">{toast.message}</span>
          <button
            type="button"
            className="toast__dismiss"
            onClick={() => onDismiss(toast.id)}
            aria-label="Dismiss"
          >
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>
      ))}
    </div>
  );
}

function getIcon(type: ToastType): string {
  switch (type) {
    case "success":
      return "\u2713";
    case "error":
      return "\u2717";
    case "warning":
      return "\u26A0";
    case "info":
    default:
      return "\u2139";
  }
}
