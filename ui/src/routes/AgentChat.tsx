import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { Link } from "react-router-dom";
import {
  runAgent,
  runAgentStream,
  submitFeedback,
  fetchQueryTargetSettings,
  generateSQL,
  executeSQL,
  type GenerateSQLRequest,
  type ExecuteSQLRequest,
  ApiError
} from "../api";
import { ExecutionProgress } from "../components/common/ExecutionProgress";
import { ErrorCard, ErrorCardProps } from "../components/common/ErrorCard";
import type { RunStatus } from "../types/runLifecycle";
import { phaseIndex, PHASE_ORDER } from "../types/runLifecycle";
import RunIdentifiers from "../components/common/RunIdentifiers";
import { useConfirmation } from "../hooks/useConfirmation";
import { ConfirmationDialog } from "../components/common/ConfirmationDialog";
import { useAvailableModels } from "../hooks/useAvailableModels";
import { ChartRenderer } from "../components/charts/ChartRenderer";
import { ErrorState } from "../components/common/ErrorState";
import { ChartSchema } from "../types/charts";
import { CopyBundleButton } from "../components/chat/CopyBundleButton";
import { SQLPreviewCard } from "../components/chat/SQLPreviewCard";
import { DecisionLogPanel } from "../components/chat/DecisionLogPanel";
import { CopyButton } from "../components/artifacts/CopyButton";
import { getVerboseModeFromSearch, loadVerboseMode, saveVerboseMode } from "../utils/verboseMode";
import { dedupeRows } from "../utils/dedupeRows";
import { getErrorMapping } from "../utils/errorMapping";
import { toPrettyJson } from "../utils/observability";

interface Message {
  role: "user" | "assistant";
  text?: string;
  sql?: string;
  result?: any;
  error?: string;
  interactionId?: string;
  requestId?: string;
  fromCache?: boolean;
  cacheSimilarity?: number;
  vizSpec?: any;
  traceId?: string;
  resultCompleteness?: any;
  retrySummary?: any;
  validationSummary?: any;
  validationReport?: any;
  decisionEvents?: any[];
  emptyResultGuidance?: string;
  errorMetadata?: any;
  originalRequest?: { question: string; tenant_id: number; thread_id: string };
}

const LLM_PROVIDERS = [
  { value: "openai", label: "OpenAI" },
  { value: "anthropic", label: "Anthropic" },
  { value: "google", label: "Google" }
];

const FALLBACK_MODELS: Record<string, { value: string; label: string }[]> = {
  openai: [
    { value: "gpt-4o", label: "GPT-4o" },
    { value: "gpt-4o-mini", label: "GPT-4o Mini" },
    { value: "gpt-4-turbo", label: "GPT-4 Turbo" }
  ],
  anthropic: [
    { value: "claude-sonnet-4-20250514", label: "Claude Sonnet 4" },
    { value: "claude-3-5-sonnet-20241022", label: "Claude 3.5 Sonnet" },
    { value: "claude-3-5-haiku-20241022", label: "Claude 3.5 Haiku" }
  ],
  google: [
    { value: "gemini-2.0-flash", label: "Gemini 2.0 Flash" },
    { value: "gemini-1.5-pro", label: "Gemini 1.5 Pro" },
    { value: "gemini-1.5-flash", label: "Gemini 1.5 Flash" }
  ]
};

function formatValue(value: any): string {
  if (value == null) return "\u2014";
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

function formatSimilarity(value: number): number {
  if (Number.isNaN(value)) return 0;
  if (value > 1) return Math.round(value);
  return Math.round(value * 100);
}

function getActionsForCategory(category?: string): Array<{ label: string; href: string }> {
  return getErrorMapping(category).actions;
}

function buildErrorData(err: unknown): ErrorCardProps {
  if (err instanceof ApiError) {
    const meta = err.details as Record<string, any>;
    const category = meta?.error_category || err.code?.toLowerCase();
    return {
      category,
      message: err.displayMessage,
      requestId: err.requestId,
      hint: meta?.hint as string | undefined,
      retryable: meta?.retryable as boolean | undefined,
      retryAfterSeconds: meta?.retry_after_seconds as number | undefined,
      detailsSafe: meta?.details_safe as Record<string, unknown> | undefined,
      actions: getActionsForCategory(category),
    };
  }
  if (err instanceof Error) {
    return { message: err.message };
  }
  return { message: "An unexpected error occurred" };
}

function mapStreamResultToMessage(
  data: any,
  request: { question: string; tenant_id: number; thread_id: string }
): Message {
  return {
    role: "assistant",
    text: data.response ?? undefined,
    sql: data.sql ?? data.current_sql ?? undefined,
    result: data.result ?? data.query_result,
    error: data.error ?? undefined,
    interactionId: data.interaction_id ?? undefined,
    requestId: data.request_id ?? undefined,
    fromCache: data.from_cache,
    cacheSimilarity: data.cache_similarity,
    vizSpec: data.viz_spec,
    traceId: data.trace_id ?? data.run_id ?? undefined,
    resultCompleteness: data.result_completeness,
    retrySummary: data.retry_summary,
    validationSummary: data.validation_summary,
    validationReport: data.validation_report,
    decisionEvents: data.decision_events,
    emptyResultGuidance: data.empty_result_guidance ?? undefined,
    errorMetadata: data.error_metadata,
    originalRequest: request,
  };
}

function ResultsTable({ rows }: { rows: any[] }) {
  const columns = useMemo(() => {
    const keys = new Set<string>();
    rows.forEach((row) => Object.keys(row || {}).forEach((key) => keys.add(key)));
    return Array.from(keys);
  }, [rows]);

  if (!columns.length) {
    return <div className="empty">No structured rows returned.</div>;
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={idx}>
              {columns.map((col) => (
                <td key={col}>{formatValue(row?.[col])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ValidationSummaryBadge({ summary }: { summary: any }) {
  if (!summary) return null;

  const { ast_valid, schema_drift_suspected, missing_identifiers, syntax_errors } = summary;

  return (
    <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", marginTop: "8px" }}>
      {ast_valid === false && (
        <span className="pill" style={{ background: "rgba(220, 53, 69, 0.1)", color: "var(--error)", border: "1px solid rgba(220, 53, 69, 0.2)" }}>
          AST Validation Failed
        </span>
      )}
      {syntax_errors && syntax_errors.length > 0 && (
        <span className="pill" style={{ background: "rgba(220, 53, 69, 0.1)", color: "var(--error)", border: "1px solid rgba(220, 53, 69, 0.2)" }}>
          {syntax_errors.length} Syntax {syntax_errors.length === 1 ? "Error" : "Errors"}
        </span>
      )}
      {schema_drift_suspected && (
        <span className="pill" style={{ background: "rgba(255, 193, 7, 0.1)", color: "#856404", border: "1px solid rgba(255, 193, 7, 0.2)" }}>
          Schema Drift Suspected
          {missing_identifiers && missing_identifiers.length > 0 && ` (${missing_identifiers.length} missing)`}
        </span>
      )}
    </div>
  );
}

function SQLValidationDetails({ summary, report }: { summary: any; report?: any }) {
  if (!summary && !report) return null;
  const syntax_errors = summary?.syntax_errors;
  const semantic_warnings = summary?.semantic_warnings;
  const missing_identifiers = summary?.missing_identifiers;

  const hasIssues = (syntax_errors && syntax_errors.length > 0) ||
    (semantic_warnings && semantic_warnings.length > 0) ||
    (missing_identifiers && missing_identifiers.length > 0);

  const tablesUsed =
    (Array.isArray(report?.table_lineage) && report.table_lineage.length > 0 && report.table_lineage) ||
    (Array.isArray(report?.affected_tables) && report.affected_tables.length > 0 && report.affected_tables) ||
    (Array.isArray(summary?.table_lineage) && summary.table_lineage.length > 0 && summary.table_lineage) ||
    (Array.isArray(summary?.tables_used) && summary.tables_used.length > 0 && summary.tables_used) ||
    [];

  const complexityScore = report?.query_complexity_score ?? summary?.query_complexity_score ?? null;

  const hasAggregation =
    report?.has_aggregation ?? report?.metadata?.has_aggregation ?? summary?.has_aggregation;
  const hasSubquery =
    report?.has_subquery ?? report?.metadata?.has_subquery ?? summary?.has_subquery;
  const hasWindowFunction =
    report?.has_window_function ??
    report?.metadata?.has_window_function ??
    summary?.has_window_function;

  const cartesianWarning = Boolean(
    report?.detected_cartesian_flag ||
    report?.metadata?.detected_cartesian_flag ||
    summary?.detected_cartesian_flag
  );
  const rawValidationReport = report ?? summary;
  const hasValidationFailure = Boolean(
    summary?.ast_valid === false ||
    (Array.isArray(syntax_errors) && syntax_errors.length > 0) ||
    (Array.isArray(missing_identifiers) && missing_identifiers.length > 0)
  );
  let failureGuidance = "";
  if (hasValidationFailure) {
    if (summary?.schema_drift_suspected) {
      failureGuidance = "Validation failed due to schema mismatch. Refresh schema metadata, then retry.";
    } else if (Array.isArray(syntax_errors) && syntax_errors.length > 0) {
      failureGuidance = "Validation failed due to SQL syntax issues. Fix syntax errors before executing.";
    } else if (Array.isArray(missing_identifiers) && missing_identifiers.length > 0) {
      failureGuidance = "Validation failed due to unresolved identifiers. Verify table and column names.";
    } else {
      failureGuidance = "Validation failed. Review the report and adjust the SQL before retrying.";
    }
  }

  if (!hasIssues && !cartesianWarning && !tablesUsed.length && complexityScore == null && !hasValidationFailure) {
    return null;
  }

  return (
    <div className="sql-validation-details" style={{ marginTop: "12px", fontSize: "0.85rem", borderTop: "1px dashed var(--border-muted)", paddingTop: "8px" }}>
      <div
        data-testid="validation-key-signals"
        style={{
          marginBottom: "10px",
          padding: "10px",
          borderRadius: "8px",
          background: "var(--surface-muted)",
          border: "1px solid var(--border-muted)",
          display: "grid",
          gap: "6px",
        }}
      >
        {tablesUsed.length > 0 && (
          <div>
            <strong>Tables used:</strong> {tablesUsed.join(", ")}
          </div>
        )}
        {complexityScore != null && (
          <div>
            <strong>Complexity score:</strong> {complexityScore}
          </div>
        )}
        {(typeof hasAggregation === "boolean" || typeof hasSubquery === "boolean" || typeof hasWindowFunction === "boolean") && (
          <div>
            <strong>Query features:</strong>{" "}
            Aggregation: {typeof hasAggregation === "boolean" ? (hasAggregation ? "Yes" : "No") : "—"} ·{" "}
            Subquery: {typeof hasSubquery === "boolean" ? (hasSubquery ? "Yes" : "No") : "—"} ·{" "}
            Window: {typeof hasWindowFunction === "boolean" ? (hasWindowFunction ? "Yes" : "No") : "—"}
          </div>
        )}
      </div>
      {cartesianWarning && (
        <div
          data-testid="validation-cartesian-warning"
          style={{
            color: "#856404",
            background: "rgba(255, 193, 7, 0.12)",
            border: "1px solid rgba(255, 193, 7, 0.3)",
            borderRadius: "8px",
            padding: "8px 10px",
            marginBottom: "8px",
            fontWeight: 600,
          }}
        >
          Potential cartesian join detected. Confirm join predicates before execution.
        </div>
      )}
      {hasValidationFailure && (
        <div
          data-testid="validation-failure-guidance"
          style={{
            marginBottom: "8px",
            padding: "8px 10px",
            borderRadius: "8px",
            background: "rgba(220, 53, 69, 0.08)",
            border: "1px solid rgba(220, 53, 69, 0.25)",
            color: "var(--error)",
            fontWeight: 600,
          }}
        >
          {failureGuidance}
        </div>
      )}
      {syntax_errors?.map((err: string, i: number) => (
        <div key={`syn-${i}`} style={{ color: "var(--error)", display: "flex", gap: "6px", marginBottom: "4px" }}>
          <span>❌</span> <span>{err}</span>
        </div>
      ))}
      {semantic_warnings?.map((warn: string, i: number) => (
        <div key={`sem-${i}`} style={{ color: "var(--warn, #f59e0b)", display: "flex", gap: "6px", marginBottom: "4px" }}>
          <span>⚠️</span> <span>{warn}</span>
        </div>
      ))}
      {missing_identifiers?.map((id: string, i: number) => (
        <div key={`miss-${i}`} style={{ color: "var(--muted)", display: "flex", gap: "6px", marginBottom: "4px" }}>
          <span>🔍</span> <span>Missing identifier: <code>{id}</code></span>
        </div>
      ))}
      {rawValidationReport && (
        <details style={{ marginTop: "8px" }}>
          <summary style={{ cursor: "pointer", color: "var(--muted)" }}>Validation report (raw)</summary>
          <div style={{ display: "flex", justifyContent: "flex-end", marginTop: "6px" }}>
            <CopyButton text={toPrettyJson(rawValidationReport)} label="Copy validation report" />
          </div>
          <pre data-testid="validation-raw-report" style={{ marginTop: "6px", overflowX: "auto", fontSize: "0.75rem" }}>
            {toPrettyJson(rawValidationReport)}
          </pre>
        </details>
      )}
    </div>
  );
}

function RetrySummaryBadge({ summary }: { summary: any }) {
  if (!summary || !summary.attempts || summary.attempts.length === 0) {
    return null;
  }

  const count = summary.attempts.length;
  const isExhausted = summary.budget_exhausted || summary.max_retries_reached;

  return (
    <div className="retry-badge" style={{
      display: "inline-flex",
      alignItems: "center",
      gap: "6px",
      fontSize: "0.75rem",
      color: isExhausted ? "var(--error)" : "var(--muted)",
      marginTop: "8px",
      fontWeight: 500
    }}>
      <span style={{
        width: "8px",
        height: "8px",
        borderRadius: "50%",
        background: isExhausted ? "var(--error)" : "#28a745"
      }} />
      {count} {count === 1 ? "retry attempt" : "retry attempts"}
      {isExhausted && " (Budget exhausted)"}
    </div>
  );
}

function ValidationCompletenessSummary({
  summary,
  report,
  completeness,
  onExpand,
}: {
  summary?: any;
  report?: any;
  completeness?: any;
  onExpand: () => void;
}) {
  if (!summary && !report && !completeness) return null;

  const syntaxCount = Array.isArray(summary?.syntax_errors) ? summary.syntax_errors.length : 0;
  const missingCount = Array.isArray(summary?.missing_identifiers) ? summary.missing_identifiers.length : 0;
  const validationFailed = Boolean(summary?.ast_valid === false || syntaxCount > 0 || missingCount > 0);

  const cartesianRisk = Boolean(
    report?.detected_cartesian_flag ||
    report?.metadata?.detected_cartesian_flag ||
    summary?.detected_cartesian_flag
  );

  let completenessLabel = "complete";
  if (completeness?.token_expired) completenessLabel = "token expired";
  else if (completeness?.schema_mismatch) completenessLabel = "schema mismatch";
  else if (completeness?.is_truncated || completeness?.is_limited) completenessLabel = "truncated";
  else if (completeness?.next_page_token) completenessLabel = "paginated";

  const pagesFetched =
    typeof completeness?.pages_fetched === "number"
      ? completeness.pages_fetched
      : completeness
        ? 1
        : "—";

  return (
    <button
      type="button"
      data-testid="validation-completeness-summary"
      onClick={onExpand}
      style={{
        marginTop: "8px",
        width: "100%",
        textAlign: "left",
        borderRadius: "8px",
        border: "1px solid var(--border)",
        background: "var(--surface-muted)",
        padding: "8px 10px",
        fontSize: "0.8rem",
        color: "var(--muted)",
        cursor: "pointer",
      }}
    >
      <strong style={{ color: validationFailed ? "var(--error)" : "var(--success)" }}>
        Validation: {validationFailed ? "fail" : "pass"}
      </strong>
      {" · "}
      <span>Cartesian: {cartesianRisk ? "risk" : "none"}</span>
      {" · "}
      <span>Completeness: {completenessLabel}</span>
      {" · "}
      <span>Pages: {pagesFetched}</span>
    </button>
  );
}

function ResultCompletenessBanner({ completeness }: { completeness: any }) {
  if (!completeness) return null;
  const { is_truncated, is_limited, partial_reason, rows_returned, row_limit, query_limit } = completeness;
  const stoppedReason = completeness.stopped_reason ?? completeness.auto_pagination_stopped_reason;
  const metadataRows: Array<{ label: string; value: string | number }> = [];
  if (typeof completeness.auto_paginated === "boolean") {
    metadataRows.push({ label: "auto_paginated", value: String(completeness.auto_paginated) });
  }
  if (typeof completeness.pages_fetched === "number") {
    metadataRows.push({ label: "pages_fetched", value: completeness.pages_fetched });
  }
  if (stoppedReason) {
    metadataRows.push({ label: "stopped_reason", value: String(stoppedReason) });
  }
  if (typeof completeness.prefetch_enabled === "boolean") {
    metadataRows.push({ label: "prefetch_enabled", value: String(completeness.prefetch_enabled) });
  }
  if (typeof completeness.prefetch_scheduled === "boolean") {
    metadataRows.push({ label: "prefetch_scheduled", value: String(completeness.prefetch_scheduled) });
  }
  if (completeness.prefetch_reason) {
    metadataRows.push({ label: "prefetch_reason", value: String(completeness.prefetch_reason) });
  }
  const hasMetadata = metadataRows.length > 0;
  if (!is_truncated && !is_limited && !completeness.next_page_token && !completeness.schema_mismatch && !completeness.token_expired && !hasMetadata) {
    return null;
  }

  if (completeness.token_expired) {
    return (
      <div data-testid="token-expired-warning" className="completeness-banner warning" style={{
        fontSize: "0.8rem",
        padding: "6px 10px",
        borderRadius: "6px",
        marginTop: "8px",
        background: "rgba(255, 193, 7, 0.15)",
        borderLeft: "3px solid #ffc107",
        color: "#856404",
      }}>
        Pagination token expired. Re-run query to see more results.
      </div>
    );
  }

  if (completeness.schema_mismatch) {
    return (
      <div data-testid="schema-mismatch-warning" className="completeness-banner warning" style={{
        fontSize: "0.8rem",
        padding: "8px 12px",
        borderRadius: "8px",
        marginTop: "12px",
        background: "rgba(220, 53, 69, 0.1)",
        borderLeft: "4px solid #dc3545",
        color: "#842029",
      }}>
        <strong>⚠️ Schema Mismatch:</strong> Column schema changed between pages. Cannot append rows.
      </div>
    );
  }

  let message = "";
  let icon = "ℹ️";
  if (is_truncated) {
    message = `Results truncated to ${rows_returned} rows per ${partial_reason || "system limits"}.`;
    icon = "✂️";
  } else if (is_limited) {
    message = `Showing first ${rows_returned} rows (Limit: ${row_limit || query_limit}).`;
    icon = "⏹️";
  } else if (completeness.next_page_token) {
    message = `Displaying ${rows_returned} rows. More results are available via pagination.`;
    icon = "📄";
  }

  if (!message && hasMetadata) {
    message = "Result pagination metadata:";
  }
  if (!message) return null;

  return (
    <div className="completeness-banner info" style={{
      fontSize: "0.8rem",
      padding: "8px 12px",
      borderRadius: "8px",
      marginTop: "12px",
      background: "var(--surface-muted)",
      border: "1px solid var(--border-muted)",
      display: "flex",
      alignItems: "center",
      gap: "8px",
      color: "var(--muted)"
    }}>
      <span style={{ fontSize: "1rem" }}>{icon}</span>
      <div style={{ display: "grid", gap: "4px" }}>
        <span>{message}</span>
        {metadataRows.length > 0 && (
          <div data-testid="completeness-metadata" style={{ display: "flex", gap: "8px", flexWrap: "wrap", fontSize: "0.75rem" }}>
            {metadataRows.map((item) => (
              <span key={item.label}>
                <strong>{item.label}:</strong> {item.value}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export default function AgentChat() {
  const [tenantId, setTenantId] = useState<number>(1);
  const [llmProvider, setLlmProvider] = useState<string>("openai");
  const [llmModel, setLlmModel] = useState<string>("gpt-4o");
  const [question, setQuestion] = useState<string>("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [runStatus, setRunStatus] = useState<RunStatus>("idle");
  const [currentPhase, setCurrentPhase] = useState<string | null>(null);
  const [completedPhases, setCompletedPhases] = useState<string[]>([]);
  const [correctionAttempt, setCorrectionAttempt] = useState<number>(0);
  const [error, setError] = useState<ErrorCardProps | null>(null);
  const [feedbackState, setFeedbackState] = useState<Record<string, string>>({});
  const [loadingMore, setLoadingMore] = useState<number | null>(null);
  const [expandedSqlSections, setExpandedSqlSections] = useState<Record<number, boolean>>({});
  const [configStatus, setConfigStatus] = useState<"loading" | "configured" | "unconfigured">("loading");
  const [isCheckingConfig, setIsCheckingConfig] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const threadIdRef = useRef<string>(crypto.randomUUID());
  const [searchParams] = useSearchParams();
  const [verboseMode, setVerboseMode] = useState(() =>
    loadVerboseMode(searchParams.toString())
  );

  const [previewData, setPreviewData] = useState<{
    sql: string;
    originalRequest: GenerateSQLRequest;
  } | null>(null);
  const [isGeneratingSQL, setIsGeneratingSQL] = useState(false);

  // Abort any in-flight stream on unmount
  useEffect(() => {
    return () => { abortRef.current?.abort(); };
  }, []);

  // Check configuration on mount (cached to prevent flicker on re-render)
  const configCheckedRef = useRef(false);
  const checkConfig = useCallback(() => {
    setIsCheckingConfig(true);
    fetchQueryTargetSettings()
      .then((settings) => {
        setConfigStatus(settings.active ? "configured" : "unconfigured");
      })
      .catch(() => {
        setConfigStatus("configured"); // assume configured on error to not block
      })
      .finally(() => {
        setIsCheckingConfig(false);
      });
  }, []);

  useEffect(() => {
    if (configCheckedRef.current) return;
    configCheckedRef.current = true;
    checkConfig();
  }, [checkConfig]);

  const isLoading = runStatus === "streaming" || runStatus === "finalizing";

  const fallbackModels = FALLBACK_MODELS[llmProvider] || FALLBACK_MODELS.openai;
  const { models: availableModels, isLoading: modelsLoading, error: modelsError } =
    useAvailableModels(llmProvider, fallbackModels);
  const { confirm, dialogProps } = useConfirmation();

  const handleProviderChange = (provider: string) => {
    setLlmProvider(provider);
  };

  useEffect(() => {
    if (!availableModels.length) return;
    const isValid = availableModels.some((model) => model.value === llmModel);
    if (!isValid) {
      setLlmModel(availableModels[0].value);
    }
  }, [availableModels, llmModel]);

  useEffect(() => {
    const fromQuery = getVerboseModeFromSearch(searchParams.toString());
    if (fromQuery) {
      setVerboseMode(true);
      saveVerboseMode(true);
    }
  }, [searchParams]);

  const handleVerboseToggle = (event: React.ChangeEvent<HTMLInputElement>) => {
    const enabled = event.target.checked;
    setVerboseMode(enabled);
    saveVerboseMode(enabled);
  };

  const handleClearHistory = async () => {
    if (messages.length === 0) return;
    const isConfirmed = await confirm({
      title: "Clear History",
      description: "Clear all messages and start a new conversation? This action cannot be undone.",
      confirmText: "Clear History",
      danger: true
    });
    if (!isConfirmed) return;

    setMessages([]);
    setFeedbackState({});
    setError(null);
    setExpandedSqlSections({});
    setRunStatus("idle");
    setCurrentPhase(null);
    setCompletedPhases([]);
    setCorrectionAttempt(0);
    threadIdRef.current = crypto.randomUUID();
  };

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!question.trim()) {
      return;
    }

    // Abort any previous in-flight stream
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    const prompt = question.trim();
    const request = {
      question: prompt,
      tenant_id: tenantId,
      thread_id: threadIdRef.current,
    };
    setQuestion("");
    setError(null);
    setCurrentPhase(null);
    setCompletedPhases([]);
    setCorrectionAttempt(0);

    setMessages((prev) => [...prev, { role: "user", text: prompt }]);

    setRunStatus("streaming");
    let didFail = false;
    try {
      // Try streaming first, fall back to blocking
      let finalResult: any = null;
      try {
        const stream = runAgentStream(request);
        const STREAM_TIMEOUT_MS = 30_000;

        // Iterate with per-event timeout and abort support
        const iterator = stream[Symbol.asyncIterator]();
        while (!controller.signal.aborted) {
          const next = await Promise.race([
            iterator.next(),
            new Promise<never>((_, reject) =>
              setTimeout(() => reject(new Error("STREAM_TIMEOUT")), STREAM_TIMEOUT_MS)
            ),
            new Promise<never>((_, reject) => {
              controller.signal.addEventListener("abort", () =>
                reject(new Error("ABORTED")), { once: true });
            }),
          ]);
          if (next.done) break;
          const evt = next.value;

          if (evt.event === "progress") {
            const nextPhase = evt.data.phase;
            // Track correction attempts
            if (nextPhase === "correct") {
              setCorrectionAttempt((prev) => prev + 1);
            }
            setCurrentPhase((prev) => {
              // Ignore out-of-order phases: only advance forward
              // Allow non-canonical phases (correct, clarify) to always show
              const prevIdx = prev ? phaseIndex(prev) : -1;
              const nextIdx = phaseIndex(nextPhase);
              if (nextIdx !== -1 && prevIdx !== -1 && nextIdx <= prevIdx) {
                return prev; // ignore regression
              }
              if (prev && prev !== nextPhase) {
                setCompletedPhases((cp) => cp.includes(prev) ? cp : [...cp, prev]);
              }
              return nextPhase;
            });
          } else if (evt.event === "error") {
            throw new Error(evt.data.error || "Unknown stream error");
          } else if (evt.event === "result") {
            finalResult = evt.data;
            // Lock stepper: mark all phases complete
            setCurrentPhase(null);
            setCompletedPhases([...PHASE_ORDER]);
          }
        }
      } catch (streamErr: any) {
        // Don't fall back if this run was aborted (new run started)
        if (controller.signal.aborted) return;
        // If stream endpoint fails (404, network, timeout), fall back to blocking runAgent
        if (!finalResult) {
          setRunStatus("finalizing");
          const result = await runAgent(request) as any;
          finalResult = result;
        } else {
          throw streamErr;
        }
      }

      if (finalResult) {
        setRunStatus("succeeded");
        setMessages((prev) => [
          ...prev,
          mapStreamResultToMessage(finalResult, request),
        ]);
      }
    } catch (err: unknown) {
      if (controller.signal.aborted) return;
      didFail = true;
      setRunStatus("failed");
      setError(buildErrorData(err));
    } finally {
      if (controller.signal.aborted) return;
      if (!didFail) setRunStatus("idle");
      setCurrentPhase(null);
      setCompletedPhases([]);
    }
  };


  const handlePreview = async () => {
    if (!question.trim()) return;

    abortRef.current?.abort();

    setIsGeneratingSQL(true);
    setError(null);

    try {
      const request: GenerateSQLRequest = {
        question: question.trim(),
        tenant_id: tenantId,
        thread_id: threadIdRef.current,
      };

      const response = await generateSQL(request);
      // TODO: Update generated types to include current_sql
      const currentSql = (response as any).current_sql;

      if (currentSql) {
        setPreviewData({
          sql: currentSql,
          originalRequest: request
        });
        setQuestion("");
      } else {
        throw new Error("No SQL generated by the agent.");
      }
    } catch (err: any) {
      setError(buildErrorData(err));
    } finally {
      setIsGeneratingSQL(false);
    }
  };

  const handleExecutePreview = async () => {
    if (!previewData) return;

    setRunStatus("streaming");
    setCurrentPhase("execute");
    setCompletedPhases(["router", "plan"]);

    try {
      const request: ExecuteSQLRequest = {
        question: previewData.originalRequest.question,
        sql: previewData.sql,
        tenant_id: previewData.originalRequest.tenant_id,
        thread_id: previewData.originalRequest.thread_id,
      };

      // Add user message to UI immediately
      setMessages(prev => [...prev, {
        role: "user",
        text: request.question,
        originalRequest: request as any
      }]);
      setPreviewData(null);

      const response = await executeSQL(request);

      const message = mapStreamResultToMessage(response, request as any);
      message.originalRequest = request as any; // Ensure correct request type for pagination

      setMessages(prev => [...prev, message]);
      setRunStatus("succeeded");
      setCurrentPhase(null);
      setCompletedPhases([...PHASE_ORDER]);

    } catch (err: any) {
      setError(buildErrorData(err));
      setRunStatus("failed");
      setCurrentPhase(null);
      setCompletedPhases([]);
    }
  };

  const handleRetry = () => {
    setError(null);
    // Re-submit with the last user message
    const lastUserMsg = [...messages].reverse().find((m) => m.role === "user");
    if (lastUserMsg?.text) {
      setQuestion(lastUserMsg.text);
    }
  };

  const handleLoadMore = async (msgIdx: number) => {
    const msg = messages[msgIdx];
    if (!msg?.resultCompleteness?.next_page_token || !msg.originalRequest) return;
    // Prevent concurrent pagination requests
    if (loadingMore !== null) return;

    setLoadingMore(msgIdx);
    try {
      let result;
      if ('sql' in msg.originalRequest) {
        result = await executeSQL({
          ...(msg.originalRequest as ExecuteSQLRequest),
          page_token: msg.resultCompleteness.next_page_token
        });
      } else {
        result = await runAgent({
          ...msg.originalRequest,
          page_token: msg.resultCompleteness.next_page_token,
        });
      }

      setMessages((prev) => {
        const updated = [...prev];
        const existing = updated[msgIdx];
        if (!existing) return prev;

        const existingRows = Array.isArray(existing.result) ? existing.result : [];
        const newRows = Array.isArray(result.result) ? result.result : [];

        // Check for column mismatch between pages
        if (existingRows.length > 0 && newRows.length > 0) {
          const existingCols = Object.keys(existingRows[0] || {}).sort().join(",");
          const newCols = Object.keys(newRows[0] || {}).sort().join(",");
          if (existingCols !== newCols) {
            // Schema mismatch — don't append, show warning
            updated[msgIdx] = {
              ...existing,
              resultCompleteness: {
                ...existing.resultCompleteness,
                next_page_token: undefined,
                schema_mismatch: true,
              },
            };
            return updated;
          }
        }

        const dedupedNewRows = dedupeRows(existingRows, newRows);
        const combinedRows = [...existingRows, ...dedupedNewRows];
        const nextCompleteness = (result as any)?.result_completeness ?? {};
        updated[msgIdx] = {
          ...existing,
          result: combinedRows,
          resultCompleteness: {
            ...existing.resultCompleteness,
            ...nextCompleteness,
            rows_returned: combinedRows.length,
            next_page_token: nextCompleteness.next_page_token,
            token_expired: nextCompleteness.token_expired ?? false,
            schema_mismatch: nextCompleteness.schema_mismatch ?? false,
          },
        };
        return updated;
      });
    } catch (err: unknown) {
      // Token expired/invalid — replace button with re-run CTA
      const errData = buildErrorData(err);
      const isTokenError =
        errData.category === "invalid_request" ||
        (errData.message && /token|expired|invalid/i.test(errData.message));

      if (isTokenError) {
        setMessages((prev) => {
          const updated = [...prev];
          const existing = updated[msgIdx];
          if (!existing) return prev;
          updated[msgIdx] = {
            ...existing,
            resultCompleteness: {
              ...existing.resultCompleteness,
              next_page_token: undefined,
              token_expired: true,
            },
          };
          return updated;
        });
      } else {
        setError(errData);
      }
    } finally {
      setLoadingMore(null);
    }
  };

  const handleFeedback = async (
    interactionId: string,
    thumb: "UP" | "DOWN",
    comment?: string
  ) => {
    try {
      await submitFeedback({ interaction_id: interactionId, thumb, comment });
      setFeedbackState((prev) => ({ ...prev, [interactionId]: "done" }));
    } catch (err: any) {
      setFeedbackState((prev) => ({
        ...prev,
        [interactionId]: `error:${err.message || "Failed"}`
      }));
    }
  };

  const mcpUrl = import.meta.env.VITE_AGENT_SERVICE_URL || "http://localhost:8081";

  return (
    <>
      <header className="hero">
        <div>
          <p className="kicker">Text2SQL</p>
          <h1>Ask in natural language. Get SQL with confidence.</h1>
          <p className="subtitle">
            This React client mirrors the Streamlit chat while the agent continues to run
            server-side.
          </p>
          <label
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: "10px",
              marginTop: "12px",
              fontWeight: 600
            }}
          >
            <input
              type="checkbox"
              checked={verboseMode}
              onChange={handleVerboseToggle}
            />
            Verbose / Diagnostic View
          </label>
        </div>
      </header>

      <div style={{ display: "grid", gridTemplateColumns: "280px 1fr", gap: "32px" }}>
        {/* Sidebar */}
        <aside>
          <div className="panel" style={{ marginBottom: "16px" }}>
            <label>
              Tenant ID
              <input
                type="number"
                min="1"
                value={tenantId}
                onChange={(event) => setTenantId(Number(event.target.value))}
              />
            </label>
          </div>

          <div className="panel" style={{ marginBottom: "16px" }}>
            <label style={{ marginBottom: "12px" }}>
              LLM Provider
              <select
                value={llmProvider}
                onChange={(e) => handleProviderChange(e.target.value)}
                style={{
                  width: "100%",
                  padding: "10px 12px",
                  borderRadius: "10px",
                  border: "1px solid var(--border)",
                  fontSize: "1rem",
                  marginTop: "6px"
                }}
              >
                {LLM_PROVIDERS.map((p) => (
                  <option key={p.value} value={p.value}>
                    {p.label}
                  </option>
                ))}
              </select>
            </label>

            <label style={{ marginTop: "16px" }}>
              Model
              <select
                value={llmModel}
                onChange={(e) => setLlmModel(e.target.value)}
                disabled={modelsLoading}
                style={{
                  width: "100%",
                  padding: "10px 12px",
                  borderRadius: "10px",
                  border: "1px solid var(--border)",
                  fontSize: "1rem",
                  marginTop: "6px"
                }}
              >
                {availableModels.map((m) => (
                  <option key={m.value} value={m.value}>
                    {m.label}
                  </option>
                ))}
              </select>
            </label>
            {modelsError && (
              <div style={{ marginTop: "8px", fontSize: "0.8rem", color: "var(--muted)" }}>
                Unable to refresh models. Using fallback list.
              </div>
            )}
          </div>

          <div className="panel" style={{ marginBottom: "16px" }}>
            <div style={{ fontSize: "0.75rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "var(--muted)", marginBottom: "12px" }}>
              System Status
            </div>
            <div style={{ fontSize: "0.85rem", display: "grid", gap: "8px" }}>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--muted)" }}>Provider</span>
                <strong>{LLM_PROVIDERS.find((p) => p.value === llmProvider)?.label}</strong>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--muted)" }}>Model</span>
                <strong style={{ fontSize: "0.8rem" }}>{llmModel}</strong>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--muted)" }}>MCP URL</span>
                <strong style={{ fontSize: "0.75rem", wordBreak: "break-all" }}>{mcpUrl.replace(/^https?:\/\//, "")}</strong>
              </div>
            </div>
          </div>

          <div className="panel">
            <div className="meta" style={{ marginTop: 0 }}>
              <span>Thread</span>
              <strong>{threadIdRef.current.slice(0, 8)}</strong>
            </div>
            <button
              type="button"
              onClick={handleClearHistory}
              disabled={messages.length === 0}
              style={{
                width: "100%",
                marginTop: "12px",
                padding: "10px",
                borderRadius: "8px",
                border: "1px solid var(--border)",
                background: messages.length === 0 ? "var(--surface-muted)" : "var(--surface)",
                cursor: messages.length === 0 ? "not-allowed" : "pointer",
                color: messages.length === 0 ? "var(--muted)" : "var(--ink)",
                fontWeight: 500
              }}
            >
              Clear History
            </button>
          </div>
        </aside>

        {/* Main chat area */}
        <main>
          {configStatus === "unconfigured" && (
            <div className="panel" data-testid="onboarding-panel" style={{
              marginBottom: "24px",
              padding: "32px",
              textAlign: "center",
              background: "rgba(99, 102, 241, 0.05)",
              border: "1px solid rgba(99, 102, 241, 0.2)",
              borderRadius: "12px",
            }}>
              <h2 style={{ margin: "0 0 8px", fontSize: "1.3rem" }}>Welcome to Text2SQL</h2>
              <p style={{ color: "var(--muted)", marginBottom: "20px" }}>
                Before you can start querying, you need to configure a data source.
              </p>
              <div style={{ display: "flex", gap: "12px", justifyContent: "center" }}>
                <Link
                  to="/admin/settings/query-target"
                  style={{
                    padding: "10px 20px",
                    borderRadius: "10px",
                    background: "var(--accent, #6366f1)",
                    color: "#fff",
                    textDecoration: "none",
                    fontWeight: 600,
                  }}
                >
                  Configure Data Source
                </Link>
                <Link
                  to="/admin/operations"
                  style={{
                    padding: "10px 20px",
                    borderRadius: "10px",
                    border: "1px solid var(--border)",
                    background: "var(--surface, #fff)",
                    color: "var(--ink)",
                    textDecoration: "none",
                    fontWeight: 500,
                  }}
                >
                  System Operations
                </Link>
              </div>
              <button
                type="button"
                data-testid="refresh-config-button"
                onClick={checkConfig}
                disabled={isCheckingConfig}
                style={{
                  marginTop: "16px",
                  background: "none",
                  border: "none",
                  color: isCheckingConfig ? "var(--muted)" : "var(--muted)",
                  fontSize: "0.8rem",
                  cursor: isCheckingConfig ? "default" : "pointer",
                  textDecoration: "underline",
                }}
              >
                {isCheckingConfig ? "Checking..." : "Refresh status"}
              </button>
            </div>
          )}

          <section className="chat">
            {messages.map((msg, idx) => (
              <article
                key={`${msg.role}-${idx}`}
                className={`bubble ${msg.role} animate-in`}
              >
                <div className="bubble-header">
                  <span>{msg.role === "user" ? "You" : "Assistant"}</span>
                </div>
                <div className="bubble-body">
                  {msg.text && <p>{msg.text}</p>}
                  {msg.error && <p className="error">Error: {msg.error}</p>}

                  {msg.sql && (
                    <details
                      open={Boolean(expandedSqlSections[idx])}
                      onToggle={(event) => {
                        const nextOpen = (event.currentTarget as HTMLDetailsElement).open;
                        setExpandedSqlSections((prev) => ({ ...prev, [idx]: nextOpen }));
                      }}
                    >
                      <summary>Generated SQL</summary>
                      <pre>{msg.sql}</pre>
                      <SQLValidationDetails summary={msg.validationSummary} report={msg.validationReport} />
                    </details>
                  )}

                  {Array.isArray(msg.result) && <ResultsTable rows={msg.result} />}
                  {msg.result && !Array.isArray(msg.result) && (
                    <pre className="result-block">{formatValue(msg.result)}</pre>
                  )}

                  {msg.emptyResultGuidance && (
                    <div className="guidance-callout" style={{
                      marginTop: "12px",
                      padding: "12px",
                      borderRadius: "8px",
                      background: "rgba(255, 193, 7, 0.05)",
                      border: "1px solid rgba(255, 193, 7, 0.2)",
                      fontSize: "0.9rem",
                      display: "flex",
                      gap: "10px",
                      alignItems: "flex-start"
                    }}>
                      <span style={{ fontSize: "1.2rem" }}>💡</span>
                      <div>
                        <strong style={{ display: "block", marginBottom: "4px", color: "#856404" }}>Note</strong>
                        <span style={{ color: "#856404" }}>{msg.emptyResultGuidance}</span>
                      </div>
                    </div>
                  )}

                  {msg.role === "assistant" && (msg.validationSummary || msg.validationReport || msg.resultCompleteness) && (
                    <ValidationCompletenessSummary
                      summary={msg.validationSummary}
                      report={msg.validationReport}
                      completeness={msg.resultCompleteness}
                      onExpand={() => setExpandedSqlSections((prev) => ({ ...prev, [idx]: true }))}
                    />
                  )}
                  {msg.role === "assistant" && msg.sql && (
                    <div style={{ marginTop: "8px", display: "flex", justifyContent: "flex-end" }}>
                      <CopyBundleButton message={msg} />
                    </div>
                  )}
                  <ResultCompletenessBanner completeness={msg.resultCompleteness} />
                  {msg.resultCompleteness?.next_page_token && msg.originalRequest && (
                    <button
                      type="button"
                      data-testid="load-more-button"
                      onClick={() => handleLoadMore(idx)}
                      disabled={loadingMore === idx}
                      style={{
                        marginTop: "8px",
                        padding: "8px 16px",
                        borderRadius: "8px",
                        border: "1px solid var(--border)",
                        background: "var(--surface, #fff)",
                        color: "var(--accent, #6366f1)",
                        cursor: loadingMore === idx ? "not-allowed" : "pointer",
                        fontWeight: 500,
                        fontSize: "0.85rem",
                      }}
                    >
                      {loadingMore === idx ? "Loading..." : "Load more rows"}
                    </button>
                  )}
                  <div style={{ display: "flex", alignItems: "center", gap: "12px", flexWrap: "wrap" }}>
                    <RetrySummaryBadge summary={msg.retrySummary} />
                    <ValidationSummaryBadge summary={msg.validationSummary} />
                  </div>
                  {msg.role === "assistant" && (msg.traceId || msg.interactionId || msg.requestId) && (
                    <div style={{ marginTop: "10px", display: "flex", alignItems: "center", gap: "8px", flexWrap: "wrap" }}>
                      <RunIdentifiers
                        traceId={msg.traceId}
                        interactionId={msg.interactionId}
                        requestId={msg.requestId}
                      />
                      {verboseMode && (msg.traceId || msg.interactionId) && (
                        <a
                          href={
                            msg.traceId
                              ? `/traces/${msg.traceId}?verbose=1`
                              : msg.interactionId
                                ? `/traces/interaction/${msg.interactionId}?verbose=1`
                                : undefined
                          }
                          className="trace-link__btn"
                          onClick={(event) => event.stopPropagation()}
                        >
                          Verbose Trace
                        </a>
                      )}
                    </div>
                  )}
                  {msg.role === "assistant" && <DecisionLogPanel events={msg.decisionEvents} />}

                  {msg.vizSpec && (
                    <div style={{ marginTop: "16px" }}>
                      {(() => {
                        const isSchema =
                          msg.vizSpec &&
                          typeof msg.vizSpec === "object" &&
                          "chartType" in msg.vizSpec;
                        const schema =
                          isSchema && Array.isArray((msg.vizSpec as ChartSchema).series)
                            ? (msg.vizSpec as ChartSchema)
                            : undefined;

                        if (!isSchema || (isSchema && !schema)) {
                          return (
                            <>
                              <ErrorState error="Invalid chart schema from agent." />
                              <pre className="result-block">
                                {formatValue(msg.vizSpec)}
                              </pre>
                            </>
                          );
                        }

                        return <ChartRenderer schema={schema} />;
                      })()}
                    </div>
                  )}

                  {msg.fromCache && (
                    <div className="pill" style={{ marginTop: "8px" }}>
                      {msg.cacheSimilarity != null
                        ? `From cache (similarity ≥ ${formatSimilarity(msg.cacheSimilarity)}%)`
                        : "From cache"}
                    </div>
                  )}
                </div>

                {msg.role === "assistant" && msg.interactionId && (
                  <div className="feedback">
                    {feedbackState[msg.interactionId] === "done" ? (
                      <span className="pill">Feedback submitted</span>
                    ) : (
                      <>
                        <button
                          type="button"
                          onClick={() => handleFeedback(msg.interactionId!, "UP")}
                        >
                          Helpful
                        </button>
                        <button
                          type="button"
                          onClick={() =>
                            setFeedbackState((prev) => ({
                              ...prev,
                              [msg.interactionId!]: "comment"
                            }))
                          }
                        >
                          Needs work
                        </button>
                        {feedbackState[msg.interactionId] === "comment" && (
                          <FeedbackForm
                            onSubmit={(comment) =>
                              handleFeedback(msg.interactionId!, "DOWN", comment)
                            }
                          />
                        )}
                        {feedbackState[msg.interactionId]?.startsWith("error") && (
                          <span className="error">Feedback failed</span>
                        )}
                      </>
                    )}
                  </div>
                )}
              </article>
            ))}

            {isLoading && (
              <div style={{ display: "flex", justifyContent: "center", marginBottom: "16px" }}>
                <ExecutionProgress currentPhase={currentPhase} completedPhases={completedPhases} correctionAttempt={correctionAttempt} />
              </div>
            )}
            {error && (
              <div style={{ marginBottom: "16px" }}>
                <ErrorCard {...error} onRetry={handleRetry} />
              </div>
            )}
          </section>

          {previewData ? (
            <div className="composer-preview-wrapper" style={{ marginTop: "auto" }}>
              <SQLPreviewCard
                sql={previewData.sql}
                isExecuting={runStatus === "streaming"}
                onRun={handleExecutePreview}
                onBack={() => {
                  setQuestion(previewData.originalRequest.question);
                  setPreviewData(null);
                }}
                onSqlChange={(newSql) => setPreviewData(prev => prev ? { ...prev, sql: newSql } : null)}
              />
            </div>
          ) : (
            <form className="composer" onSubmit={handleSubmit}>
              <input
                type="text"
                placeholder={configStatus === "unconfigured" ? "Configure a data source first" : "Ask a question about your data"}
                value={question}
                onChange={(event) => setQuestion(event.target.value)}
                disabled={configStatus === "unconfigured" || isGeneratingSQL}
              />
              <button
                type="button"
                onClick={handlePreview}
                disabled={isLoading || configStatus === "unconfigured" || isGeneratingSQL || !question.trim()}
                style={{ whiteSpace: "nowrap", background: "var(--surface)", color: "var(--ink)", border: "1px solid var(--border)" }}
              >
                {isGeneratingSQL ? "Generating..." : "Preview"}
              </button>
              <button type="submit" disabled={isLoading || configStatus === "unconfigured" || isGeneratingSQL}>
                {isLoading ? "Running..." : "Run"}
              </button>
            </form>
          )}
        </main>
      </div>
      <ConfirmationDialog {...dialogProps} />
    </>
  );
}

function FeedbackForm({ onSubmit }: { onSubmit: (comment: string) => void }) {
  const [value, setValue] = useState<string>("");

  return (
    <div className="feedback-form">
      <textarea
        value={value}
        placeholder="Tell us what went wrong"
        onChange={(event) => setValue(event.target.value)}
      />
      <button
        type="button"
        onClick={() => {
          onSubmit(value);
          setValue("");
        }}
      >
        Send feedback
      </button>
    </div>
  );
}
