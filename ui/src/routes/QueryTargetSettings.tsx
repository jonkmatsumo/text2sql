import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  activateQueryTargetSettings,
  fetchQueryTargetSettings,
  getErrorMessage,
  QueryTargetConfigPayload,
  QueryTargetConfigResponse,
  QueryTargetTestResponse,
  testQueryTargetSettings,
  upsertQueryTargetSettings
} from "../api";
import { useToast } from "../hooks/useToast";

const providerOptions = [
  { value: "postgres", label: "Postgres" },
  { value: "cockroachdb", label: "CockroachDB" },
  { value: "mysql", label: "MySQL / MariaDB" },
  { value: "redshift", label: "Redshift" },
  { value: "sqlite", label: "SQLite" },
  { value: "duckdb", label: "DuckDB" },
  { value: "snowflake", label: "Snowflake" },
  { value: "bigquery", label: "BigQuery" },
  { value: "athena", label: "Athena" },
  { value: "databricks", label: "Databricks" },
  { value: "clickhouse", label: "ClickHouse" }
];

const metadataFields: Record<string, Array<{ key: string; label: string; type?: string; placeholder?: string }>> = {
  postgres: [
    { key: "host", label: "Host", placeholder: "db.example.com" },
    { key: "port", label: "Port", type: "number", placeholder: "5432" },
    { key: "db_name", label: "Database", placeholder: "analytics" },
    { key: "user", label: "User", placeholder: "readonly" }
  ],
  cockroachdb: [
    { key: "host", label: "Host", placeholder: "crdb.example.com" },
    { key: "port", label: "Port", type: "number", placeholder: "26257" },
    { key: "db_name", label: "Database", placeholder: "analytics" },
    { key: "user", label: "User", placeholder: "readonly" }
  ],
  mysql: [
    { key: "host", label: "Host", placeholder: "db.example.com" },
    { key: "port", label: "Port", type: "number", placeholder: "3306" },
    { key: "db_name", label: "Database", placeholder: "analytics" },
    { key: "user", label: "User", placeholder: "readonly" }
  ],
  redshift: [
    { key: "host", label: "Host", placeholder: "cluster.redshift.amazonaws.com" },
    { key: "port", label: "Port", type: "number", placeholder: "5439" },
    { key: "db_name", label: "Database", placeholder: "dev" },
    { key: "user", label: "User", placeholder: "awsuser" }
  ],
  sqlite: [{ key: "path", label: "DB Path", placeholder: "./local-data/query-target.sqlite" }],
  duckdb: [{ key: "path", label: "DB Path", placeholder: ":memory:" }],
  snowflake: [
    { key: "account", label: "Account", placeholder: "xy12345.us-east-1" },
    { key: "user", label: "User", placeholder: "readonly" },
    { key: "warehouse", label: "Warehouse", placeholder: "ANALYTICS_WH" },
    { key: "database", label: "Database", placeholder: "ANALYTICS" },
    { key: "schema", label: "Schema", placeholder: "PUBLIC" },
    { key: "role", label: "Role", placeholder: "ANALYST" },
    { key: "authenticator", label: "Authenticator", placeholder: "externalbrowser" }
  ],
  bigquery: [
    { key: "project", label: "Project", placeholder: "my-gcp-project" },
    { key: "dataset", label: "Dataset", placeholder: "analytics" },
    { key: "location", label: "Location", placeholder: "US" }
  ],
  athena: [
    { key: "region", label: "Region", placeholder: "us-east-1" },
    { key: "workgroup", label: "Workgroup", placeholder: "primary" },
    { key: "output_location", label: "Output S3 URI", placeholder: "s3://bucket/path/" },
    { key: "database", label: "Database", placeholder: "default" }
  ],
  databricks: [
    { key: "host", label: "Workspace Host", placeholder: "https://workspace.cloud.databricks.com" },
    { key: "warehouse_id", label: "Warehouse ID", placeholder: "warehouse-id" },
    { key: "catalog", label: "Catalog", placeholder: "main" },
    { key: "schema", label: "Schema", placeholder: "default" }
  ],
  clickhouse: [
    { key: "host", label: "Host", placeholder: "clickhouse.local" },
    { key: "port", label: "Port", type: "number", placeholder: "9000" },
    { key: "database", label: "Database", placeholder: "default" },
    { key: "user", label: "User", placeholder: "default" },
    { key: "secure", label: "Secure (TLS)", type: "checkbox" }
  ]
};

const guardrailFields: Record<string, Array<{ key: string; label: string; type?: string }>> = {
  postgres: [{ key: "max_rows", label: "Max Rows", type: "number" }],
  cockroachdb: [{ key: "max_rows", label: "Max Rows", type: "number" }],
  mysql: [{ key: "max_rows", label: "Max Rows", type: "number" }],
  redshift: [{ key: "max_rows", label: "Max Rows", type: "number" }],
  sqlite: [{ key: "max_rows", label: "Max Rows", type: "number" }],
  duckdb: [
    { key: "max_rows", label: "Max Rows", type: "number" },
    { key: "query_timeout_seconds", label: "Query Timeout (sec)", type: "number" },
    { key: "read_only", label: "Read Only", type: "checkbox" }
  ],
  snowflake: [
    { key: "max_rows", label: "Max Rows", type: "number" },
    { key: "query_timeout_seconds", label: "Query Timeout (sec)", type: "number" },
    { key: "poll_interval_seconds", label: "Poll Interval (sec)", type: "number" },
    { key: "warn_after_seconds", label: "Warn After (sec)", type: "number" }
  ],
  bigquery: [
    { key: "max_rows", label: "Max Rows", type: "number" },
    { key: "query_timeout_seconds", label: "Query Timeout (sec)", type: "number" },
    { key: "poll_interval_seconds", label: "Poll Interval (sec)", type: "number" }
  ],
  athena: [
    { key: "max_rows", label: "Max Rows", type: "number" },
    { key: "query_timeout_seconds", label: "Query Timeout (sec)", type: "number" },
    { key: "poll_interval_seconds", label: "Poll Interval (sec)", type: "number" }
  ],
  databricks: [
    { key: "max_rows", label: "Max Rows", type: "number" },
    { key: "query_timeout_seconds", label: "Query Timeout (sec)", type: "number" },
    { key: "poll_interval_seconds", label: "Poll Interval (sec)", type: "number" }
  ],
  clickhouse: [
    { key: "max_rows", label: "Max Rows", type: "number" },
    { key: "query_timeout_seconds", label: "Query Timeout (sec)", type: "number" }
  ]
};

const defaultPayload = (provider: string): QueryTargetConfigPayload => ({
  provider,
  metadata: {},
  auth: {},
  guardrails: {}
});

const pillStyle = (status?: string) => ({
  display: "inline-flex",
  padding: "4px 10px",
  borderRadius: "999px",
  fontSize: "0.75rem",
  fontWeight: 600,
  textTransform: "capitalize" as const,
  backgroundColor: status === "active" ? "#10b98133" : status === "pending" ? "#f59e0b33" : "#47556922",
  color: status === "active" ? "#10b981" : status === "pending" ? "#f59e0b" : "#94a3b8"
});

const errorGuidanceByCategory: Record<string, string> = {
  auth: "Verify the secret reference resolves to valid credentials for this provider.",
  connectivity: "Check network access, host, and port from the backend environment.",
  timeout: "The provider timed out. Try again or reduce the scope of the test.",
  resource_exhausted: "The provider reported resource limits. Try again later or adjust capacity.",
  syntax: "Review any SQL or identifiers for syntax issues.",
  unsupported: "This provider or capability is not supported in this environment.",
  transient: "The provider appears unavailable. Retry once the service is healthy.",
  unknown: "Review provider logs or connection details for more context."
};

export default function QueryTargetSettings() {
  const { show: showToast } = useToast();
  const [settings, setSettings] = useState<{ active?: QueryTargetConfigResponse | null; pending?: QueryTargetConfigResponse | null }>({});
  const [form, setForm] = useState<QueryTargetConfigPayload>(defaultPayload("postgres"));
  const [configId, setConfigId] = useState<string | undefined>(undefined);
  const [isLoading, setIsLoading] = useState(false);
  const [lastTestResult, setLastTestResult] = useState<QueryTargetTestResponse | null>(null);

  const currentProvider = form.provider;
  const metadataConfig = metadataFields[currentProvider] || [];
  const guardrailConfig = guardrailFields[currentProvider] || [];

  const loadSettings = useCallback(async () => {
    setIsLoading(true);
    try {
      const data = await fetchQueryTargetSettings();
      setSettings(data);
      const candidate = data.pending || data.active;
      if (candidate) {
        setForm({
          provider: candidate.provider,
          metadata: { ...candidate.metadata },
          auth: { ...candidate.auth },
          guardrails: { ...candidate.guardrails },
          config_id: candidate.id
        });
        setConfigId(candidate.id);
      }
    } catch (err) {
      showToast(getErrorMessage(err), "error");
    } finally {
      setIsLoading(false);
    }
  }, [showToast]);

  useEffect(() => {
    loadSettings();
  }, [loadSettings]);

  const updateMetadata = (key: string, value: unknown) => {
    setForm((prev) => ({
      ...prev,
      metadata: { ...prev.metadata, [key]: value }
    }));
  };

  const updateAuth = (key: string, value: unknown) => {
    setForm((prev) => ({
      ...prev,
      auth: { ...prev.auth, [key]: value }
    }));
  };

  const updateGuardrails = (key: string, value: unknown) => {
    setForm((prev) => ({
      ...prev,
      guardrails: { ...prev.guardrails, [key]: value }
    }));
  };

  const onProviderChange = (value: string) => {
    setForm(defaultPayload(value));
    setConfigId(undefined);
    setLastTestResult(null);
  };

  const handleSave = async () => {
    setIsLoading(true);
    try {
      const payload = {
        ...form,
        config_id: configId
      };
      const record = await upsertQueryTargetSettings(payload);
      setConfigId(record.id);
      setLastTestResult(null);
      setSettings((prev) => ({
        ...prev,
        pending: record.status === "pending" ? record : prev.pending,
        active: record.status === "active" ? record : prev.active
      }));
      showToast("Query-target settings saved", "success");
    } catch (err) {
      showToast(getErrorMessage(err), "error");
    } finally {
      setIsLoading(false);
    }
  };

  const handleTest = async () => {
    setIsLoading(true);
    try {
      const payload = {
        ...form,
        config_id: configId
      };
      const result = await testQueryTargetSettings(payload);
      setLastTestResult(result.ok ? null : result);
      if (result.ok) {
        showToast("Connection test passed", "success");
      } else {
        showToast(result.error_message || "Connection test failed", "error");
      }
      await loadSettings();
    } catch (err) {
      showToast(getErrorMessage(err), "error");
    } finally {
      setIsLoading(false);
    }
  };

  const handleActivate = async () => {
    if (!configId) {
      showToast("Save settings before activating", "error");
      return;
    }
    setIsLoading(true);
    try {
      const record = await activateQueryTargetSettings(configId);
      setSettings((prev) => ({
        ...prev,
        pending: record,
        active: prev.active
      }));
      showToast("Activation queued. Restart required.", "info");
    } catch (err) {
      showToast(getErrorMessage(err), "error");
    } finally {
      setIsLoading(false);
    }
  };

  const authFields = useMemo(() => {
    return [
      { key: "secret_ref", label: "Secret Reference", placeholder: "env:DB_PASS" },
      { key: "identity_profile", label: "Identity Profile", placeholder: "aws-profile" }
    ];
  }, []);

  const latestError = useMemo(() => {
    if (lastTestResult && !lastTestResult.ok) {
      return {
        message: lastTestResult.error_message,
        code: lastTestResult.error_code,
        category: lastTestResult.error_category
      };
    }
    const record = settings.pending || settings.active;
    if (!record) return null;
    if (!record.last_error_message && !record.last_error_code) {
      return null;
    }
    return {
      message: record.last_error_message,
      code: record.last_error_code,
      category: record.last_error_category
    };
  }, [lastTestResult, settings]);

  const errorGuidance = latestError?.category
    ? errorGuidanceByCategory[latestError.category] || null
    : null;

  return (
    <>
      <header className="hero">
        <div>
          <p className="kicker">Settings</p>
          <h1>Query Target</h1>
          <p className="subtitle">
            Configure the data source for query execution. Changes require a backend restart to take effect.
          </p>
        </div>
      </header>

      <section style={{ display: "grid", gap: "16px", marginBottom: "32px" }}>
        <div style={{ display: "flex", gap: "16px", flexWrap: "wrap" }}>
          <div style={{ padding: "16px", borderRadius: "16px", border: "1px solid var(--border)", minWidth: "240px" }}>
            <div className="kicker">Active</div>
            <div style={{ display: "flex", alignItems: "center", gap: "8px", marginTop: "6px" }}>
              <div style={{ fontWeight: 600 }}>{settings.active?.provider || "None"}</div>
              <span style={pillStyle(settings.active?.status)}>{settings.active?.status || "inactive"}</span>
            </div>
          </div>
          <div style={{ padding: "16px", borderRadius: "16px", border: "1px solid var(--border)", minWidth: "240px" }}>
            <div className="kicker">Pending</div>
            <div style={{ display: "flex", alignItems: "center", gap: "8px", marginTop: "6px" }}>
              <div style={{ fontWeight: 600 }}>{settings.pending?.provider || "None"}</div>
              <span style={pillStyle(settings.pending?.status)}>{settings.pending?.status || "inactive"}</span>
            </div>
            <div style={{ marginTop: "6px", color: "var(--muted)", fontSize: "0.85rem" }}>
              Restart required to apply pending config.
            </div>
          </div>
        </div>
      </section>

      <section style={{ display: "grid", gap: "24px" }}>
        <div style={{ padding: "24px", borderRadius: "20px", border: "1px solid var(--border)", background: "var(--surface)" }}>
          <div style={{ marginBottom: "16px" }}>
            <div className="kicker">Provider</div>
            <select
              value={currentProvider}
              onChange={(event) => onProviderChange(event.target.value)}
              style={{ marginTop: "8px", padding: "10px 12px", borderRadius: "10px", border: "1px solid var(--border)", width: "100%" }}
            >
              {providerOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>

          <div style={{ display: "grid", gap: "16px" }}>
            <div>
              <div className="kicker">Metadata</div>
              <div style={{ display: "grid", gap: "12px", marginTop: "8px" }}>
                {metadataConfig.map((field) => {
                  if (field.type === "checkbox") {
                    return (
                      <label key={field.key} style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                        <input
                          type="checkbox"
                          checked={Boolean(form.metadata[field.key])}
                          onChange={(event) => updateMetadata(field.key, event.target.checked)}
                        />
                        {field.label}
                      </label>
                    );
                  }
                  return (
                    <label key={field.key} style={{ display: "grid", gap: "6px" }}>
                      <span style={{ fontSize: "0.85rem", color: "var(--muted)" }}>{field.label}</span>
                      <input
                        type={field.type || "text"}
                        value={(form.metadata[field.key] as string) || ""}
                        placeholder={field.placeholder}
                        onChange={(event) => {
                          const value = field.type === "number" ? Number(event.target.value) : event.target.value;
                          updateMetadata(field.key, event.target.value === "" ? "" : value);
                        }}
                        style={{ padding: "10px 12px", borderRadius: "10px", border: "1px solid var(--border)" }}
                      />
                    </label>
                  );
                })}
              </div>
            </div>

            <div>
              <div className="kicker">Auth (references only)</div>
              <div style={{ display: "grid", gap: "12px", marginTop: "8px" }}>
                {authFields.map((field) => (
                  <label key={field.key} style={{ display: "grid", gap: "6px" }}>
                    <span style={{ fontSize: "0.85rem", color: "var(--muted)" }}>{field.label}</span>
                    <input
                      type="text"
                      value={(form.auth[field.key] as string) || ""}
                      placeholder={field.placeholder}
                      onChange={(event) => updateAuth(field.key, event.target.value)}
                      style={{ padding: "10px 12px", borderRadius: "10px", border: "1px solid var(--border)" }}
                    />
                  </label>
                ))}
              </div>
            </div>

            <div>
              <div className="kicker">Guardrails</div>
              <div style={{ display: "grid", gap: "12px", marginTop: "8px" }}>
                {guardrailConfig.map((field) => {
                  if (field.type === "checkbox") {
                    return (
                      <label key={field.key} style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                        <input
                          type="checkbox"
                          checked={Boolean(form.guardrails[field.key])}
                          onChange={(event) => updateGuardrails(field.key, event.target.checked)}
                        />
                        {field.label}
                      </label>
                    );
                  }
                  return (
                    <label key={field.key} style={{ display: "grid", gap: "6px" }}>
                      <span style={{ fontSize: "0.85rem", color: "var(--muted)" }}>{field.label}</span>
                      <input
                        type={field.type || "text"}
                        value={form.guardrails[field.key] !== undefined ? String(form.guardrails[field.key]) : ""}
                        onChange={(event) => {
                          const value = event.target.value === "" ? undefined : Number(event.target.value);
                          updateGuardrails(field.key, value);
                        }}
                        style={{ padding: "10px 12px", borderRadius: "10px", border: "1px solid var(--border)" }}
                      />
                    </label>
                  );
                })}
              </div>
            </div>
          </div>

          <div style={{ display: "flex", gap: "12px", marginTop: "24px" }}>
            <button className="btn" onClick={handleSave} disabled={isLoading}>
              Save Settings
            </button>
            <button className="btn secondary" onClick={handleTest} disabled={isLoading}>
              Test Connection
            </button>
            <button className="btn ghost" onClick={handleActivate} disabled={isLoading}>
              Activate (Restart Required)
            </button>
          </div>
          {latestError && (
            <div className="error-banner" style={{ marginTop: "16px" }}>
              <div style={{ fontWeight: 600, marginBottom: "4px" }}>Connection error</div>
              <div>{latestError.message || "Connection test failed."}</div>
              {latestError.code && (
                <div style={{ marginTop: "4px", color: "var(--muted)" }}>Code: {latestError.code}</div>
              )}
              {errorGuidance && (
                <div style={{ marginTop: "6px", color: "var(--muted)" }}>Guidance: {errorGuidance}</div>
              )}
            </div>
          )}
        </div>
      </section>
    </>
  );
}
