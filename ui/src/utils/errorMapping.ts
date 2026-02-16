/**
 * Single source of truth for mapping backend ErrorCategory values
 * to UI labels, severity styles, and suggested CTAs.
 */

export type ErrorSeverity = "info" | "warn" | "error";

export interface ErrorMapping {
  title: string;
  severity: ErrorSeverity;
  actions: Array<{ label: string; href: string }>;
}

const ERROR_MAP: Record<string, ErrorMapping> = {
  schema_drift: {
    title: "Schema Mismatch",
    severity: "error",
    actions: [{ label: "Open Ingestion Wizard", href: "/admin/operations" }],
  },
  schema_missing: {
    title: "Schema Not Found",
    severity: "error",
    actions: [{ label: "Open Ingestion Wizard", href: "/admin/operations" }],
  },
  auth: {
    title: "Authentication Error",
    severity: "error",
    actions: [{ label: "Check Permissions", href: "/admin/settings/query-target" }],
  },
  unauthorized: {
    title: "Unauthorized",
    severity: "error",
    actions: [{ label: "Check Permissions", href: "/admin/settings/query-target" }],
  },
  permission_denied: {
    title: "Permission Denied",
    severity: "error",
    actions: [{ label: "Check Permissions", href: "/admin/settings/query-target" }],
  },
  connectivity: {
    title: "Connection Error",
    severity: "error",
    actions: [{ label: "Configure Data Source", href: "/admin/settings/query-target" }],
  },
  timeout: {
    title: "Timeout",
    severity: "warn",
    actions: [],
  },
  limit_exceeded: {
    title: "Limit Exceeded",
    severity: "warn",
    actions: [],
  },
  llm_rate_limit_exceeded: {
    title: "LLM Rate Limit",
    severity: "warn",
    actions: [],
  },
  throttling: {
    title: "Rate Limited",
    severity: "warn",
    actions: [],
  },
  invalid_request: {
    title: "Invalid Request",
    severity: "info",
    actions: [],
  },
  tool_response_malformed: {
    title: "Malformed Response",
    severity: "error",
    actions: [],
  },
  syntax: {
    title: "SQL Syntax Error",
    severity: "warn",
    actions: [],
  },
  budget_exhausted: {
    title: "Budget Exhausted",
    severity: "warn",
    actions: [{ label: "View Quotas", href: "/admin/settings/query-target" }],
  },
  budget_exceeded: {
    title: "Budget Exceeded",
    severity: "warn",
    actions: [{ label: "View Quotas", href: "/admin/settings/query-target" }],
  },
  resource_exhausted: {
    title: "Resource Exhausted",
    severity: "error",
    actions: [{ label: "System Operations", href: "/admin/operations" }],
  },
  mutation_blocked: {
    title: "Mutation Blocked",
    severity: "info",
    actions: [],
  },
  transient: {
    title: "Transient Error",
    severity: "warn",
    actions: [],
  },
  internal: {
    title: "Internal Error",
    severity: "error",
    actions: [],
  },
};

const DEFAULT_MAPPING: ErrorMapping = {
  title: "Error",
  severity: "error",
  actions: [],
};

/**
 * Look up the error mapping for a given category.
 * Falls back to a formatted version of the category string.
 */
export function getErrorMapping(category?: string): ErrorMapping {
  if (!category) return DEFAULT_MAPPING;
  if (category in ERROR_MAP) return ERROR_MAP[category];
  return {
    title: category.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()),
    severity: "error",
    actions: [],
  };
}
