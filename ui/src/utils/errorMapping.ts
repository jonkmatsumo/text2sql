/**
 * Single source of truth for mapping backend ErrorCategory values
 * to UI labels, severity styles, and suggested CTAs.
 */

export type ErrorSeverity = "info" | "warn" | "error";

export interface ErrorMapping {
  title: string;
  severity: ErrorSeverity;
  actions: Array<{ label: string; href: string }>;
  /** Human-readable explanation shown in WorkflowGuidance */
  description?: string;
  /** Richer CTAs for WorkflowGuidance (may include primary flag) */
  guidanceActions?: Array<{ label: string; href: string; primary?: boolean }>;
}

const ERROR_MAP: Record<string, ErrorMapping> = {
  schema_drift: {
    title: "Schema Mismatch",
    severity: "error",
    actions: [{ label: "Resolve Schema Mismatch", href: "/admin/operations" }],
    description: "The database schema appears to have changed. Updating the system's metadata snapshot might resolve this.",
    guidanceActions: [
      { label: "Run Schema Hydration", href: "/admin/operations?tab=schema", primary: true },
      { label: "Reload NLP Patterns", href: "/admin/operations?tab=nlp" },
    ],
  },
  schema_missing: {
    title: "Schema Not Found",
    severity: "error",
    actions: [{ label: "Ingest Missing Table", href: "/admin/operations" }],
    description: "The agent couldn't find the necessary table schema. You may need to ingest the table or refresh the system metadata.",
    guidanceActions: [
      { label: "Go to Ingestion Wizard", href: "/admin/operations?tab=ingestion", primary: true },
      { label: "Check Schema Hydration", href: "/admin/operations?tab=schema" },
    ],
  },
  auth: {
    title: "Authentication Error",
    severity: "error",
    actions: [{ label: "Update Permissions", href: "/admin/settings/query-target" }],
    description: "The system encountered a credential or permission issue. Verify your configuration and access levels.",
    guidanceActions: [
      { label: "Update Target Settings", href: "/admin/settings/query-target", primary: true },
    ],
  },
  unauthorized: {
    title: "Unauthorized",
    severity: "error",
    actions: [{ label: "Update Permissions", href: "/admin/settings/query-target" }],
    description: "You do not have permission to access the target database. Please check your user credentials.",
    guidanceActions: [
      { label: "Check Permissions", href: "/admin/settings/query-target", primary: true },
    ],
  },
  permission_denied: {
    title: "Permission Denied",
    severity: "error",
    actions: [{ label: "Update Permissions", href: "/admin/settings/query-target" }],
    description: "Access was denied by the target database. This often indicates insufficient database-level privileges.",
    guidanceActions: [
      { label: "Review User Roles", href: "/admin/settings/query-target", primary: true },
    ],
  },
  connectivity: {
    title: "Connection Error",
    severity: "error",
    actions: [{ label: "Check Definition", href: "/admin/settings/query-target" }],
    description: "The system is having trouble reaching the target database. Please check your connection strings and credentials.",
    guidanceActions: [
      { label: "Verify Target Settings", href: "/admin/settings/query-target", primary: true },
      { label: "Check Connectivity Diagnostics", href: "/admin/diagnostics" },
    ],
  },
  timeout: {
    title: "Timeout",
    severity: "warn",
    actions: [],
    description: "The request took too long to complete. Try a simpler query or check if the database is under heavy load.",
    guidanceActions: [
      { label: "Check Connectivity", href: "/admin/diagnostics", primary: true },
      { label: "View System Health", href: "/admin/operations" },
    ],
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
  // budget_exhausted and budget_exceeded are semantically identical in the UI;
  // both indicate hard quota limits reached. We alias them here to handle
  // inconsistent naming across different provider backends.
  budget_exhausted: {
    title: "Budget Exhausted",
    severity: "warn",
    actions: [{ label: "Manage Quotas", href: "/admin/settings/query-target" }],
    description: "You have reached your allocated usage quota. You can request a limit increase or wait for the next billing cycle.",
    guidanceActions: [
      { label: "Manage Quotas", href: "/admin/settings/query-target", primary: true },
    ],
  },
  budget_exceeded: {
    title: "Budget Exceeded",
    severity: "warn",
    actions: [{ label: "Manage Quotas", href: "/admin/settings/query-target" }],
    description: "You have reached your allocated usage quota. You can request a limit increase or wait for the next billing cycle.",
    guidanceActions: [
      { label: "Manage Quotas", href: "/admin/settings/query-target", primary: true },
    ],
  },
  resource_exhausted: {
    title: "Resource Exhausted",
    severity: "error",
    actions: [{ label: "Check System Health", href: "/admin/operations" }],
    description: "The system or target database has run out of resources (CPU, Memory, or Connections).",
    guidanceActions: [
      { label: "Go to Operations", href: "/admin/operations", primary: true },
      { label: "Review Diagnostics", href: "/admin/diagnostics" },
    ],
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
    description: "A temporary error occurred in the system. Often, retrying the operation will resolve this.",
    guidanceActions: [
      { label: "Retry Operation", href: "#", primary: true },
    ],
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
