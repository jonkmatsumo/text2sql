import { describe, it, expect } from "vitest";
import { buildCopyBundlePayload } from "../observability";

describe("buildCopyBundlePayload", () => {
  it("builds expected SQL and metadata payload", () => {
    const payload = buildCopyBundlePayload({
      sql: "SELECT * FROM orders",
      traceId: "trace-1",
      validationSummary: { ast_valid: true },
      validationReport: { detected_cartesian_flag: false },
      resultCompleteness: { pages_fetched: 2, next_page_token: "next-page" },
    });

    expect(payload).toEqual({
      sql: "SELECT * FROM orders",
      trace_id: "trace-1",
      validation: {
        status: "pass",
        cartesian_risk: false,
        validation_summary: { ast_valid: true },
        validation_report: { detected_cartesian_flag: false },
      },
      completeness: {
        status: "paginated",
        pages_fetched: 2,
        completeness_summary: { pages_fetched: 2, next_page_token: "next-page" },
      },
    });
  });

  it("handles missing optional metadata fields", () => {
    const payload = buildCopyBundlePayload({
      sql: "SELECT 1",
    });

    expect(payload).not.toHaveProperty("trace_id");
    expect(payload).toEqual({
      sql: "SELECT 1",
      validation: {
        status: "pass",
        cartesian_risk: false,
        validation_summary: null,
        validation_report: null,
      },
      completeness: {
        status: "complete",
        pages_fetched: null,
        completeness_summary: null,
      },
    });
  });
});
