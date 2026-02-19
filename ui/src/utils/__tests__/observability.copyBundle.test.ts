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

    expect(payload).toEqual(expect.objectContaining({
      schema_version: 1,
      trace_id: "trace-1",
      sql: "SELECT * FROM orders",
      identifiers: {
        "Trace ID": "trace-1"
      },
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
      bundle_metadata: expect.objectContaining({
        environment: "test",
        generated_at: expect.any(String),
      })
    }));
  });

  it("handles missing optional metadata fields", () => {
    const payload = buildCopyBundlePayload({
      sql: "SELECT 1",
    });

    expect(payload).toEqual(expect.objectContaining({
      schema_version: 1,
      sql: "SELECT 1",
      identifiers: {},
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
      bundle_metadata: expect.objectContaining({
        environment: "test",
        generated_at: expect.any(String),
      })
    }));
  });
});
