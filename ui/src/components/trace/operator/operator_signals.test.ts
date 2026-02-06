import { describe, expect, it } from "vitest";
import { extractOperatorSignals } from "./operator_signals";

describe("extractOperatorSignals", () => {
  it("extracts key operator sections from allowlisted attributes", () => {
    const sections = extractOperatorSignals({
      "result.rows_returned": 42,
      "result.is_truncated": true,
      "result.partial_reason": "PROVIDER_CAP",
      "retry.attempt_number": 2,
      "validation.pre_exec_check_passed": false,
      "capability.required": "pagination",
      "capability.fallback_applied": true,
      "pagination.auto_paginated": true,
      "prefetch.scheduled": true
    });

    const ids = sections.map((section) => section.id);
    expect(ids).toContain("completeness");
    expect(ids).toContain("retries");
    expect(ids).toContain("validation");
    expect(ids).toContain("capability");
    expect(ids).toContain("pagination");
  });

  it("returns no sections for unrelated attributes", () => {
    const sections = extractOperatorSignals({
      "custom.foo": "bar",
      "another.value": 1
    });
    expect(sections).toEqual([]);
  });

  it("does not expose non-allowlisted sensitive keys", () => {
    const sections = extractOperatorSignals({
      "llm.prompt.user": "secret text",
      "auth.token": "abc123"
    });
    expect(sections).toEqual([]);
  });
});
