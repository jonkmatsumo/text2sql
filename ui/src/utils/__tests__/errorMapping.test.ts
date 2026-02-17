import { describe, it, expect } from "vitest";
import { getErrorMapping } from "../errorMapping";

describe("getErrorMapping", () => {
  it("maps schema_missing to Ingestion Wizard action", () => {
    const mapping = getErrorMapping("schema_missing");
    expect(mapping.title).toBe("Schema Not Found");
    expect(mapping.severity).toBe("error");
    expect(mapping.actions).toEqual([
      { label: "Ingest Missing Table", href: "/admin/operations" },
    ]);
  });

  it("maps limit_exceeded to warn severity with no actions", () => {
    const mapping = getErrorMapping("limit_exceeded");
    expect(mapping.title).toBe("Limit Exceeded");
    expect(mapping.severity).toBe("warn");
    expect(mapping.actions).toEqual([]);
  });

  it("maps llm_rate_limit_exceeded to warn severity", () => {
    const mapping = getErrorMapping("llm_rate_limit_exceeded");
    expect(mapping.title).toBe("LLM Rate Limit");
    expect(mapping.severity).toBe("warn");
  });

  it("maps timeout to warn severity", () => {
    const mapping = getErrorMapping("timeout");
    expect(mapping.severity).toBe("warn");
  });

  it("maps permission_denied to settings action", () => {
    const mapping = getErrorMapping("permission_denied");
    expect(mapping.title).toBe("Permission Denied");
    expect(mapping.actions[0].href).toBe("/admin/settings/query-target");
  });

  it("maps tool_response_malformed correctly", () => {
    const mapping = getErrorMapping("tool_response_malformed");
    expect(mapping.title).toBe("Malformed Response");
    expect(mapping.severity).toBe("error");
  });

  it("maps invalid_request to info severity", () => {
    const mapping = getErrorMapping("invalid_request");
    expect(mapping.severity).toBe("info");
  });

  it("falls back to formatted category for unknown values", () => {
    const mapping = getErrorMapping("some_new_category");
    expect(mapping.title).toBe("Some New Category");
    expect(mapping.severity).toBe("error");
    expect(mapping.actions).toEqual([]);
  });

  it("returns default 'Error' for undefined category", () => {
    const mapping = getErrorMapping(undefined);
    expect(mapping.title).toBe("Error");
  });
});
