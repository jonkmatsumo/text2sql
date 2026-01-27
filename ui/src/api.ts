import { AgentRunRequest, AgentRunResponse, FeedbackRequest } from "./types";
import {
  Interaction,
  ApprovedExample,
  PinRule,
  RecommendationResult,
  PatternGenerationResult,
  PatternReloadResult
} from "./types/admin";

const agentBase = import.meta.env.VITE_AGENT_SERVICE_URL || "http://localhost:8081";
const uiApiBase = import.meta.env.VITE_UI_API_URL || "http://localhost:8082";

export async function runAgent({
  question,
  tenantId,
  threadId
}: AgentRunRequest): Promise<AgentRunResponse> {
  const response = await fetch(`${agentBase}/agent/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      question,
      tenant_id: tenantId,
      thread_id: threadId
    })
  });

  if (!response.ok) {
    throw new Error(`Agent service error (${response.status})`);
  }

  return response.json();
}

export async function submitFeedback({
  interactionId,
  thumb,
  comment
}: FeedbackRequest): Promise<any> {
  const response = await fetch(`${uiApiBase}/feedback`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      interaction_id: interactionId,
      thumb,
      comment
    })
  });

  if (!response.ok) {
    throw new Error(`Feedback error (${response.status})`);
  }

  return response.json();
}

export const AdminService = {
  async listInteractions(
    limit: number = 50,
    thumb: string = "All",
    status: string = "All"
  ): Promise<Interaction[]> {
    const params = new URLSearchParams({
      limit: limit.toString(),
      thumb,
      status
    });
    const response = await fetch(`${uiApiBase}/interactions?${params}`);
    if (!response.ok) throw new Error("Failed to load interactions");
    return response.json();
  },

  async getInteractionDetails(id: string): Promise<Interaction> {
    const response = await fetch(`${uiApiBase}/interactions/${id}`);
    if (!response.ok) throw new Error("Failed to load interaction details");
    return response.json();
  },

  async approveInteraction(
    id: string,
    correctedSql: string,
    originalSql: string,
    notes: string = ""
  ): Promise<string> {
    const response = await fetch(`${uiApiBase}/interactions/${id}/approve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        corrected_sql: correctedSql,
        original_sql: originalSql,
        notes
      })
    });
    if (!response.ok) throw new Error("Failed to approve interaction");
    return response.json();
  },

  async rejectInteraction(
    id: string,
    reason: string = "CANNOT_FIX",
    notes: string = ""
  ): Promise<string> {
    const response = await fetch(`${uiApiBase}/interactions/${id}/reject`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason, notes })
    });
    if (!response.ok) throw new Error("Failed to reject interaction");
    return response.json();
  },

  async publishApproved(limit: number = 50): Promise<any> {
    const response = await fetch(`${uiApiBase}/registry/publish-approved`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ limit })
    });
    if (!response.ok) throw new Error("Failed to publish approved interactions");
    return response.json();
  },

  async listExamples(limit: number = 100, search?: string): Promise<ApprovedExample[]> {
    const params = new URLSearchParams({ limit: limit.toString() });
    if (search) params.append("search", search);
    const response = await fetch(`${uiApiBase}/registry/examples?${params}`);
    if (!response.ok) throw new Error("Failed to load examples");
    return response.json();
  },

  async listPins(tenantId: number): Promise<PinRule[]> {
    const params = new URLSearchParams({ tenant_id: tenantId.toString() });
    const response = await fetch(`${uiApiBase}/pins?${params}`);
    if (!response.ok) throw new Error("Failed to list pins");
    return response.json();
  },

  async upsertPin(data: Partial<PinRule> & { tenant_id: number }): Promise<PinRule> {
    const isUpdate = !!data.id;
    const url = isUpdate ? `${uiApiBase}/pins/${data.id}` : `${uiApiBase}/pins`;
    const response = await fetch(url, {
      method: isUpdate ? "PATCH" : "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data)
    });
    if (!response.ok) throw new Error("Failed to upsert pin");
    return response.json();
  },

  async deletePin(id: string, tenantId: number): Promise<{ success: boolean }> {
    const params = new URLSearchParams({ tenant_id: tenantId.toString() });
    const response = await fetch(`${uiApiBase}/pins/${id}?${params}`, {
      method: "DELETE"
    });
    if (!response.ok) throw new Error("Failed to delete pin");
    return response.json();
  }
};

export const OpsService = {
  async runRecommendations(
    query: string,
    tenantId: number,
    limit: number,
    enableFallback: boolean
  ): Promise<RecommendationResult> {
    const response = await fetch(`${uiApiBase}/recommendations/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        query,
        tenant_id: tenantId,
        limit,
        enable_fallback: enableFallback
      })
    });
    if (!response.ok) throw new Error("Recommendation run failed");
    return response.json();
  },

  async generatePatterns(dryRun: boolean = false): Promise<PatternGenerationResult> {
    const response = await fetch(`${uiApiBase}/ops/patterns/generate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ dry_run: dryRun })
    });
    if (!response.ok) throw new Error("Pattern generation failed");
    return response.json();
  },

  async reloadPatterns(): Promise<PatternReloadResult> {
    const response = await fetch(`${uiApiBase}/ops/patterns/reload`, {
      method: "POST"
    });
    if (!response.ok) throw new Error("Pattern reload failed");
    return response.json();
  }
};
