import { AgentRunRequest, AgentRunResponse, FeedbackRequest } from "./types";

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
