export interface AgentRunRequest {
    question: string;
    tenantId: number;
    threadId: string;
    llmProvider?: string;
    llmModel?: string;
}

export interface AgentRunResponse {
    sql?: string;
    result?: any;
    response?: string;
    error?: string;
    from_cache: boolean;
    interaction_id?: string;
    trace_id?: string;
    viz_spec?: any;
}

export interface FeedbackRequest {
    interactionId: string;
    thumb: "UP" | "DOWN";
    comment?: string;
}
