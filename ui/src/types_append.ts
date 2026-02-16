
export interface AgentProgressData {
    phase: string;
    timestamp: number;
}

export type AgentStreamEventType = "startup" | "progress" | "result" | "error";

export interface AgentStreamEvent {
    event: AgentStreamEventType;
    data: any; // Flexible data based on event type
}
