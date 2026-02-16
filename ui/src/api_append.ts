
/**
 * Run the agent with streaming progress updates.
 * Yields parsed SSE events.
 */
export async function* runAgentStream(request: AgentRunRequest): AsyncGenerator<{ event: string, data: any }, void, unknown> {
    const url = `${agentBase}/agent/run/stream`; // Using agentBase (could use uiApiBase)
    const response = await fetch(url, {
        method: "POST",
        headers: {
            ...getAuthHeaders(),
            "Content-Type": "application/json"
        },
        body: JSON.stringify(request)
    });

    if (!response.ok) {
        const text = await response.text();
        throw new Error(`Agent run failed: ${response.status} ${text}`);
    }

    if (!response.body) {
        throw new Error("No response body received from stream endpoint");
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buffer = "";

    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n\n");
            // Keep the last partial chunk in buffer
            buffer = lines.pop() || "";

            for (const line of lines) {
                if (!line.trim()) continue;
                const linesInBlock = line.split("\n");
                let eventType = "";
                let data = null;

                for (const l of linesInBlock) {
                    if (l.startsWith("event: ")) {
                        eventType = l.substring(7).trim();
                    } else if (l.startsWith("data: ")) {
                        try {
                            data = JSON.parse(l.substring(6));
                        } catch (e) {
                            console.warn("Failed to parse SSE data:", l);
                        }
                    }
                }

                if (eventType && data !== null) {
                    yield { event: eventType, data };
                }
            }
        }
    } final {
        reader.releaseLock();
    }
}
