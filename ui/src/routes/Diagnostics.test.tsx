import { render, screen, waitFor, fireEvent, act } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import Diagnostics from "./Diagnostics";
import { getDiagnostics } from "../api";

vi.mock("../api", () => ({
    getDiagnostics: vi.fn(),
}));

const mockData = {
    diagnostics_schema_version: 1,
    active_database_provider: "bigquery",
    retry_policy: { mode: "exponential", max_retries: 3 },
    schema_cache_ttl_seconds: 3600,
    runtime_indicators: {
        active_schema_cache_size: 42,
        last_schema_refresh_timestamp: 1700000000,
        avg_query_complexity: 2.5,
        recent_truncation_event_count: 0
    },
    enabled_flags: {
        schema_binding_validation: true,
        disable_llm_retries: false
    }
};

describe("Diagnostics Route", () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it("renders loading state initially", async () => {
        (getDiagnostics as any).mockReturnValue(new Promise(() => { })); // Hang
        render(<Diagnostics />);
        expect(screen.getByText(/Loading system diagnostics/i)).toBeInTheDocument();
    });

    it("renders rich metrics panels when data is loaded", async () => {
        (getDiagnostics as any).mockResolvedValue(mockData);
        render(<Diagnostics />);

        await waitFor(() => {
            expect(screen.getByText("42 items")).toBeInTheDocument();
        });

        // Use regex for case-insensitive matching due to text-transform: capitalize
        expect(screen.getByText(/bigquery/i)).toBeInTheDocument();
        expect(screen.getByText("2.5")).toBeInTheDocument();
        expect(screen.getByText(/schema binding validation: true/i)).toBeInTheDocument();
    });

    it("shows debug panels only when isDebug is true", async () => {
        const dataWithDebug = {
            ...mockData,
            debug: { latency_breakdown_ms: { "llm_call": 150.5 } }
        };
        (getDiagnostics as any).mockResolvedValue(dataWithDebug);

        render(<Diagnostics />);

        await waitFor(() => {
            expect(screen.getByText(/llm call/i)).toBeInTheDocument();
            expect(screen.getByText("150.50")).toBeInTheDocument();
        });

        // Raw snapshot should not be visible by default
        expect(screen.queryByText(/Raw Diagnostic Snapshot/i)).not.toBeInTheDocument();

        // Toggle debug checkbox wrapping in act
        const checkbox = screen.getByLabelText(/Verbose \/ Diagnostic View/i);
        await act(async () => {
            fireEvent.click(checkbox);
        });

        expect(screen.getByText(/Raw Diagnostic Snapshot/i)).toBeInTheDocument();
    });

    it("shows error card on failure", async () => {
        (getDiagnostics as any).mockRejectedValue({
            code: "forbidden",
            message: "Not authorized",
            requestId: "req-123"
        });

        render(<Diagnostics />);

        await waitFor(() => {
            expect(screen.getByText("Not authorized")).toBeInTheDocument();
        });
        // getErrorMapping titleizes "forbidden" to "Forbidden"
        expect(screen.getByTestId("error-category")).toHaveTextContent("Forbidden");
    });

    it("renders fallback error message when error payload is incomplete", async () => {
        (getDiagnostics as any).mockRejectedValue({
            code: "diagnostics_error",
        });

        render(<Diagnostics />);

        await waitFor(() => {
            expect(screen.getByText("Failed to load diagnostics")).toBeInTheDocument();
        });
        expect(screen.getByTestId("error-category")).toHaveTextContent("Diagnostics Error");
    });
});
