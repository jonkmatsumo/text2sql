import { render, screen, waitFor, fireEvent, act, within } from "@testing-library/react";
import { useState } from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter, useLocation } from "react-router-dom";
import Diagnostics from "./Diagnostics";
import { getDiagnostics, OpsService } from "../api";
import { DIAGNOSTICS_SECTION_ARIA_LABEL } from "../constants/operatorUi";
import { DIAGNOSTICS_RUN_SIGNAL_PAGE_SIZE } from "../constants/pagination";
import * as useToastHook from "../hooks/useToast";

vi.mock("../api", () => ({
    getDiagnostics: vi.fn(),
    OpsService: {
        listRuns: vi.fn(),
    },
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

function LocationProbe() {
    const location = useLocation();
    return <div data-testid="location-search">{location.search}</div>;
}

function renderDiagnostics(initialPath: string = "/diagnostics") {
    return render(
        <MemoryRouter initialEntries={[initialPath]}>
            <Diagnostics />
            <LocationProbe />
        </MemoryRouter>
    );
}

function DiagnosticsRerenderHarness({ initialPath }: { initialPath: string }) {
    const [tick, setTick] = useState(0);
    return (
        <MemoryRouter initialEntries={[initialPath]}>
            <button type="button" data-testid="diagnostics-force-rerender" onClick={() => setTick((v) => v + 1)}>
                rerender-{tick}
            </button>
            <Diagnostics />
            <LocationProbe />
        </MemoryRouter>
    );
}

describe("Diagnostics Route", () => {
    let showToastMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        vi.clearAllMocks();
        showToastMock = vi.fn();
        vi.spyOn(useToastHook, "useToast").mockReturnValue({ show: showToastMock } as any);
        (OpsService.listRuns as any).mockResolvedValue({ runs: [] });
    });

    it("renders loading state initially", async () => {
        (getDiagnostics as any).mockReturnValue(new Promise(() => { })); // Hang
        renderDiagnostics();
        expect(screen.getByText(/Loading system diagnostics/i)).toBeInTheDocument();
    });

    it("renders rich metrics panels when data is loaded", async () => {
        (getDiagnostics as any).mockResolvedValue(mockData);
        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByText("42 items")).toBeInTheDocument();
        });

        // Use regex for case-insensitive matching due to text-transform: capitalize
        expect(screen.getByText(/bigquery/i)).toBeInTheDocument();
        expect(screen.getByText("2.5")).toBeInTheDocument();
        expect(screen.getByText(/schema binding validation: true/i)).toBeInTheDocument();
        expect(screen.getByTestId("diagnostics-last-updated")).toHaveTextContent(/Last updated:/i);
    });

    it("renders unknown enabled_flags keys without failing", async () => {
        (getDiagnostics as any).mockResolvedValue({
            ...mockData,
            enabled_flags: {
                ...mockData.enabled_flags,
                experimental_backend_switch: true,
            },
        });
        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByText(/experimental backend switch: true/i)).toBeInTheDocument();
        });
    });

    it("requests degraded runs concurrently", async () => {
        let resolveFailed: ((value: any) => void) | undefined;
        const failedPromise = new Promise<any>((resolve) => {
            resolveFailed = resolve;
        });
        (getDiagnostics as any).mockResolvedValue(mockData);
        (OpsService.listRuns as any)
            .mockImplementationOnce(() => failedPromise)
            .mockImplementationOnce(() => Promise.resolve({ runs: [] }));

        renderDiagnostics();

        await waitFor(() => {
            expect(OpsService.listRuns).toHaveBeenCalledTimes(2);
        });
        expect(screen.getByTestId("diagnostics-run-signals-loading")).toBeInTheDocument();

        expect((OpsService.listRuns as any).mock.calls[0]).toEqual([DIAGNOSTICS_RUN_SIGNAL_PAGE_SIZE, 0, "FAILED"]);
        expect((OpsService.listRuns as any).mock.calls[1]).toEqual([DIAGNOSTICS_RUN_SIGNAL_PAGE_SIZE, 0, "All", "DOWN"]);

        if (resolveFailed) {
            await act(async () => {
                resolveFailed!({ runs: [] });
            });
        }

        await waitFor(() => {
            expect(screen.queryByTestId("diagnostics-run-signals-loading")).not.toBeInTheDocument();
        });
    });

    it("keeps low-rated runs visible when failed-runs fetch rejects", async () => {
        const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => { });
        const lowRatedTs = new Date(Date.now() - 5 * 60 * 1000).toISOString();
        (getDiagnostics as any).mockResolvedValue(mockData);
        (OpsService.listRuns as any)
            .mockRejectedValueOnce(new Error("failed-runs-broken"))
            .mockResolvedValueOnce({
                runs: [
                    {
                        id: "low-success",
                        user_nlq_text: "Low-rating run still visible",
                        execution_status: "SUCCESS",
                        thumb: "DOWN",
                        created_at: lowRatedTs,
                    },
                ],
            });

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByText("Low-rating run still visible")).toBeInTheDocument();
        });

        const failuresSection = screen.getByTestId("diagnostics-failures-section");
        const lowRatingsSection = screen.getByTestId("diagnostics-low-ratings-section");
        expect(within(failuresSection).getByText("Could not load recent failures.")).toBeInTheDocument();
        expect(within(failuresSection).getByTestId("diagnostics-failures-section-retry")).toBeInTheDocument();
        expect(within(failuresSection).queryByText("No recent system failures found.")).not.toBeInTheDocument();
        expect(within(lowRatingsSection).getByText("Low-rating run still visible")).toBeInTheDocument();

        expect(showToastMock).toHaveBeenCalledWith(
            "Failed to load failed run signals. Refresh to retry.",
            "error",
            expect.objectContaining({
                dedupeKey: expect.stringContaining("panel=failed-runs"),
            })
        );
        expect(showToastMock).toHaveBeenCalledWith(
            "Failed to load failed run signals. Refresh to retry.",
            "error",
            expect.objectContaining({
                dedupeKey: expect.stringContaining("error=Error"),
            })
        );
        expect(consoleErrorSpy).toHaveBeenCalledWith(
            "Failed to load degraded runs",
            expect.objectContaining({
                surface: "Diagnostics.runSignals",
                failed: expect.objectContaining({
                    surface: "Diagnostics.runSignals.failed",
                    reason: expect.stringContaining("failed-runs-broken"),
                }),
            })
        );
        consoleErrorSpy.mockRestore();
    });

    it("keeps failed-runs visible when low-ratings fetch rejects", async () => {
        const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => { });
        const failedTs = new Date(Date.now() - 7 * 60 * 1000).toISOString();
        (getDiagnostics as any).mockResolvedValue(mockData);
        (OpsService.listRuns as any)
            .mockResolvedValueOnce({
                runs: [
                    {
                        id: "failed-success",
                        user_nlq_text: "Failure run still visible",
                        execution_status: "FAILED",
                        thumb: "None",
                        created_at: failedTs,
                    },
                ],
            })
            .mockRejectedValueOnce(new Error("low-ratings-broken"));

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByText("Failure run still visible")).toBeInTheDocument();
        });

        const failuresSection = screen.getByTestId("diagnostics-failures-section");
        const lowRatingsSection = screen.getByTestId("diagnostics-low-ratings-section");
        expect(within(failuresSection).getByText("Failure run still visible")).toBeInTheDocument();
        expect(within(lowRatingsSection).getByText("Could not load recent low-rated runs.")).toBeInTheDocument();
        expect(within(lowRatingsSection).getByTestId("diagnostics-low-ratings-section-retry")).toBeInTheDocument();
        expect(within(lowRatingsSection).queryByText("No recent low-rated runs found.")).not.toBeInTheDocument();

        expect(showToastMock).toHaveBeenCalledWith(
            "Failed to load low-rated run signals. Refresh to retry.",
            "error",
            expect.objectContaining({
                dedupeKey: expect.stringContaining("panel=low-ratings"),
            })
        );
        expect(showToastMock).toHaveBeenCalledWith(
            "Failed to load low-rated run signals. Refresh to retry.",
            "error",
            expect.objectContaining({
                dedupeKey: expect.stringContaining("error=Error"),
            })
        );
        consoleErrorSpy.mockRestore();
    });

    it("shows separate dedupable toasts when both run-signal calls fail", async () => {
        const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => { });
        (getDiagnostics as any).mockResolvedValue(mockData);
        (OpsService.listRuns as any)
            .mockRejectedValueOnce(new Error("failed-runs-broken"))
            .mockRejectedValueOnce(new Error("low-ratings-broken"));

        renderDiagnostics();

        await waitFor(() => {
            const failuresSection = screen.getByTestId("diagnostics-failures-section");
            const lowRatingsSection = screen.getByTestId("diagnostics-low-ratings-section");
            expect(within(failuresSection).getByText("Could not load recent failures.")).toBeInTheDocument();
            expect(within(lowRatingsSection).getByText("Could not load recent low-rated runs.")).toBeInTheDocument();
        });

        expect(showToastMock).toHaveBeenCalledTimes(2);
        expect(showToastMock).toHaveBeenNthCalledWith(
            1,
            "Failed to load failed run signals. Refresh to retry.",
            "error",
            expect.objectContaining({
                dedupeKey: expect.stringContaining("panel=failed-runs"),
            })
        );
        expect(showToastMock).toHaveBeenNthCalledWith(
            2,
            "Failed to load low-rated run signals. Refresh to retry.",
            "error",
            expect.objectContaining({
                dedupeKey: expect.stringContaining("panel=low-ratings"),
            })
        );
        consoleErrorSpy.mockRestore();
    });

    it("retries run-signal fetches from section-level retry controls without reloading diagnostics", async () => {
        const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => { });
        const failedTs = new Date(Date.now() - 9 * 60 * 1000).toISOString();
        const lowTs = new Date(Date.now() - 6 * 60 * 1000).toISOString();
        (getDiagnostics as any).mockResolvedValue(mockData);
        (OpsService.listRuns as any)
            .mockRejectedValueOnce(new Error("failed-runs-broken"))
            .mockResolvedValueOnce({
                runs: [
                    {
                        id: "low-initial",
                        user_nlq_text: "Low-rating run from initial fetch",
                        execution_status: "SUCCESS",
                        thumb: "DOWN",
                        created_at: lowTs,
                    },
                ],
            })
            .mockResolvedValueOnce({
                runs: [
                    {
                        id: "failed-retry",
                        user_nlq_text: "Failure run recovered",
                        execution_status: "FAILED",
                        thumb: "None",
                        created_at: failedTs,
                    },
                ],
            })
            .mockResolvedValueOnce({
                runs: [
                    {
                        id: "low-retry",
                        user_nlq_text: "Low-rating run after retry",
                        execution_status: "SUCCESS",
                        thumb: "DOWN",
                        created_at: lowTs,
                    },
                ],
            });

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-failures-section-retry")).toBeInTheDocument();
        });

        fireEvent.click(screen.getByTestId("diagnostics-failures-section-retry"));

        await waitFor(() => {
            expect(screen.getByText("Failure run recovered")).toBeInTheDocument();
        });

        expect(getDiagnostics).toHaveBeenCalledTimes(1);
        expect(OpsService.listRuns).toHaveBeenCalledTimes(4);
        consoleErrorSpy.mockRestore();
    });

    it("keeps degraded runs with missing created_at from surfacing as most recent", async () => {
        const recentTs1 = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(); // 2h ago
        const recentTs2 = new Date(Date.now() - 1 * 60 * 60 * 1000).toISOString(); // 1h ago
        (getDiagnostics as any).mockResolvedValue(mockData);
        (OpsService.listRuns as any)
            .mockResolvedValueOnce({
                runs: [
                    {
                        id: "missing-date",
                        user_nlq_text: "Run without timestamp",
                        execution_status: "FAILED",
                        thumb: "DOWN",
                    },
                    {
                        id: "older-run",
                        user_nlq_text: "Older degraded run",
                        execution_status: "FAILED",
                        thumb: "DOWN",
                        created_at: recentTs1,
                    },
                ]
            })
            .mockResolvedValueOnce({
                runs: [
                    {
                        id: "newer-run",
                        user_nlq_text: "Newest degraded run",
                        execution_status: "SUCCESS",
                        thumb: "DOWN",
                        created_at: recentTs2,
                    },
                ]
            });

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByText("Newest degraded run")).toBeInTheDocument();
        });

        expect(screen.getByText("Older degraded run")).toBeInTheDocument();
        expect(screen.queryByText("Run without timestamp")).not.toBeInTheDocument();
    });

    it("renders failures and low ratings in separate sections", async () => {
        const failTs = new Date(Date.now() - 5 * 60 * 1000).toISOString();  // 5m ago
        const lowTs = new Date(Date.now() - 10 * 60 * 1000).toISOString(); // 10m ago
        (getDiagnostics as any).mockResolvedValue(mockData);
        (OpsService.listRuns as any)
            .mockResolvedValueOnce({
                runs: [
                    {
                        id: "failed-1",
                        user_nlq_text: "Failure query",
                        execution_status: "FAILED",
                        thumb: "None",
                        created_at: failTs,
                    },
                ]
            })
            .mockResolvedValueOnce({
                runs: [
                    {
                        id: "low-1",
                        user_nlq_text: "Low rating query",
                        execution_status: "SUCCESS",
                        thumb: "DOWN",
                        created_at: lowTs,
                    },
                ]
            });

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByText("System failures (FAILED)")).toBeInTheDocument();
            expect(screen.getByText("Operator feedback (DOWN)")).toBeInTheDocument();
        });
        expect(
            screen.getByText(
                new RegExp(`Showing most recent ${DIAGNOSTICS_RUN_SIGNAL_PAGE_SIZE} failures`)
            )
        ).toBeInTheDocument();
        expect(screen.getByText(/no server-side time window/i)).toBeInTheDocument();

        const failuresSection = screen.getByTestId("diagnostics-failures-section");
        const lowRatingsSection = screen.getByTestId("diagnostics-low-ratings-section");

        expect(within(failuresSection).getByText("Failure query")).toBeInTheDocument();
        expect(within(lowRatingsSection).getByText("Low rating query")).toBeInTheDocument();
        expect(within(failuresSection).getByText(new Date(failTs).toLocaleString())).toBeInTheDocument();
        expect(within(failuresSection).queryByText("Low rating query")).not.toBeInTheDocument();
    });

    it("renders recency window dropdown with default 7d (168h) selection", async () => {
        (getDiagnostics as any).mockResolvedValue(mockData);
        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-recency-window")).toBeInTheDocument();
        });

        expect(screen.getByTestId("diagnostics-recency-window")).toHaveValue("168");
        expect(screen.getByText(/Degraded run signals \(last 7 days\)/i)).toBeInTheDocument();
    });

    it("client-side recency filter excludes runs older than selected window", async () => {
        const now = Date.now();
        const recentTs = new Date(now - 30 * 60 * 1000).toISOString(); // 30m ago
        const oldTs = new Date(now - 48 * 60 * 60 * 1000).toISOString(); // 48h ago

        (getDiagnostics as any).mockResolvedValue(mockData);
        (OpsService.listRuns as any)
            .mockResolvedValueOnce({
                runs: [
                    { id: "recent-fail", user_nlq_text: "Recent failure", execution_status: "FAILED", thumb: "None", created_at: recentTs },
                    { id: "old-fail", user_nlq_text: "Old failure", execution_status: "FAILED", thumb: "None", created_at: oldTs },
                ]
            })
            .mockResolvedValueOnce({ runs: [] });

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByText("Recent failure")).toBeInTheDocument();
        });

        // Both visible under default 7d window (48h < 168h)
        expect(screen.getByText("Old failure")).toBeInTheDocument();

        // Switching to 1h window should hide both (since both are older than 1h, actually 30m is within 1h)
        // Switch to 1h window
        fireEvent.change(screen.getByTestId("diagnostics-recency-window"), { target: { value: "1" } });
        expect(screen.getByText("Recent failure")).toBeInTheDocument();
        expect(screen.queryByText("Old failure")).not.toBeInTheDocument();
        expect(screen.getByText(/Degraded run signals \(last 1 hour\)/i)).toBeInTheDocument();
    });

    it("excludes runs with missing or invalid created_at and shows data-driven excluded count", async () => {
        const now = Date.now();
        const validTimestamp = new Date(now - 15 * 60 * 1000).toISOString();
        const failedRuns = [
            { id: "fail-valid", user_nlq_text: "Failure with valid timestamp", execution_status: "FAILED", thumb: "None", created_at: validTimestamp },
            { id: "fail-missing", user_nlq_text: "Failure missing timestamp", execution_status: "FAILED", thumb: "None" },
            { id: "fail-invalid", user_nlq_text: "Failure invalid timestamp", execution_status: "FAILED", thumb: "None", created_at: "invalid-date" },
        ];
        const lowRatingRuns = [
            { id: "low-valid", user_nlq_text: "Low rating with valid timestamp", execution_status: "SUCCESS", thumb: "DOWN", created_at: validTimestamp },
            { id: "low-missing", user_nlq_text: "Low rating missing timestamp", execution_status: "SUCCESS", thumb: "DOWN" },
        ];
        const expectedExcludedCount = [...failedRuns, ...lowRatingRuns].filter((run) => !run.created_at || Number.isNaN(Date.parse(run.created_at))).length;

        (getDiagnostics as any).mockResolvedValue(mockData);
        (OpsService.listRuns as any).mockImplementation((_limit: number, _offset: number, status?: string, thumb?: string) => {
            if (status === "FAILED") {
                return Promise.resolve({ runs: failedRuns });
            }
            if (thumb === "DOWN") {
                return Promise.resolve({ runs: lowRatingRuns });
            }
            return Promise.resolve({ runs: [] });
        });

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-excluded-note")).toBeInTheDocument();
        });

        const note = screen.getByTestId("diagnostics-excluded-note");
        expect(note).toHaveTextContent(String(expectedExcludedCount));
        expect(note).toHaveTextContent(/excluded due to missing or invalid timestamps/i);
        expect(screen.queryByText("Failure missing timestamp")).not.toBeInTheDocument();
        expect(screen.queryByText("Failure invalid timestamp")).not.toBeInTheDocument();
        expect(screen.queryByText("Low rating missing timestamp")).not.toBeInTheDocument();
        expect(screen.getByText("Failure with valid timestamp")).toBeInTheDocument();
        expect(screen.getByText("Low rating with valid timestamp")).toBeInTheDocument();
    });

    it("shows debug panels only when isDebug is true", async () => {
        const dataWithDebug = {
            ...mockData,
            debug: { latency_breakdown_ms: { "llm_call": 150.5 } }
        };
        (getDiagnostics as any).mockResolvedValue(dataWithDebug);

        renderDiagnostics();

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
        expect(screen.getByTestId("diagnostics-raw-json")).toHaveTextContent("\"diagnostics_schema_version\": 1");
    });

    it("shows error card on failure", async () => {
        (getDiagnostics as any).mockRejectedValue({
            code: "forbidden",
            message: "Not authorized",
            requestId: "req-123"
        });

        renderDiagnostics();

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

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByText("Failed to load diagnostics")).toBeInTheDocument();
        });
        expect(screen.getByTestId("error-category")).toHaveTextContent("Diagnostics Error");
    });

    it("invokes diagnostics refresh only once per click while loading", async () => {
        let resolveRefresh: ((value: typeof mockData) => void) | undefined;
        (getDiagnostics as any)
            .mockResolvedValueOnce(mockData)
            .mockImplementationOnce(() => new Promise((resolve) => {
                resolveRefresh = resolve;
            }));

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByText("42 items")).toBeInTheDocument();
        });

        const refreshButton = screen.getByRole("button", { name: "Refresh" });
        fireEvent.click(refreshButton);
        fireEvent.click(refreshButton);

        expect(getDiagnostics).toHaveBeenCalledTimes(2); // initial load + one refresh

        if (resolveRefresh) {
            resolveRefresh(mockData);
        }
        await waitFor(() => {
            expect(screen.getByRole("button", { name: "Refresh" })).toBeEnabled();
        });
    });

    it("supports anomaly-only filtering with degraded status highlighting", async () => {
        (getDiagnostics as any).mockResolvedValue({
            ...mockData,
            runtime_indicators: {
                ...mockData.runtime_indicators,
                avg_query_complexity: 11.2,
                recent_truncation_event_count: 8,
            },
        });

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-status-strip")).toHaveTextContent("System Status: Degraded");
        });
        expect(screen.getByTestId("diagnostics-status-strip")).toHaveTextContent("2 anomalies detected");

        fireEvent.click(screen.getByTestId("diagnostics-filter-anomalies"));

        // Non-anomalous runtime row is hidden in anomaly-only mode
        expect(screen.queryByText("Schema Cache Size")).not.toBeInTheDocument();
        // Configuration panel is hidden in anomaly-only mode to reduce noise
        expect(screen.queryByText("Configuration & Policy")).not.toBeInTheDocument();
        expect(screen.getByText(/context only; not anomaly-derived/i)).toBeInTheDocument();
    });

    it("round-trips diagnostics view state through query params", async () => {
        (getDiagnostics as any).mockResolvedValue(mockData);
        renderDiagnostics("/diagnostics?filter=anomalies&debug=1&section=runtime");

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-status-strip")).toBeInTheDocument();
        });

        expect(screen.getByLabelText(/Verbose \/ Diagnostic View/i)).toBeChecked();
        expect(screen.getByLabelText(DIAGNOSTICS_SECTION_ARIA_LABEL)).toBeInTheDocument();
        expect(screen.getByTestId("diagnostics-section-select")).toHaveValue("runtime");
        expect(screen.queryByText("Configuration & Policy")).not.toBeInTheDocument();

        fireEvent.click(screen.getByTestId("diagnostics-filter-all"));
        fireEvent.click(screen.getByLabelText(/Verbose \/ Diagnostic View/i));
        fireEvent.change(screen.getByTestId("diagnostics-section-select"), { target: { value: "config" } });

        await waitFor(() => {
            expect(screen.getByTestId("location-search")).toHaveTextContent("?section=config");
        });
    });

    it("persists diagnostics query params across component rerenders", async () => {
        (getDiagnostics as any).mockResolvedValue(mockData);
        render(<DiagnosticsRerenderHarness initialPath="/diagnostics?filter=anomalies&debug=1&section=runtime" />);

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-status-strip")).toBeInTheDocument();
        });

        fireEvent.click(screen.getByTestId("diagnostics-filter-all"));
        fireEvent.click(screen.getByLabelText(/Verbose \/ Diagnostic View/i));
        fireEvent.change(screen.getByTestId("diagnostics-section-select"), { target: { value: "config" } });

        await waitFor(() => {
            expect(screen.getByTestId("location-search")).toHaveTextContent("?section=config");
        });

        fireEvent.click(screen.getByTestId("diagnostics-force-rerender"));

        expect(screen.getByTestId("location-search")).toHaveTextContent("?section=config");
        expect(screen.getByTestId("diagnostics-section-select")).toHaveValue("config");
    });

    it("shows diagnostics deep links and copy selected panel action", async () => {
        (getDiagnostics as any).mockResolvedValue(mockData);
        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-open-trace-search")).toBeInTheDocument();
        });

        expect(screen.getByTestId("diagnostics-open-trace-search")).toHaveAttribute("href", "/admin/traces/search");
        expect(screen.getByTestId("diagnostics-open-jobs-dashboard")).toHaveAttribute("href", "/admin/jobs");
        expect(screen.getByRole("button", { name: "Copy selected panel" })).toBeInTheDocument();
    });

    it("copies raw diagnostics JSON with expected keys", async () => {
        const writeText = vi.fn().mockResolvedValue(undefined);
        Object.defineProperty(window.navigator, "clipboard", {
            value: { writeText },
            configurable: true,
        });
        (getDiagnostics as any).mockResolvedValue({
            ...mockData,
            debug: { latency_breakdown_ms: { execute: 120 } },
        });
        renderDiagnostics("/diagnostics?debug=1&section=raw");

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-raw-json-summary")).toBeInTheDocument();
        });

        fireEvent.click(screen.getByTestId("diagnostics-raw-json-summary"));
        fireEvent.click(screen.getByRole("button", { name: "Copy JSON" }));

        await waitFor(() => {
            expect(writeText).toHaveBeenCalledTimes(1);
        });

        const copiedPayload = JSON.parse(String(writeText.mock.calls[0][0]));
        expect(copiedPayload).toHaveProperty("diagnostics_schema_version");
        expect(copiedPayload).toHaveProperty("runtime_indicators");
        expect(copiedPayload).toHaveProperty("enabled_flags");
    });

    it("renders safe placeholders for invalid numeric diagnostics values", async () => {
        (getDiagnostics as any).mockResolvedValue({
            ...mockData,
            schema_cache_ttl_seconds: Number.NaN,
            runtime_indicators: {
                ...mockData.runtime_indicators,
                avg_query_complexity: Number.NaN,
                active_schema_cache_size: Number.NaN,
                recent_truncation_event_count: Number.NaN,
            },
            debug: {
                latency_breakdown_ms: {
                    execute: Number.NaN,
                    router: -15,
                },
            },
        });

        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-status-strip")).toBeInTheDocument();
        });

        expect(screen.queryByText("NaN")).not.toBeInTheDocument();
        expect(screen.queryByText("undefineds")).not.toBeInTheDocument();
        expect(screen.getByText("0.00")).toBeInTheDocument();
    });

    it("renders trace and request identifier controls when diagnostics includes ids", async () => {
        const traceId = "0123456789abcdef0123456789abcdef";
        (getDiagnostics as any).mockResolvedValue({
            ...mockData,
            trace_id: traceId,
            request_id: "req-diagnostics-1",
        });
        renderDiagnostics();

        await waitFor(() => {
            expect(screen.getByTestId("diagnostics-status-strip")).toBeInTheDocument();
        });

        expect(screen.getByRole("link", { name: "View Trace" })).toHaveAttribute("href", `/traces/${traceId}`);
        expect(screen.getByRole("button", { name: "Copy trace id" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Copy request id" })).toBeInTheDocument();
    });
});
