import React, { useEffect, useState, useCallback } from "react";
import { Link, useLocation } from "react-router-dom";
import OtelHealthBanner from "./OtelHealthBanner";
import { fetchQueryTargetSettings } from "../../api";

interface LayoutProps {
    children: React.ReactNode;
}

type HealthStatus = "loading" | "healthy" | "not_configured" | "error";

const HEALTH_COLORS: Record<HealthStatus, string> = {
    loading: "#94a3b8",
    healthy: "#10b981",
    not_configured: "#f59e0b",
    error: "#ef4444",
};

const HEALTH_LABELS: Record<HealthStatus, string> = {
    loading: "Checking...",
    healthy: "System Online",
    not_configured: "Not Configured",
    error: "Connection Failed",
};

const navItems = [
    { path: "/", label: "Agent Chat", icon: "💬" },
    { path: "/admin/review", label: "Review & Curation", icon: "📝" },
    { path: "/admin/recommendations", label: "Recommendations", icon: "✨" },
    { path: "/admin/operations", label: "System Operations", icon: "⚙️" },
    { path: "/admin/runs", label: "Run History", icon: "🕒" },
    { path: "/admin/jobs", label: "Unified Jobs", icon: "📋" },
    { path: "/admin/settings/query-target", label: "Query Target Settings", icon: "🧭" },
    { path: "/admin/traces", label: "Trace Explorer", icon: "🔍" },
    { path: "/admin/diagnostics", label: "Diagnostics", icon: "🏥" }
];

export default function Layout({ children }: LayoutProps) {
    const location = useLocation();
    const [healthStatus, setHealthStatus] = useState<HealthStatus>("loading");

    const checkHealth = useCallback(async () => {
        try {
            const settings = await fetchQueryTargetSettings();
            if (settings.active) {
                setHealthStatus("healthy");
            } else {
                setHealthStatus("not_configured");
            }
        } catch {
            setHealthStatus("error");
        }
    }, []);

    useEffect(() => {
        checkHealth();
        const interval = setInterval(checkHealth, 30000);
        return () => clearInterval(interval);
    }, [checkHealth]);

    return (
        <>
            <OtelHealthBanner />
            <div style={{ display: "flex", minHeight: "100vh" }}>
                {/* Sidebar */}
                <aside
                    style={{
                        width: "280px",
                        backgroundColor: "var(--surface)",
                        borderRight: "1px solid var(--border)",
                        padding: "32px 20px",
                        display: "flex",
                        flexDirection: "column",
                        position: "sticky",
                        top: 0,
                        height: "100vh"
                    }}
                >
                    <div style={{ marginBottom: "48px", padding: "0 12px" }}>
                        <div className="kicker" style={{ marginBottom: "4px" }}>Text2SQL</div>
                        <div style={{ fontFamily: "'Space Grotesk', sans-serif", fontSize: "1.5rem", fontWeight: 700 }}>
                            Dashboard
                        </div>
                    </div>

                    <nav style={{ flex: 1 }}>
                        <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "grid", gap: "8px" }}>
                            {navItems.map((item) => {
                                const isActive = location.pathname === item.path;
                                return (
                                    <li key={item.path}>
                                        <Link
                                            to={item.path}
                                            style={{
                                                display: "flex",
                                                alignItems: "center",
                                                gap: "12px",
                                                padding: "12px 16px",
                                                borderRadius: "12px",
                                                textDecoration: "none",
                                                color: isActive ? "var(--ink)" : "var(--muted)",
                                                backgroundColor: isActive ? "var(--surface-muted)" : "transparent",
                                                fontWeight: isActive ? 600 : 400,
                                                transition: "all 0.2s ease"
                                            }}
                                        >
                                            <span style={{ fontSize: "1.2rem" }}>{item.icon}</span>
                                            <span>{item.label}</span>
                                        </Link>
                                    </li>
                                );
                            })}
                        </ul>
                    </nav>

                    <div style={{ marginTop: "auto", padding: "20px 12px", borderTop: "1px solid var(--border)", color: "var(--muted)", fontSize: "0.85rem" }}>
                        <Link
                            to="/admin/settings/query-target"
                            data-testid="health-badge"
                            style={{
                                display: "flex",
                                alignItems: "center",
                                gap: "8px",
                                textDecoration: "none",
                                color: "inherit",
                            }}
                        >
                            <div
                                data-testid="health-dot"
                                className={`status-dot ${healthStatus === "healthy" ? "status-dot--pulse" : healthStatus === "error" ? "status-dot--error-pulse" : ""}`}
                                style={{
                                    backgroundColor: HEALTH_COLORS[healthStatus],
                                }}
                            />
                            {HEALTH_LABELS[healthStatus]}
                        </Link>
                    </div>
                </aside>

                {/* Main Content */}
                <main style={{ flex: 1, backgroundColor: "var(--bg)", overflow: "auto" }}>
                    <div className="page" style={{ padding: "48px 40px", maxWidth: "1200px" }}>
                        {children}
                    </div>
                </main>
            </div>
        </>
    );
}
