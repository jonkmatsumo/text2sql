import React from "react";

interface Tab {
    id: string;
    label: string;
}

interface TabsProps {
    tabs: Tab[];
    activeTab: string;
    onChange: (id: string) => void;
}

export default function Tabs({ tabs, activeTab, onChange }: TabsProps) {
    return (
        <div className="tabs-container" style={{ display: "flex", gap: "24px", marginBottom: "24px", borderBottom: "1px solid var(--border)" }}>
            {tabs.map((tab) => (
                <button
                    key={tab.id}
                    onClick={() => onChange(tab.id)}
                    style={{
                        background: "none",
                        border: "none",
                        padding: "12px 4px",
                        cursor: "pointer",
                        fontSize: "1rem",
                        fontWeight: activeTab === tab.id ? 600 : 400,
                        color: activeTab === tab.id ? "var(--accent)" : "var(--muted)",
                        borderBottom: activeTab === tab.id ? "2px solid var(--accent)" : "2px solid transparent",
                        transition: "all 0.2s ease"
                    }}
                >
                    {tab.label}
                </button>
            ))}
        </div>
    );
}
