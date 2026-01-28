import React, { useState, useEffect } from "react";
import { PinRule } from "../types/admin";

export interface PinRuleFormProps {
    tenantId: number;
    initialData?: PinRule;
    onSuccess: () => void;
    onCancel: () => void;
    onSubmit: (data: Partial<PinRule> & { tenant_id: number }) => Promise<void>;
}

export default function PinRuleForm({
    tenantId,
    initialData,
    onSuccess,
    onCancel,
    onSubmit
}: PinRuleFormProps) {
    const [matchType, setMatchType] = useState<"exact" | "contains">(initialData?.match_type || "contains");
    const [matchValue, setMatchValue] = useState(initialData?.match_value || "");
    const [signatureKeys, setSignatureKeys] = useState(
        initialData?.registry_example_ids?.join("\n") || ""
    );
    const [priority, setPriority] = useState(initialData?.priority ?? 100);
    const [enabled, setEnabled] = useState(initialData?.enabled ?? true);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        if (initialData) {
            setMatchType(initialData.match_type);
            setMatchValue(initialData.match_value);
            setSignatureKeys(initialData.registry_example_ids?.join("\n") || "");
            setPriority(initialData.priority);
            setEnabled(initialData.enabled);
        }
    }, [initialData]);

    const parseSignatureKeys = (input: string): string[] => {
        return input
            .split(/[\n,]+/)
            .map((s) => s.trim())
            .filter((s) => s.length > 0);
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setError(null);

        if (!matchValue.trim()) {
            setError("Match value is required");
            return;
        }

        const exampleIds = parseSignatureKeys(signatureKeys);
        if (exampleIds.length === 0) {
            setError("At least one example ID is required");
            return;
        }

        setIsSubmitting(true);
        try {
            await onSubmit({
                id: initialData?.id,
                tenant_id: tenantId,
                match_type: matchType,
                match_value: matchValue.trim(),
                registry_example_ids: exampleIds,
                priority,
                enabled
            });
            onSuccess();
        } catch (err: any) {
            setError(err.message || "Failed to save pin rule");
        } finally {
            setIsSubmitting(false);
        }
    };

    return (
        <form onSubmit={handleSubmit} style={{ display: "grid", gap: "20px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 2fr", gap: "16px" }}>
                <div>
                    <label style={{ display: "block", fontWeight: 600, marginBottom: "8px" }}>
                        Match Type
                    </label>
                    <select
                        value={matchType}
                        onChange={(e) => setMatchType(e.target.value as "exact" | "contains")}
                        style={{
                            width: "100%",
                            padding: "10px 12px",
                            borderRadius: "8px",
                            border: "1px solid var(--border)",
                            fontSize: "0.95rem"
                        }}
                    >
                        <option value="exact">Exact</option>
                        <option value="contains">Contains</option>
                    </select>
                </div>

                <div>
                    <label style={{ display: "block", fontWeight: 600, marginBottom: "8px" }}>
                        Match Value
                    </label>
                    <input
                        type="text"
                        value={matchValue}
                        onChange={(e) => setMatchValue(e.target.value)}
                        placeholder="e.g., revenue, sales report"
                        style={{
                            width: "100%",
                            padding: "10px 12px",
                            borderRadius: "8px",
                            border: "1px solid var(--border)",
                            fontSize: "0.95rem"
                        }}
                    />
                </div>
            </div>

            <div>
                <label style={{ display: "block", fontWeight: 600, marginBottom: "8px" }}>
                    Registry Example IDs
                    <span style={{ fontWeight: 400, color: "var(--muted)", marginLeft: "8px", fontSize: "0.85rem" }}>
                        (one per line or comma-separated)
                    </span>
                </label>
                <textarea
                    value={signatureKeys}
                    onChange={(e) => setSignatureKeys(e.target.value)}
                    placeholder="example-id-1&#10;example-id-2"
                    style={{
                        width: "100%",
                        minHeight: "100px",
                        padding: "12px",
                        borderRadius: "8px",
                        border: "1px solid var(--border)",
                        fontSize: "0.9rem",
                        fontFamily: "monospace",
                        resize: "vertical"
                    }}
                />
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px" }}>
                <div>
                    <label style={{ display: "block", fontWeight: 600, marginBottom: "8px" }}>
                        Priority
                    </label>
                    <input
                        type="number"
                        value={priority}
                        onChange={(e) => setPriority(Number(e.target.value))}
                        min={0}
                        max={1000}
                        style={{
                            width: "100%",
                            padding: "10px 12px",
                            borderRadius: "8px",
                            border: "1px solid var(--border)",
                            fontSize: "0.95rem"
                        }}
                    />
                    <div style={{ fontSize: "0.8rem", color: "var(--muted)", marginTop: "4px" }}>
                        Higher priority rules are applied first
                    </div>
                </div>

                <div>
                    <label style={{ display: "block", fontWeight: 600, marginBottom: "8px" }}>
                        Status
                    </label>
                    <label style={{ display: "flex", alignItems: "center", gap: "8px", cursor: "pointer" }}>
                        <input
                            type="checkbox"
                            checked={enabled}
                            onChange={(e) => setEnabled(e.target.checked)}
                            style={{ width: "18px", height: "18px" }}
                        />
                        <span>Enabled</span>
                    </label>
                </div>
            </div>

            {error && (
                <div className="error-banner">{error}</div>
            )}

            <div style={{ display: "flex", gap: "12px", justifyContent: "flex-end" }}>
                <button
                    type="button"
                    onClick={onCancel}
                    style={{
                        padding: "10px 20px",
                        borderRadius: "8px",
                        border: "1px solid var(--border)",
                        background: "var(--surface)",
                        cursor: "pointer",
                        fontWeight: 500
                    }}
                >
                    Cancel
                </button>
                <button
                    type="submit"
                    disabled={isSubmitting}
                    style={{
                        padding: "10px 24px",
                        borderRadius: "8px",
                        border: "none",
                        background: "var(--accent)",
                        color: "#fff",
                        cursor: isSubmitting ? "not-allowed" : "pointer",
                        fontWeight: 600,
                        opacity: isSubmitting ? 0.7 : 1
                    }}
                >
                    {isSubmitting ? "Saving..." : initialData ? "Update Rule" : "Create Rule"}
                </button>
            </div>
        </form>
    );
}
