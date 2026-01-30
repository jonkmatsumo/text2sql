import React, { useEffect, useMemo, useState, useCallback } from "react";
import Tabs from "../components/common/Tabs";
import DataTable from "../components/common/DataTable";
import Modal from "../components/common/Modal";
import FilterSelect from "../components/common/FilterSelect";
import TraceLink from "../components/common/TraceLink";
import VirtualList from "../components/common/VirtualList";
import { AdminService } from "../api";
import { Interaction, ApprovedExample } from "../types/admin";
import { useQueryParams } from "../hooks/useQueryParams";
import { useToast } from "../hooks/useToast";

const THUMB_OPTIONS = [
    { value: "All", label: "All" },
    { value: "UP", label: "UP" },
    { value: "DOWN", label: "DOWN" },
    { value: "None", label: "None" }
];

const STATUS_OPTIONS = [
    { value: "All", label: "All" },
    { value: "PENDING", label: "PENDING" },
    { value: "APPROVED", label: "APPROVED" },
    { value: "REJECTED", label: "REJECTED" }
];

const DEFAULT_FILTERS = {
    thumb: "All",
    status: "PENDING"
};

export default function ReviewCuration() {
    const [activeTab, setActiveTab] = useState("inbox");
    const [interactions, setInteractions] = useState<Interaction[]>([]);
    const [examples, setExamples] = useState<ApprovedExample[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [selectedInteraction, setSelectedInteraction] = useState<Interaction | null>(null);
    const [isDetailOpen, setIsDetailOpen] = useState(false);
    const [correctedSql, setCorrectedSql] = useState("");
    const [reviewerNotes, setReviewerNotes] = useState("");
    const [searchQuery, setSearchQuery] = useState("");

    const [filters, setFilters] = useQueryParams(DEFAULT_FILTERS);
    const { show: showToast } = useToast();

    const tabs = [
        { id: "inbox", label: "Inbox" },
        { id: "publication", label: "Publication" },
        { id: "registry", label: "Registry" }
    ];

    const loadInteractions = async (status: string = "PENDING", thumb: string = "All") => {
        setIsLoading(true);
        try {
            const data = await AdminService.listInteractions(50, thumb, status);
            setInteractions(data);
        } catch (err) {
            console.error(err);
            showToast("Failed to load interactions", "error");
        } finally {
            setIsLoading(false);
        }
    };

    const loadExamples = async () => {
        setIsLoading(true);
        try {
            const data = await AdminService.listExamples(100, searchQuery);
            setExamples(data);
        } catch (err) {
            console.error(err);
            showToast("Failed to load examples", "error");
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        if (activeTab === "inbox") {
            loadInteractions(filters.status, filters.thumb);
        } else if (activeTab === "publication") {
            loadInteractions("APPROVED", "All");
        } else if (activeTab === "registry") {
            loadExamples();
        }
    }, [activeTab, filters.thumb, filters.status, searchQuery]);

    const handleRowClick = useCallback(async (item: Interaction) => {
        setIsLoading(true);
        try {
            const details = await AdminService.getInteractionDetails(item.id);
            setSelectedInteraction(details);
            setCorrectedSql(details.generated_sql || "");
            setReviewerNotes("");
            setIsDetailOpen(true);
        } catch (err) {
            showToast("Failed to load interaction details", "error");
        } finally {
            setIsLoading(false);
        }
    }, [showToast]);

    const handleApprove = async () => {
        if (!selectedInteraction) return;
        try {
            await AdminService.approveInteraction(
                selectedInteraction.id,
                correctedSql,
                selectedInteraction.generated_sql,
                reviewerNotes
            );
            setIsDetailOpen(false);
            showToast("Interaction approved", "success");
            loadInteractions(filters.status, filters.thumb);
        } catch (err) {
            showToast("Failed to approve", "error");
        }
    };

    const handleReject = async () => {
        if (!selectedInteraction) return;
        try {
            await AdminService.rejectInteraction(selectedInteraction.id, "CANNOT_FIX", reviewerNotes);
            setIsDetailOpen(false);
            showToast("Interaction rejected", "success");
            loadInteractions(filters.status, filters.thumb);
        } catch (err) {
            showToast("Failed to reject", "error");
        }
    };

    const handlePublish = async () => {
        setIsLoading(true);
        try {
            const result = await AdminService.publishApproved();
            showToast(`Published ${result.published} examples!`, "success");
            loadInteractions("APPROVED", "All");
        } catch (err) {
            showToast("Failed to publish", "error");
        } finally {
            setIsLoading(false);
        }
    };

    const parsedResponsePayload = useMemo(() => {
        if (!selectedInteraction?.response_payload) return null;
        try {
            return JSON.parse(selectedInteraction.response_payload);
        } catch {
            return selectedInteraction.response_payload;
        }
    }, [selectedInteraction?.response_payload]);

    const renderInteractionRow = useCallback((item: Interaction, index: number) => (
        <div
            key={item.id}
            onClick={() => handleRowClick(item)}
            style={{
                display: "grid",
                gridTemplateColumns: "2fr 2fr 80px 100px 140px 60px",
                gap: "12px",
                padding: "12px 16px",
                borderBottom: "1px solid var(--border)",
                cursor: "pointer",
                backgroundColor: index % 2 === 0 ? "transparent" : "var(--surface-muted)",
                alignItems: "center",
                height: "56px",
                boxSizing: "border-box"
            }}
            onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = "var(--surface-muted)"; }}
            onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = index % 2 === 0 ? "transparent" : "var(--surface-muted)"; }}
        >
            <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {item.user_nlq_text || "—"}
            </div>
            <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                <code style={{ fontSize: "0.8rem" }}>{item.generated_sql_preview || "—"}</code>
            </div>
            <div>{item.thumb || "—"}</div>
            <div>{item.execution_status || "—"}</div>
            <div>{item.created_at ? item.created_at.replace("T", " ").slice(0, 16) : "—"}</div>
            <div>
                {(item.trace_id || item.id) ? (
                    <TraceLink
                        traceId={item.trace_id}
                        interactionId={item.id}
                        variant="icon"
                        showCopy={false}
                    />
                ) : (
                    <span style={{ color: "var(--muted)" }}>-</span>
                )}
            </div>
        </div>
    ), [handleRowClick]);

    return (
        <>
            <header className="hero">
                <div>
                    <p className="kicker">Admin Panel</p>
                    <h1>Review & Curation</h1>
                    <p className="subtitle">
                        Manage recent interactions and the few-shot example registry.
                    </p>
                </div>
            </header>

            <Tabs tabs={tabs} activeTab={activeTab} onChange={setActiveTab} />

            {activeTab === "inbox" && (
                <>
                    <div className="filter-bar">
                        <FilterSelect
                            label="Thumb"
                            value={filters.thumb}
                            options={THUMB_OPTIONS}
                            onChange={(value) => setFilters({ thumb: value })}
                        />
                        <FilterSelect
                            label="Status"
                            value={filters.status}
                            options={STATUS_OPTIONS}
                            onChange={(value) => setFilters({ status: value })}
                        />
                    </div>

                    {isLoading && <div className="loading">Loading data...</div>}

                    {!isLoading && interactions.length === 0 && (
                        <div className="loading" style={{ padding: "40px" }}>No items found.</div>
                    )}

                    {!isLoading && interactions.length > 0 && (
                        <div className="table-wrap">
                            {/* Static header */}
                            <div
                                style={{
                                    display: "grid",
                                    gridTemplateColumns: "2fr 2fr 80px 100px 140px 60px",
                                    gap: "12px",
                                    padding: "12px 16px",
                                    backgroundColor: "var(--surface-muted)",
                                    borderBottom: "1px solid var(--border)",
                                    fontWeight: 600,
                                    fontSize: "0.85rem",
                                    color: "var(--muted)"
                                }}
                            >
                                <div>NLQ Text</div>
                                <div>SQL Preview</div>
                                <div>Feedback</div>
                                <div>Status</div>
                                <div>Created At</div>
                                <div>Trace</div>
                            </div>
                            {/* Virtualized rows */}
                            <VirtualList
                                items={interactions}
                                rowHeight={56}
                                height={Math.min(interactions.length * 56, 600)}
                                overscan={6}
                                renderRow={renderInteractionRow}
                            />
                        </div>
                    )}
                </>
            )}

            {activeTab === "publication" && (
                <>
                    <div style={{ marginBottom: "20px", display: "flex", justifyContent: "flex-end" }}>
                        <button
                            className="feedback button"
                            onClick={handlePublish}
                            disabled={isLoading || interactions.length === 0}
                            style={{ background: "var(--accent)", color: "#fff", padding: "10px 20px", borderRadius: "10px", border: "none", cursor: "pointer" }}
                        >
                            Sync Approved to Registry
                        </button>
                    </div>
                    <DataTable
                        data={interactions}
                        isLoading={isLoading}
                        columns={[
                            { header: "NLQ Text", key: "user_nlq_text" },
                            { header: "SQL Preview", key: "generated_sql_preview" },
                            { header: "Status", key: "execution_status" },
                            { header: "Created At", key: "created_at" }
                        ]}
                    />
                </>
            )}

            {activeTab === "registry" && (
                <>
                    <div className="composer" style={{ marginBottom: "20px" }}>
                        <input
                            type="text"
                            placeholder="Search examples..."
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                        />
                    </div>
                    <DataTable
                        data={examples}
                        isLoading={isLoading}
                        columns={[
                            { header: "Question", key: "question" },
                            { header: "SQL Query", key: "sql_query" },
                            { header: "Status", key: "status" },
                            { header: "Created At", key: "created_at" }
                        ]}
                    />
                </>
            )}

            <Modal
                isOpen={isDetailOpen}
                onClose={() => setIsDetailOpen(false)}
                title="Review Interaction"
            >
                {selectedInteraction && (
                    <div style={{ display: "grid", gap: "24px" }}>
                        <div>
                            <label style={{ fontWeight: 600, display: "block", marginBottom: "8px" }}>User Query</label>
                            <div className="bubble user" style={{ padding: "16px" }}>{selectedInteraction.user_nlq_text}</div>
                        </div>

                        <div>
                            <label style={{ fontWeight: 600, display: "block", marginBottom: "8px" }}>Corrected SQL</label>
                            <textarea
                                value={correctedSql}
                                onChange={(e) => setCorrectedSql(e.target.value)}
                                style={{
                                    width: "100%",
                                    height: "200px",
                                    fontFamily: "monospace",
                                    padding: "16px",
                                    borderRadius: "12px",
                                    border: "1px solid var(--border)",
                                    backgroundColor: "#1e1e20",
                                    color: "#fff"
                                }}
                            />
                        </div>

                        {parsedResponsePayload && (
                            <div>
                                <label style={{ fontWeight: 600, display: "block", marginBottom: "8px" }}>Response Payload</label>
                                <pre style={{
                                    background: "#1e1e20",
                                    color: "#fefefe",
                                    padding: "12px",
                                    borderRadius: "12px",
                                    overflow: "auto",
                                    fontSize: "0.85rem",
                                    maxHeight: "200px"
                                }}>
                                    {typeof parsedResponsePayload === "string"
                                        ? parsedResponsePayload
                                        : JSON.stringify(parsedResponsePayload, null, 2)}
                                </pre>
                            </div>
                        )}

                        <div>
                            <label style={{ fontWeight: 600, display: "block", marginBottom: "8px" }}>Metadata</label>
                            <div style={{
                                display: "grid",
                                gridTemplateColumns: "repeat(3, 1fr)",
                                gap: "12px",
                                padding: "16px",
                                background: "var(--surface-muted)",
                                borderRadius: "12px",
                                border: "1px solid var(--border)"
                            }}>
                                <div>
                                    <div style={{ fontSize: "0.75rem", color: "var(--muted)", textTransform: "uppercase" }}>Model</div>
                                    <div style={{ fontWeight: 500 }}>{selectedInteraction.model_version || "-"}</div>
                                </div>
                                <div>
                                    <div style={{ fontSize: "0.75rem", color: "var(--muted)", textTransform: "uppercase" }}>Status</div>
                                    <div style={{ fontWeight: 500 }}>{selectedInteraction.execution_status}</div>
                                </div>
                                <div>
                                    <div style={{ fontSize: "0.75rem", color: "var(--muted)", textTransform: "uppercase" }}>Tables Used</div>
                                    <div style={{ fontWeight: 500, fontSize: "0.9rem" }}>
                                        {selectedInteraction.tables_used?.join(", ") || "-"}
                                    </div>
                                </div>
                            </div>
                        </div>

                        {selectedInteraction.feedback && selectedInteraction.feedback.length > 0 && (
                            <div>
                                <label style={{ fontWeight: 600, display: "block", marginBottom: "8px" }}>Feedback</label>
                                <div style={{ display: "grid", gap: "8px" }}>
                                    {selectedInteraction.feedback.map((fb, idx) => (
                                        <div
                                            key={idx}
                                            style={{
                                                padding: "12px",
                                                background: fb.thumb === "UP" ? "#ecfdf3" : "#fef3f2",
                                                borderRadius: "8px",
                                                border: `1px solid ${fb.thumb === "UP" ? "#bbf7d0" : "#fecaca"}`
                                            }}
                                        >
                                            <span style={{ fontWeight: 600, color: fb.thumb === "UP" ? "#0f5132" : "#b42318" }}>
                                                {fb.thumb}
                                            </span>
                                            {fb.comment && (
                                                <p style={{ margin: "8px 0 0", fontSize: "0.9rem" }}>{fb.comment}</p>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {(selectedInteraction.trace_id || selectedInteraction.id) && (
                            <div>
                                <label style={{ fontWeight: 600, display: "block", marginBottom: "8px" }}>Trace</label>
                                <TraceLink
                                    traceId={selectedInteraction.trace_id}
                                    interactionId={selectedInteraction.id}
                                    variant="button"
                                />
                            </div>
                        )}

                        <div>
                            <label style={{ fontWeight: 600, display: "block", marginBottom: "8px" }}>Reviewer Notes</label>
                            <input
                                type="text"
                                value={reviewerNotes}
                                onChange={(e) => setReviewerNotes(e.target.value)}
                                placeholder="Why are you making this change?"
                                style={{
                                    width: "100%",
                                    padding: "12px",
                                    borderRadius: "12px",
                                    border: "1px solid var(--border)"
                                }}
                            />
                        </div>

                        <div style={{ display: "flex", gap: "16px", marginTop: "12px" }}>
                            <button
                                onClick={handleApprove}
                                style={{ flex: 1, backgroundColor: "var(--accent)", color: "#fff", border: "none", padding: "12px", borderRadius: "10px", cursor: "pointer", fontWeight: 600 }}
                            >
                                Approve
                            </button>
                            <button
                                onClick={handleReject}
                                style={{ flex: 1, backgroundColor: "#b42318", color: "#fff", border: "none", padding: "12px", borderRadius: "10px", cursor: "pointer", fontWeight: 600 }}
                            >
                                Reject
                            </button>
                        </div>
                    </div>
                )}
            </Modal>
        </>
    );
}
