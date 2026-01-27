import React, { useEffect, useState } from "react";
import Tabs from "../components/common/Tabs";
import DataTable from "../components/common/DataTable";
import Modal from "../components/common/Modal";
import { AdminService } from "../api";
import { Interaction, ApprovedExample } from "../types/admin";

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

    const tabs = [
        { id: "inbox", label: "📥 Inbox" },
        { id: "publication", label: "🚀 Publication" },
        { id: "registry", label: "📚 Registry" }
    ];

    const loadInteractions = async (status: string = "PENDING") => {
        setIsLoading(true);
        try {
            const data = await AdminService.listInteractions(50, "All", status);
            setInteractions(data);
        } catch (err) {
            console.error(err);
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
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        if (activeTab === "inbox") {
            loadInteractions("PENDING");
        } else if (activeTab === "publication") {
            loadInteractions("APPROVED");
        } else if (activeTab === "registry") {
            loadExamples();
        }
    }, [activeTab, searchQuery]);

    const handleRowClick = (item: Interaction) => {
        setSelectedInteraction(item);
        setCorrectedSql(item.generated_sql || "");
        setReviewerNotes("");
        setIsDetailOpen(true);
    };

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
            loadInteractions("PENDING");
        } catch (err) {
            alert("Failed to approve");
        }
    };

    const handleReject = async () => {
        if (!selectedInteraction) return;
        try {
            await AdminService.rejectInteraction(selectedInteraction.id, "CANNOT_FIX", reviewerNotes);
            setIsDetailOpen(false);
            loadInteractions("PENDING");
        } catch (err) {
            alert("Failed to reject");
        }
    };

    const handlePublish = async () => {
        setIsLoading(true);
        try {
            const result = await AdminService.publishApproved();
            alert(`Published ${result.published} examples!`);
            loadInteractions("APPROVED");
        } catch (err) {
            alert("Failed to publish");
        } finally {
            setIsLoading(false);
        }
    };

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
                <DataTable
                    data={interactions}
                    isLoading={isLoading}
                    onRowClick={handleRowClick}
                    columns={[
                        { header: "NLQ Text", key: "user_nlq_text" },
                        {
                            header: "SQL Preview",
                            key: "generated_sql_preview",
                            render: (row) => <code style={{ fontSize: "0.8rem" }}>{row.generated_sql_preview}</code>
                        },
                        { header: "Feedback", key: "thumb" },
                        {
                            header: "Created At",
                            key: "created_at",
                            render: (row) => row.created_at.replace("T", " ").slice(0, 16)
                        }
                    ]}
                />
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
                            🚀 Sync Approved to Registry
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
