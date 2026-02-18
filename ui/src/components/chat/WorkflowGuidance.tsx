import React from "react";
import { Link } from "react-router-dom";

export interface WorkflowGuidanceProps {
    category?: string;
    details?: Record<string, any>;
}

export const WorkflowGuidance: React.FC<WorkflowGuidanceProps> = ({ category, details }) => {
    if (!category) return null;

    const normalizedCategory = category.toLowerCase();

    // Mapping of categories to specific guidance content
    const getGuidance = () => {
        switch (normalizedCategory) {
            case "schema_missing":
                return {
                    title: "Missing Schema Guidance",
                    description: "The agent couldn't find the necessary table schema. You may need to ingest the table or refresh the system metadata.",
                    actions: [
                        { label: "Go to Ingestion Wizard", href: "/admin/operations?tab=ingestion", primary: true },
                        { label: "Check Schema Hydration", href: "/admin/operations?tab=schema" }
                    ]
                };
            case "schema_drift":
                return {
                    title: "Schema Drift Detected",
                    description: "The database schema appears to have changed. Updating the system's metadata snapshot might resolve this.",
                    actions: [
                        { label: "Run Schema Hydration", href: "/admin/operations?tab=schema", primary: true },
                        { label: "Reload NLP Patterns", href: "/admin/operations?tab=nlp" }
                    ]
                };
            case "connectivity":
                return {
                    title: "Connection Trouble",
                    description: "The system is having trouble reaching the target database. Please check your connection strings and credentials.",
                    actions: [
                        { label: "Verify Target Settings", href: "/admin/settings/query-target", primary: true },
                        { label: "Check Connectivity Diagnostics", href: "/admin/diagnostics" }
                    ]
                };
            case "budget_exhausted":
            case "budget_exceeded":
                return {
                    title: "Usage Quota Reached",
                    description: "You have reached your allocated usage quota. You can request a limit increase or wait for the next billing cycle.",
                    actions: [
                        { label: "Manage Quotas", href: "/admin/settings/query-target", primary: true }
                    ]
                };
            default:
                return null;
        }
    };

    const guidance = getGuidance();
    if (!guidance) return null;

    return (
        <div className="mt-4 p-5 bg-indigo-50 dark:bg-indigo-950/30 border border-indigo-100 dark:border-indigo-900/50 rounded-xl shadow-sm">
            <h4 className="text-sm font-bold text-indigo-900 dark:text-indigo-200 uppercase tracking-tight flex items-center gap-2">
                <span className="text-lg">💡</span> {guidance.title}
            </h4>
            <p className="mt-2 text-sm text-indigo-800 dark:text-indigo-300 leading-relaxed">
                {guidance.description}
            </p>
            <div className="mt-4 flex flex-wrap gap-3">
                {guidance.actions.map((action, idx) => (
                    <Link
                        key={idx}
                        to={action.href}
                        className={`px-4 py-2 rounded-lg text-sm font-semibold transition-all shadow-sm ${action.primary
                                ? "bg-indigo-600 hover:bg-indigo-700 text-white"
                                : "bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700 text-indigo-700 dark:text-indigo-300 border border-indigo-200 dark:border-indigo-800"
                            }`}
                    >
                        {action.label}
                    </Link>
                ))}
            </div>
        </div>
    );
};
