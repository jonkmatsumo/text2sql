import React from "react";
import { Link } from "react-router-dom";
import { getErrorMapping } from "../../utils/errorMapping";

export interface WorkflowGuidanceProps {
    category?: string;
}

/**
 * Renders contextual guidance and CTAs for actionable error categories.
 * Consumes errorMapping.ts as the single source of truth for descriptions and actions.
 */
// Simple set to track rendered categories in the current pass to prevent duplicates.
// This is a simple defensive measure for components that might render WorkflowGuidance in a loop.
const renderedCategories = new Set<string>();

/** @internal - for testing only */
export function __clearGuidanceCache() {
    renderedCategories.clear();
}

export const WorkflowGuidance: React.FC<WorkflowGuidanceProps> = ({ category }) => {
    if (!category) return null;

    // Deduplication: prevent redundant guidance in multi-error views
    if (renderedCategories.has(category)) {
        return null;
    }

    const mapping = getErrorMapping(category);

    // Only render if there is guidance content (description + at least one action)
    if (!mapping.description || !mapping.guidanceActions?.length) return null;

    // Mark as rendered and clear it after a short delay to allow fresh rendering on future passes
    renderedCategories.add(category);
    setTimeout(() => renderedCategories.delete(category), 1000);

    return (
        <div className="mt-4 p-5 bg-indigo-50 dark:bg-indigo-950/30 border border-indigo-100 dark:border-indigo-900/50 rounded-xl shadow-sm">
            <div className="flex justify-between items-start mb-2">
                <h4 className="text-sm font-bold text-indigo-900 dark:text-indigo-200 uppercase tracking-tight flex items-center gap-2">
                    <span className="text-lg">💡</span> {mapping.title}
                </h4>
                <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${mapping.severity === "error"
                    ? "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300"
                    : mapping.severity === "warn"
                        ? "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300"
                        : "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300"
                    }`}>
                    {mapping.severity}
                </span>
            </div>
            <p className="mt-2 text-sm text-indigo-800 dark:text-indigo-300 leading-relaxed">
                {mapping.description}
            </p>
            <div className="mt-4 flex flex-wrap gap-3">
                {mapping.guidanceActions.map((action, idx) => (
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
