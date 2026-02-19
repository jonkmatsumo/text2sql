import React, { createContext, useContext, useRef, useCallback } from "react";
import { Link } from "react-router-dom";
import { getErrorMapping } from "../../utils/errorMapping";

export interface WorkflowGuidanceProps {
    category?: string;
}

interface WorkflowGuidanceContextType {
    shouldRender: (category: string) => boolean;
}

const WorkflowGuidanceContext = createContext<WorkflowGuidanceContextType | null>(null);

const DEDUPE_TTL_MS = 1000;

export const WorkflowGuidanceProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    // category -> lastShownAtMs
    const renderedAt = useRef<Map<string, number>>(new Map());

    const shouldRender = useCallback((category: string) => {
        const now = Date.now();
        const lastShown = renderedAt.current.get(category);

        if (lastShown && now - lastShown < DEDUPE_TTL_MS) {
            return false;
        }

        renderedAt.current.set(category, now);
        return true;
    }, []);

    return (
        <WorkflowGuidanceContext.Provider value={{ shouldRender }}>
            {children}
        </WorkflowGuidanceContext.Provider>
    );
};

function useWorkflowGuidanceDedupe(category?: string): boolean {
    const context = useContext(WorkflowGuidanceContext);
    if (!context || !category) return true; // Fallback to allow rendering if no provider or no category
    return context.shouldRender(category);
}

/**
 * Renders contextual guidance and CTAs for actionable error categories.
 * Consumes errorMapping.ts as the single source of truth for descriptions and actions.
 */

export const WorkflowGuidance: React.FC<WorkflowGuidanceProps> = ({ category }) => {
    const canRender = useWorkflowGuidanceDedupe(category);

    if (!category || !canRender) return null;

    const mapping = getErrorMapping(category);

    // Only render if there is guidance content (description + at least one action)
    if (!mapping.description || !mapping.guidanceActions?.length) return null;

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
