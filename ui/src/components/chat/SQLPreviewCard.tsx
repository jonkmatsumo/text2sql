import React from "react";

interface SQLPreviewCardProps {
    sql: string;
    onRun: () => void;
    onBack: () => void;
    isExecuting?: boolean;
    onSqlChange?: (newSql: string) => void;
    isEditable?: boolean;
}

export const SQLPreviewCard: React.FC<SQLPreviewCardProps> = ({
    sql,
    onRun,
    onBack,
    isExecuting = false,
    onSqlChange,
    isEditable = true,
}) => {
    return (
        <div className="p-4 border rounded-lg bg-gray-50 dark:bg-gray-900 border-gray-200 dark:border-gray-800 my-4 shadow-sm">
            <h3 className="text-sm font-semibold mb-2 text-gray-700 dark:text-gray-300 flex items-center justify-between">
                <span>SQL Preview</span>
                <span className={`text-xs font-normal px-2 py-0.5 rounded ${isEditable ? "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200" : "bg-gray-100 text-gray-500 dark:bg-gray-800"}`}>
                    {isEditable ? "Editable" : "Read-only"}
                </span>
            </h3>
            <div className="relative mb-4 group">
                <textarea
                    className="w-full p-3 h-48 bg-white dark:bg-black rounded border border-gray-300 dark:border-gray-700 font-mono text-sm text-gray-800 dark:text-gray-200 shadow-inner focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 resize-y"
                    value={sql}
                    onChange={(e) => onSqlChange?.(e.target.value)}
                    readOnly={!isEditable || isExecuting}
                    spellCheck={false}
                />
            </div>
            <div className="flex justify-end space-x-3">
                <button
                    onClick={onBack}
                    disabled={isExecuting}
                    className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 transition-colors"
                >
                    Back
                </button>
                <button
                    onClick={onRun}
                    disabled={isExecuting}
                    className="px-4 py-2 text-sm font-medium text-white bg-indigo-600 rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 flex items-center shadow-sm transition-colors"
                >
                    {isExecuting ? (
                        <>
                            <svg
                                className="animate-spin -ml-1 mr-2 h-4 w-4 text-white"
                                xmlns="http://www.w3.org/2000/svg"
                                fill="none"
                                viewBox="0 0 24 24"
                            >
                                <circle
                                    className="opacity-25"
                                    cx="12"
                                    cy="12"
                                    r="10"
                                    stroke="currentColor"
                                    strokeWidth="4"
                                ></circle>
                                <path
                                    className="opacity-75"
                                    fill="currentColor"
                                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                                ></path>
                            </svg>
                            Running...
                        </>
                    ) : (
                        "Run SQL"
                    )}
                </button>
            </div>
        </div>
    );
};
