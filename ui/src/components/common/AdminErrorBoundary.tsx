import React, { Component, ErrorInfo, ReactNode } from "react";
import { Outlet } from "react-router-dom";

interface Props {
    children?: ReactNode;
}

interface State {
    hasError: boolean;
}

/**
 * AdminErrorBoundary provides a graceful fallback UI for unhandled exceptions
 * occurring specifically within the Operator Console routes.
 */
export class AdminErrorBoundary extends Component<Props, State> {
    public state: State = {
        hasError: false,
    };

    public static getDerivedStateFromError(_: Error): State {
        return { hasError: true };
    }

    public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
        console.error("Uncaught error in Admin Route:", error, errorInfo);
    }

    private handleReset = () => {
        this.setState({ hasError: false });
        window.location.reload();
    };

    public render() {
        if (this.state.hasError) {
            return (
                <div className="min-h-[400px] flex items-center justify-center p-4 bg-gray-50 dark:bg-gray-900 border-2 border-dashed border-red-200 dark:border-red-900 rounded-lg m-4">
                    <div className="text-center max-w-md">
                        <div className="mb-4 flex justify-center">
                            <div className="p-3 bg-red-100 dark:bg-red-900/30 rounded-full">
                                <svg
                                    className="w-12 h-12 text-red-600 dark:text-red-500"
                                    fill="none"
                                    stroke="currentColor"
                                    viewBox="0 0 24 24"
                                >
                                    <path
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                        strokeWidth={2}
                                        d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                                    />
                                </svg>
                            </div>
                        </div>
                        <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100 mb-2">
                            Something went wrong
                        </h2>
                        <p className="text-gray-600 dark:text-gray-400 mb-8">
                            An unexpected error occurred in the Operator Console. This may be due
                            to contract drift or a runtime guard violation.
                        </p>
                        <div className="flex flex-col sm:flex-row justify-center gap-4">
                            <button
                                onClick={this.handleReset}
                                className="px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-medium rounded-md transition-colors shadow-sm"
                            >
                                Refresh Console
                            </button>
                            <button
                                onClick={() => window.history.back()}
                                className="px-6 py-2 border border-gray-300 dark:border-gray-700 text-gray-700 dark:text-gray-300 font-medium rounded-md hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors shadow-sm"
                            >
                                Go Back
                            </button>
                        </div>
                    </div>
                </div>
            );
        }

        return this.props.children || <Outlet />;
    }
}
