import React, { useRef, useEffect } from "react";
import type { ShortcutDef } from "../../hooks/useOperatorShortcuts";

interface KeyboardShortcutsModalProps {
    isOpen: boolean;
    onClose: () => void;
    shortcuts: ShortcutDef[];
}

/**
 * Displays a table of available keyboard shortcuts for operator pages.
 * Opens via the `?` key or a help icon button.
 * Closes on Escape or backdrop click.
 */
export function KeyboardShortcutsModal({ isOpen, onClose, shortcuts }: KeyboardShortcutsModalProps) {
    const closeButtonRef = useRef<HTMLButtonElement>(null);

    // Trap focus and handle Escape
    useEffect(() => {
        if (!isOpen) return;

        // Focus the close button when modal opens
        closeButtonRef.current?.focus();

        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === "Escape") {
                e.stopPropagation();
                onClose();
            }
        };

        window.addEventListener("keydown", handleKeyDown, true);
        return () => window.removeEventListener("keydown", handleKeyDown, true);
    }, [isOpen, onClose]);

    if (!isOpen) return null;

    return (
        <div
            role="dialog"
            aria-modal="true"
            aria-label="Keyboard shortcuts"
            className="fixed inset-0 z-50 flex items-center justify-center p-4"
            style={{ backgroundColor: "rgba(0,0,0,0.45)", backdropFilter: "blur(4px)" }}
            onClick={onClose}
        >
            <div
                className="bg-white dark:bg-gray-900 rounded-xl shadow-2xl w-full max-w-md p-6 relative"
                onClick={(e) => e.stopPropagation()}
            >
                <div className="flex items-center justify-between mb-5">
                    <h2 className="text-base font-semibold text-gray-900 dark:text-gray-100">
                        Keyboard Shortcuts
                    </h2>
                    <button
                        ref={closeButtonRef}
                        onClick={onClose}
                        aria-label="Close shortcuts modal"
                        className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 text-xl leading-none"
                    >
                        ×
                    </button>
                </div>

                <table className="w-full text-sm" aria-label="Keyboard shortcuts table">
                    <thead>
                        <tr className="border-b border-gray-100 dark:border-gray-800">
                            <th className="text-left pb-2 text-xs font-semibold text-gray-400 uppercase tracking-wider w-24">Key</th>
                            <th className="text-left pb-2 text-xs font-semibold text-gray-400 uppercase tracking-wider">Action</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-50 dark:divide-gray-800">
                        {shortcuts.map((s) => (
                            <tr key={s.key}>
                                <td className="py-2.5 pr-4">
                                    <kbd className="inline-flex items-center justify-center px-2 py-0.5 rounded border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300 font-mono text-xs font-semibold">
                                        {s.key === " " ? "Space" : s.key}
                                    </kbd>
                                </td>
                                <td className="py-2.5 text-gray-700 dark:text-gray-300">{s.label}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
