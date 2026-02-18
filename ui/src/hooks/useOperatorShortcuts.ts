import { useEffect } from "react";

export interface ShortcutDef {
    /** The key to listen for (e.g. "r", "/", "?") */
    key: string;
    /** Human-readable description shown in the help modal */
    label: string;
    /** Handler to invoke */
    handler: () => void;
    /** If true, fires even when an input is focused (default: false) */
    allowInInput?: boolean;
}

interface UseOperatorShortcutsOptions {
    shortcuts: ShortcutDef[];
    /**
     * When true, all shortcuts are suppressed.
     * Use this to disable shortcuts while a modal is open.
     */
    disabled?: boolean;
}

const INPUT_TAGS = new Set(["INPUT", "TEXTAREA", "SELECT"]);

function isInputFocused(): boolean {
    const el = document.activeElement;
    if (!el) return false;
    if (INPUT_TAGS.has(el.tagName)) return true;
    if ((el as HTMLElement).isContentEditable) return true;
    return false;
}

/**
 * Registers keyboard shortcuts for operator pages.
 *
 * Guards:
 * - Skips when an input/textarea/select/contenteditable is focused (unless allowInInput)
 * - Skips all shortcuts when `disabled` is true (e.g. modal is open)
 */
export function useOperatorShortcuts({ shortcuts, disabled = false }: UseOperatorShortcutsOptions): void {
    useEffect(() => {
        if (disabled) return;

        const handleKeyDown = (e: KeyboardEvent) => {
            const inInput = isInputFocused();

            for (const shortcut of shortcuts) {
                if (e.key !== shortcut.key) continue;
                if (inInput && !shortcut.allowInInput) continue;

                e.preventDefault();
                shortcut.handler();
                break;
            }
        };

        window.addEventListener("keydown", handleKeyDown);
        return () => window.removeEventListener("keydown", handleKeyDown);
    }, [shortcuts, disabled]);
}
