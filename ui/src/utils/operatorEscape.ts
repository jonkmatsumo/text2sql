const INPUT_TAGS = new Set(["INPUT", "TEXTAREA", "SELECT"]);

function isInputLikeElement(value: Element | null): value is HTMLElement {
    if (!(value instanceof HTMLElement)) return false;
    if (INPUT_TAGS.has(value.tagName)) return true;
    return value.isContentEditable;
}

interface EscapeShortcutOptions {
    isModalOpen: boolean;
    closeModal: () => void;
    clearFilters: () => void;
}

/**
 * Operator Escape precedence:
 * 1) close keyboard modal
 * 2) blur focused input/select/textarea/contenteditable
 * 3) clear filters only when nothing is focused
 */
export function handleOperatorEscapeShortcut(options: EscapeShortcutOptions): void {
    if (options.isModalOpen) {
        options.closeModal();
        return;
    }

    const activeElement = document.activeElement;
    if (isInputLikeElement(activeElement)) {
        activeElement.blur();
        return;
    }

    options.clearFilters();
}
