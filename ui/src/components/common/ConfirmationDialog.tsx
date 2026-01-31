import React from "react";
import Modal from "./Modal";

interface ConfirmationDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onConfirm: () => void;
  title: string;
  description: React.ReactNode;
  confirmText?: string;
  cancelText?: string;
  danger?: boolean;
}

export function ConfirmationDialog({
  isOpen,
  onClose,
  onConfirm,
  title,
  description,
  confirmText = "Confirm",
  cancelText = "Cancel",
  danger = false,
}: ConfirmationDialogProps) {
  return (
    <Modal isOpen={isOpen} onClose={onClose} title={title}>
      <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
        <div style={{ fontSize: "1rem", color: "var(--ink)" }}>{description}</div>
        <div style={{ display: "flex", justifyContent: "flex-end", gap: "12px" }}>
          <button
            onClick={onClose}
            style={{
              padding: "10px 16px",
              borderRadius: "8px",
              border: "1px solid var(--border)",
              background: "transparent",
              cursor: "pointer",
              fontWeight: 500,
            }}
          >
            {cancelText}
          </button>
          <button
            onClick={() => {
              onConfirm();
              // onClose is handled by the caller or by wrapper, but standard is to close
              // However, in our hook pattern, onConfirm resolves the promise, but usually expects closing.
              // Let's rely on the parent/hook to close it.
            }}
            style={{
              padding: "10px 16px",
              borderRadius: "8px",
              border: danger ? "1px solid var(--error-border)" : "none",
              background: danger ? "var(--error-bg)" : "var(--accent)",
              color: danger ? "var(--error-text)" : "#fff",
              cursor: "pointer",
              fontWeight: 500,
            }}
          >
            {confirmText}
          </button>
        </div>
      </div>
    </Modal>
  );
}
