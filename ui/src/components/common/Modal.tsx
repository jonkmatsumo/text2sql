import React, { useEffect } from "react";

interface ModalProps {
    isOpen: boolean;
    onClose: () => void;
    title: string;
    children: React.ReactNode;
}

export default function Modal({ isOpen, onClose, title, children }: ModalProps) {
    useEffect(() => {
        const handleEsc = (e: KeyboardEvent) => {
            if (e.key === "Escape") {
                e.preventDefault();
                onClose();
            }
        };
        if (isOpen) window.addEventListener("keydown", handleEsc);
        return () => window.removeEventListener("keydown", handleEsc);
    }, [isOpen, onClose]);

    if (!isOpen) return null;

    return (
        <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="modal-title"
            style={{
                position: "fixed",
                top: 0,
                left: 0,
                right: 0,
                bottom: 0,
                backgroundColor: "rgba(0, 0, 0, 0.4)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                zIndex: 1000,
                padding: "20px",
                backdropFilter: "blur(4px)"
            }}
            onClick={onClose}
        >
            <div
                style={{
                    backgroundColor: "var(--surface)",
                    borderRadius: "20px",
                    width: "100%",
                    maxWidth: "800px",
                    maxHeight: "90vh",
                    overflow: "auto",
                    boxShadow: "var(--shadow)",
                    padding: "32px",
                    position: "relative"
                }}
                onClick={(e) => e.stopPropagation()}
            >
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "24px" }}>
                    <h2 id="modal-title" style={{ margin: 0, fontFamily: "'Space Grotesk', sans-serif" }}>{title}</h2>
                    <button
                        onClick={onClose}
                        style={{
                            background: "none",
                            border: "none",
                            fontSize: "1.5rem",
                            cursor: "pointer",
                            color: "var(--muted)"
                        }}
                    >
                        ×
                    </button>
                </div>
                <div>{children}</div>
            </div>
        </div>
    );
}
