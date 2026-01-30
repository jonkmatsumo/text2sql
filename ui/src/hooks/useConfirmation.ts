import { useState, useCallback, useRef } from "react";

interface ConfirmationOptions {
  title: string;
  description: React.ReactNode;
  confirmText?: string;
  cancelText?: string;
  danger?: boolean;
}

export function useConfirmation() {
  const [isOpen, setIsOpen] = useState(false);
  const [options, setOptions] = useState<ConfirmationOptions>({
    title: "",
    description: "",
  });
  const resolveRef = useRef<(value: boolean) => void>(() => {});

  const confirm = useCallback((opts: ConfirmationOptions) => {
    setOptions(opts);
    setIsOpen(true);
    return new Promise<boolean>((resolve) => {
      resolveRef.current = resolve;
    });
  }, []);

  const handleClose = useCallback(() => {
    setIsOpen(false);
    resolveRef.current(false);
  }, []);

  const handleConfirm = useCallback(() => {
    setIsOpen(false);
    resolveRef.current(true);
  }, []);

  return {
    confirm,
    dialogProps: {
      isOpen,
      onClose: handleClose,
      onConfirm: handleConfirm,
      ...options,
    },
  };
}
