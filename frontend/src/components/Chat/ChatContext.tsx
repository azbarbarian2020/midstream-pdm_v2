"use client";

import { createContext, useContext, useState, useCallback, type ReactNode } from "react";

interface ChatContextState {
  isOpen: boolean;
  open: (message?: string, context?: AssetContext) => void;
  close: () => void;
  toggle: () => void;
  pendingMessage: string | null;
  pendingContext: AssetContext | null;
  clearPending: () => void;
}

export interface AssetContext {
  asset_id: number;
  asset_type: string;
  predicted_class: string;
  rul_days: number;
}

const ChatContext = createContext<ChatContextState>({
  isOpen: false,
  open: () => {},
  close: () => {},
  toggle: () => {},
  pendingMessage: null,
  pendingContext: null,
  clearPending: () => {},
});

export function useChatContext() {
  return useContext(ChatContext);
}

export function ChatProvider({ children }: { children: ReactNode }) {
  const [isOpen, setIsOpen] = useState(false);
  const [pendingMessage, setPendingMessage] = useState<string | null>(null);
  const [pendingContext, setPendingContext] = useState<AssetContext | null>(null);

  const open = useCallback((message?: string, context?: AssetContext) => {
    if (message) setPendingMessage(message);
    if (context) setPendingContext(context);
    setIsOpen(true);
  }, []);

  const close = useCallback(() => setIsOpen(false), []);
  const toggle = useCallback(() => setIsOpen((v) => !v), []);

  const clearPending = useCallback(() => {
    setPendingMessage(null);
    setPendingContext(null);
  }, []);

  return (
    <ChatContext.Provider value={{ isOpen, open, close, toggle, pendingMessage, pendingContext, clearPending }}>
      {children}
    </ChatContext.Provider>
  );
}
