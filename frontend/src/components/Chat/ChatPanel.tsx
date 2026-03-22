"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import { api } from "@/lib/api";
import { MessageSquare, Send, X, Loader2, GripHorizontal } from "lucide-react";
import { useChatContext } from "./ChatContext";
import { useTimeTravel } from "@/components/TimeTravel/TimeTravelContext";
import clsx from "clsx";

interface Message {
  role: "user" | "assistant";
  content: string;
  toolsUsed?: string[];
}

export function ChatPanel() {
  const { isOpen, open, close, pendingMessage, pendingContext, clearPending } = useChatContext();
  const { asOfTimestamp, activeOffset } = useTimeTravel();
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [threadId, setThreadId] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  const [position, setPosition] = useState({ x: 0, y: 0 });
  const [hasCustomPosition, setHasCustomPosition] = useState(false);
  const dragRef = useRef<{ startX: number; startY: number; origX: number; origY: number } | null>(null);
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (pendingMessage && isOpen) {
      setInput(pendingMessage);
      clearPending();
    } else if (pendingContext && isOpen) {
      setInput(
        `What's wrong with asset ${pendingContext.asset_id}? It's predicted as ${pendingContext.predicted_class} with ${pendingContext.rul_days} days RUL. What should I do?`
      );
      clearPending();
    }
  }, [pendingMessage, pendingContext, isOpen, clearPending]);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" });
  }, [messages]);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const rect = panelRef.current?.getBoundingClientRect();
    if (!rect) return;

    const currentX = hasCustomPosition ? position.x : window.innerWidth - rect.width - 16;
    const currentY = hasCustomPosition ? position.y : window.innerHeight - rect.height - 16;

    dragRef.current = {
      startX: e.clientX,
      startY: e.clientY,
      origX: currentX,
      origY: currentY,
    };

    const handleMouseMove = (ev: MouseEvent) => {
      if (!dragRef.current) return;
      const dx = ev.clientX - dragRef.current.startX;
      const dy = ev.clientY - dragRef.current.startY;
      const newX = Math.max(0, Math.min(window.innerWidth - 384, dragRef.current.origX + dx));
      const newY = Math.max(0, Math.min(window.innerHeight - 100, dragRef.current.origY + dy));
      setPosition({ x: newX, y: newY });
      setHasCustomPosition(true);
    };

    const handleMouseUp = () => {
      dragRef.current = null;
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
    };

    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
  }, [hasCustomPosition, position]);

  const sendMessage = useCallback(async () => {
    if (!input.trim() || isLoading) return;
    const text = input.trim();
    setInput("");
    setMessages((prev) => [...prev, { role: "user", content: text }]);
    setIsLoading(true);

    try {
      let tid = threadId;
      if (!tid) {
        const resp = await api.createThread();
        tid = resp.thread_id;
        setThreadId(tid);
      }

      const resp = await api.sendMessage(tid!, text, undefined, pendingContext || undefined, asOfTimestamp || undefined, activeOffset || undefined);

      if (!resp.ok) {
        let errMsg = `Error: ${resp.status}`;
        try {
          const errBody = await resp.json();
          errMsg = errBody.detail || errBody.error || errMsg;
        } catch {}
        setMessages((prev) => [...prev, { role: "assistant", content: errMsg }]);
        return;
      }

      const reader = resp.body?.getReader();
      if (!reader) return;

      let assistantText = "";
      const toolsUsed: string[] = [];
      setMessages((prev) => [...prev, { role: "assistant", content: "", toolsUsed: [] }]);

      const decoder = new TextDecoder();
      let currentEvent = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value, { stream: true });
        const lines = chunk.split("\n").filter((l) => l.trim());

        for (const line of lines) {
          if (line.startsWith("event:")) {
            currentEvent = line.slice(6).trim();
          } else if (line.startsWith("data:")) {
            const raw = line.slice(5).trim();
            if (raw === "[DONE]") continue;
            try {
              const data = JSON.parse(raw);
              if (currentEvent === "response.text.delta" && data.text) {
                assistantText += data.text;
                setMessages((prev) => {
                  const updated = [...prev];
                  updated[updated.length - 1] = {
                    role: "assistant",
                    content: assistantText,
                    toolsUsed,
                  };
                  return updated;
                });
              }
              if (currentEvent === "response.tool_use" && data.name) {
                if (!toolsUsed.includes(data.name)) {
                  toolsUsed.push(data.name);
                }
              }
              if (currentEvent === "response.tool_result.status" && data.message) {
                const toolName = data.message.replace("Running ", "");
                if (toolName && !toolsUsed.includes(toolName)) {
                  toolsUsed.push(toolName);
                }
              }
            } catch {
            }
          }
        }
      }
    } catch {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: "Error connecting to agent. Please try again." },
      ]);
    } finally {
      setIsLoading(false);
    }
  }, [input, threadId, isLoading, pendingContext]);

  if (!isOpen) {
    return (
      <button
        onClick={() => open()}
        className="fixed bottom-4 right-4 bg-indigo-600 hover:bg-indigo-500 text-white rounded-full p-4 shadow-lg z-50"
      >
        <MessageSquare size={24} />
      </button>
    );
  }

  const panelStyle: React.CSSProperties = hasCustomPosition
    ? { position: "fixed", left: position.x, top: position.y, zIndex: 50 }
    : { position: "fixed", bottom: 16, right: 16, zIndex: 50 };

  return (
    <div
      ref={panelRef}
      style={panelStyle}
      className="w-96 h-[600px] bg-[var(--surface)] border border-[var(--border)] rounded-xl shadow-2xl flex flex-col"
    >
      <div
        onMouseDown={handleMouseDown}
        className="flex items-center justify-between px-4 py-3 border-b border-[var(--border)] cursor-grab active:cursor-grabbing select-none"
      >
        <div className="flex items-center gap-2">
          <GripHorizontal size={14} className="text-[var(--muted)]" />
          <MessageSquare size={16} className="text-indigo-500" />
          <span className="font-medium text-sm text-[var(--foreground)]">Cortex Assistant</span>
        </div>
        <div className="flex gap-1">
          <button onClick={() => { setMessages([]); setThreadId(null); }} className="p-1 hover:bg-[var(--hover)] rounded text-xs text-[var(--muted)]">
            Clear
          </button>
          <button onClick={close} className="p-1 hover:bg-[var(--hover)] rounded">
            <X size={14} className="text-[var(--muted)]" />
          </button>
        </div>
      </div>

      <div ref={scrollRef} className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.length === 0 && (
          <div className="flex flex-col items-center gap-4 mt-8">
            <div className="text-sm text-[var(--muted)] text-center">
              Ask about fleet health, maintenance procedures, or route planning.
            </div>
            <div className="flex flex-col gap-2 w-full px-2">
              {[
                "Which assets have the highest risk right now?",
                "Plan a service route for critical assets",
                "What maintenance is recommended for bearing wear?",
              ].map((q) => (
                <button
                  key={q}
                  onClick={() => { setInput(q); }}
                  className="text-left text-xs px-3 py-2 rounded-lg border border-[var(--border)] bg-[var(--surface-secondary)] text-[var(--muted)] hover:bg-[var(--hover)] hover:text-[var(--foreground)] transition-colors"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}
        {messages.map((m, i) => (
          <div key={i} className={clsx("flex", m.role === "user" ? "justify-end" : "justify-start")}>
            <div
              className={clsx(
                "max-w-[85%] rounded-lg px-3 py-2 text-sm",
                m.role === "user"
                  ? "bg-indigo-600 text-white"
                  : "bg-[var(--surface-secondary)] text-[var(--foreground)]"
              )}
            >
              {m.toolsUsed && m.toolsUsed.length > 0 && (
                <div className="flex gap-1 mb-1 flex-wrap">
                  {m.toolsUsed.map((t) => (
                    <span key={t} className="text-[10px] px-1.5 py-0.5 bg-[var(--badge-bg)] rounded text-[var(--muted)]">
                      {t}
                    </span>
                  ))}
                </div>
              )}
              <div className="whitespace-pre-wrap">{m.content || "..."}</div>
            </div>
          </div>
        ))}
        {isLoading && messages[messages.length - 1]?.role === "user" && (
          <div className="flex items-center gap-2 text-sm text-[var(--muted)]">
            <Loader2 size={14} className="animate-spin" />
            Thinking...
          </div>
        )}
      </div>

      <div className="p-3 border-t border-[var(--border)]">
        <div className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && sendMessage()}
            placeholder="Ask about assets, maintenance, routes..."
            className="flex-1 bg-[var(--input-bg)] border border-[var(--input-border)] rounded-lg px-3 py-2 text-sm text-[var(--input-text)] placeholder-[var(--muted)] outline-none focus:border-indigo-500"
          />
          <button
            onClick={sendMessage}
            disabled={isLoading || !input.trim()}
            className="bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white rounded-lg px-3 py-2"
          >
            <Send size={16} />
          </button>
        </div>
      </div>
    </div>
  );
}
