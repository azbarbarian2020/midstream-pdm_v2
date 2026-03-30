"use client";

import { useTimeTravel } from "@/components/TimeTravel/TimeTravelContext";
import clsx from "clsx";
import { Clock, RotateCcw, Loader2 } from "lucide-react";
import { useMemo } from "react";

function addDays(baseTs: string, days: number): string {
  if (!baseTs) return "";
  const d = new Date(baseTs);
  if (isNaN(d.getTime())) return "";
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10) + "T00:00:00";
}

export function TimeTravelBar() {
  const { activeOffset, setOffset, reset, toDisplayDate, dataNow, isLoading } = useTimeTravel();

  const OFFSETS = useMemo(() => {
    if (!dataNow) return [];
    return [
      { label: "Now", value: "Now", ts: dataNow },
      { label: "+24h", value: "+24h", ts: addDays(dataNow, 1) },
      { label: "+72h", value: "+72h", ts: addDays(dataNow, 3) },
      { label: "+7d", value: "+7d", ts: addDays(dataNow, 7) },
    ];
  }, [dataNow]);

  if (isLoading || !dataNow) {
    return (
      <div className="flex items-center gap-3 bg-[var(--surface-secondary)] border border-[var(--border)] rounded-lg px-4 py-2">
        <Loader2 size={16} className="text-[var(--muted)] animate-spin" />
        <span className="text-xs text-[var(--muted)]">Loading time data...</span>
      </div>
    );
  }

  return (
    <div className="flex items-center gap-3 bg-[var(--surface-secondary)] border border-[var(--border)] rounded-lg px-4 py-2">
      <Clock size={16} className="text-[var(--muted)]" />
      <span className="text-xs text-[var(--muted)] uppercase tracking-wider">Time</span>
      <div className="flex gap-1">
        {OFFSETS.map((o) => {
          const isActive = o.value === activeOffset || (o.label === "Now" && activeOffset === "Now");
          return (
            <button
              key={o.label}
              onClick={() => (o.value ? setOffset(o.value) : reset())}
              className={clsx(
                "px-3 py-1 text-sm rounded-md transition-colors flex flex-col items-center",
                isActive
                  ? "bg-blue-600 text-white"
                  : "bg-[var(--card)] text-[var(--muted)] hover:bg-[var(--hover)] border border-[var(--border)]"
              )}
            >
              <span>{o.label}</span>
              <span className={clsx("text-[10px] leading-tight", isActive ? "text-blue-200" : "text-[var(--muted)] opacity-70")}>
                {o.ts ? toDisplayDate(o.ts).slice(5, 10).replace("-", "/") : ""}
              </span>
            </button>
          );
        })}
      </div>
      {activeOffset && activeOffset !== "Now" && (
        <>
          <span className="px-2 py-0.5 bg-amber-100 text-amber-700 dark:bg-amber-900/50 dark:text-amber-400 text-xs rounded-full border border-amber-300 dark:border-amber-800">
            SIMULATION
          </span>
          <button
            onClick={reset}
            className="p-1 hover:bg-[var(--hover)] rounded"
            title="Reset to now"
          >
            <RotateCcw size={14} className="text-[var(--muted)]" />
          </button>
        </>
      )}
    </div>
  );
}
