"use client";

import clsx from "clsx";
import type { KPIs } from "@/lib/types";
import {
  Activity,
  AlertTriangle,
  AlertOctagon,
  ShieldCheck,
  Clock,
} from "lucide-react";

interface KPICardsProps {
  kpis: KPIs | undefined;
  isLoading: boolean;
}

const cards = [
  { key: "total_assets", label: "Total Assets", icon: Activity, color: "text-blue-500", bg: "bg-[var(--blue-surface)]" },
  { key: "critical", label: "Critical", icon: AlertOctagon, color: "text-red-500", bg: "bg-[var(--red-surface)]" },
  { key: "warning", label: "Warning", icon: AlertTriangle, color: "text-amber-500", bg: "bg-[var(--amber-surface)]" },
  { key: "healthy", label: "Healthy", icon: ShieldCheck, color: "text-emerald-500", bg: "bg-[var(--emerald-surface)]" },
  { key: "avg_rul", label: "Avg RUL At-Risk", icon: Clock, color: "text-purple-500", bg: "bg-[var(--purple-surface)]" },
] as const;

export function KPICards({ kpis, isLoading }: KPICardsProps) {
  return (
    <div className="grid grid-cols-5 gap-3">
      {cards.map((c) => (
        <div
          key={c.key}
          className={clsx(
            "rounded-lg border border-[var(--card-border)] p-4",
            c.bg
          )}
        >
          <div className="flex items-center gap-2 mb-2">
            <c.icon size={16} className={c.color} />
            <span className="text-xs text-[var(--muted)] uppercase tracking-wider">
              {c.label}
            </span>
          </div>
          <div className="text-2xl font-bold text-[var(--foreground)]">
            {isLoading ? "—" : c.key === "critical" ? ((kpis?.failed || 0) + (kpis?.critical || 0)) || "—" : kpis?.[c.key] ?? "—"}
          </div>
        </div>
      ))}
    </div>
  );
}
