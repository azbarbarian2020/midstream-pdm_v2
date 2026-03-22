"use client";

import Link from "next/link";
import type { Asset } from "@/lib/types";
import { AlertOctagon, AlertTriangle, Wrench, Power } from "lucide-react";
import clsx from "clsx";

interface AlertListProps {
  assets: Asset[];
}

export function AlertList({ assets }: AlertListProps) {
  const alerts = assets
    .filter((a) => a.RISK_LEVEL === "FAILED" || a.RISK_LEVEL === "CRITICAL" || a.RISK_LEVEL === "WARNING" || a.RISK_LEVEL === "OFFLINE")
    .sort((a, b) => {
      const order: Record<string, number> = { FAILED: 0, OFFLINE: 1, CRITICAL: 2, WARNING: 3 };
      const orderA = order[a.RISK_LEVEL || ""] ?? 99;
      const orderB = order[b.RISK_LEVEL || ""] ?? 99;
      if (orderA !== orderB) return orderA - orderB;
      return (a.PREDICTED_RUL_DAYS ?? 999) - (b.PREDICTED_RUL_DAYS ?? 999);
    });

  return (
    <div className="flex flex-col gap-2 overflow-y-auto max-h-full">
      <div className="text-xs text-[var(--muted)] uppercase tracking-wider px-1">
        Alerts ({alerts.length})
      </div>
      {alerts.map((a) => (
        <Link
          key={a.ASSET_ID}
          href={`/asset/${a.ASSET_ID}`}
          className={clsx(
            "block rounded-lg border p-3 transition-colors hover:opacity-80",
            a.RISK_LEVEL === "FAILED"
              ? "border-red-900 bg-red-700/20"
              : a.RISK_LEVEL === "OFFLINE"
              ? "border-gray-500 bg-gray-500/20"
              : a.RISK_LEVEL === "CRITICAL"
              ? "border-[var(--red-border)] bg-[var(--red-surface)]"
              : "border-[var(--amber-border)] bg-[var(--amber-surface)]"
          )}
        >
          <div className="flex items-center gap-2">
            {a.RISK_LEVEL === "FAILED" ? (
              <AlertOctagon size={14} className="text-red-800" />
            ) : a.RISK_LEVEL === "OFFLINE" ? (
              <Power size={14} className="text-gray-500" />
            ) : a.RISK_LEVEL === "CRITICAL" ? (
              <AlertOctagon size={14} className="text-red-500" />
            ) : (
              <AlertTriangle size={14} className="text-amber-500" />
            )}
            <span className="font-medium text-sm text-[var(--foreground)]">
              Asset {a.ASSET_ID}
            </span>
            <span className="text-xs text-[var(--muted)]">{a.ASSET_TYPE}</span>
            {a.ASSIGNED_TECH_ID && (
              <span className="ml-auto flex items-center gap-1 text-[10px] text-indigo-600 font-medium">
                <Wrench size={10} />
                {a.ASSIGNED_TECH_NAME || a.ASSIGNED_TECH_ID}
              </span>
            )}
          </div>
          <div className="text-xs text-[var(--muted)] mt-1">
            {a.PREDICTED_CLASS} · RUL: {a.PREDICTED_RUL_DAYS?.toFixed(1) ?? "—"}d
          </div>
          <div className="text-xs text-[var(--muted)] mt-0.5">
            {a.STATION_NAME}
          </div>
        </Link>
      ))}
      {alerts.length === 0 && (
        <div className="text-sm text-[var(--muted)] px-1">No alerts</div>
      )}
    </div>
  );
}
