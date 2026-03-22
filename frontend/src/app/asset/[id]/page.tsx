"use client";

import { useState, use, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { useTimeTravel } from "@/components/TimeTravel/TimeTravelContext";
import { Header } from "@/components/Header";

import Link from "next/link";
import {
  ArrowLeft,
  Activity,
  TrendingUp,
  Wrench,
  AlertOctagon,
  AlertTriangle,
  ShieldCheck,
  Power,
} from "lucide-react";
import clsx from "clsx";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  Cell,
  ReferenceArea,
  ReferenceLine,
} from "recharts";

const TABS = [
  { id: "overview", label: "Overview", icon: Activity },
  { id: "trends", label: "Trends", icon: TrendingUp },
  { id: "maintenance", label: "Maintenance", icon: Wrench },
] as const;

type TabId = (typeof TABS)[number]["id"];

const RISK_BADGE: Record<string, { icon: typeof AlertOctagon; color: string; bg: string }> = {
  FAILED: { icon: AlertOctagon, color: "text-white", bg: "bg-red-700 border-red-900" },
  OFFLINE: { icon: Power, color: "text-gray-500", bg: "bg-gray-200 border-gray-400" },
  CRITICAL: { icon: AlertOctagon, color: "text-red-500", bg: "bg-[var(--red-surface)] border-[var(--red-border)]" },
  WARNING: { icon: AlertTriangle, color: "text-amber-500", bg: "bg-[var(--amber-surface)] border-[var(--amber-border)]" },
  HEALTHY: { icon: ShieldCheck, color: "text-emerald-500", bg: "bg-[var(--emerald-surface)] border-[var(--emerald-border)]" },
};

const SHARED_SENSORS = [
  "VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW_RATE", "RPM", "POWER_DRAW",
];
const PUMP_ONLY_SENSORS = ["DIFFERENTIAL_PRESSURE", "SUCTION_PRESSURE", "SEAL_TEMPERATURE", "CAVITATION_INDEX"];
const COMPRESSOR_ONLY_SENSORS = ["DISCHARGE_TEMP", "INLET_TEMP", "COMPRESSION_RATIO", "OIL_PRESSURE"];

function getSensorsForType(assetType: string | undefined) {
  if (assetType === "PUMP") return [...SHARED_SENSORS, ...PUMP_ONLY_SENSORS];
  if (assetType === "COMPRESSOR") return [...SHARED_SENSORS, ...COMPRESSOR_ONLY_SENSORS];
  return [...SHARED_SENSORS, ...PUMP_ONLY_SENSORS, ...COMPRESSOR_ONLY_SENSORS];
}

const SENSOR_COLORS: Record<string, string> = {
  VIBRATION: "#ef4444",
  TEMPERATURE: "#f59e0b",
  PRESSURE: "#3b82f6",
  FLOW_RATE: "#10b981",
  RPM: "#8b5cf6",
  POWER_DRAW: "#ec4899",
  DIFFERENTIAL_PRESSURE: "#06b6d4",
  SUCTION_PRESSURE: "#14b8a6",
  SEAL_TEMPERATURE: "#f97316",
  CAVITATION_INDEX: "#6366f1",
  DISCHARGE_TEMP: "#e11d48",
  INLET_TEMP: "#0ea5e9",
  COMPRESSION_RATIO: "#84cc16",
  OIL_PRESSURE: "#a855f7",
};

const FAILURE_CONTRIBUTING_SENSORS: Record<string, Record<string, string[]>> = {
  BEARING_WEAR: {
    PUMP: ["VIBRATION", "TEMPERATURE", "RPM", "POWER_DRAW"],
    COMPRESSOR: ["VIBRATION", "TEMPERATURE", "RPM", "OIL_PRESSURE", "POWER_DRAW"],
  },
  VALVE_FAILURE: {
    PUMP: ["PRESSURE", "FLOW_RATE", "DIFFERENTIAL_PRESSURE", "TEMPERATURE"],
    COMPRESSOR: ["PRESSURE", "FLOW_RATE", "DISCHARGE_TEMP", "COMPRESSION_RATIO"],
  },
  SEAL_LEAK: {
    PUMP: ["PRESSURE", "SEAL_TEMPERATURE", "FLOW_RATE", "SUCTION_PRESSURE", "CAVITATION_INDEX"],
    COMPRESSOR: ["PRESSURE", "FLOW_RATE", "VIBRATION"],
  },
  OVERHEATING: {
    PUMP: ["TEMPERATURE", "POWER_DRAW", "SEAL_TEMPERATURE"],
    COMPRESSOR: ["TEMPERATURE", "DISCHARGE_TEMP", "POWER_DRAW", "OIL_PRESSURE"],
  },
  SURGE: {
    PUMP: ["FLOW_RATE", "PRESSURE", "VIBRATION"],
    COMPRESSOR: ["COMPRESSION_RATIO", "DISCHARGE_TEMP", "FLOW_RATE", "PRESSURE", "VIBRATION", "OIL_PRESSURE"],
  },
};

function getContributingSensors(failureMode: string, assetType: string | undefined): string[] {
  const modeMap = FAILURE_CONTRIBUTING_SENSORS[failureMode];
  if (!modeMap) return SHARED_SENSORS;
  return modeMap[assetType || "PUMP"] || modeMap.PUMP || SHARED_SENSORS;
}

const NOMINAL_BOUNDS: Record<string, { min: number; max: number }> = {
  VIBRATION: { min: 1.5, max: 5.5 },
  TEMPERATURE: { min: 155, max: 210 },
  PRESSURE: { min: 180, max: 820 },
  FLOW_RATE: { min: 280, max: 2050 },
  RPM: { min: 1450, max: 3650 },
  POWER_DRAW: { min: 40, max: 500 },
  DIFFERENTIAL_PRESSURE: { min: 28, max: 102 },
  SUCTION_PRESSURE: { min: 48, max: 205 },
  SEAL_TEMPERATURE: { min: 118, max: 182 },
  CAVITATION_INDEX: { min: 0.0, max: 0.16 },
  DISCHARGE_TEMP: { min: 195, max: 355 },
  INLET_TEMP: { min: 58, max: 125 },
  COMPRESSION_RATIO: { min: 1.5, max: 4.0 },
  OIL_PRESSURE: { min: 38, max: 82 },
};

export default function AssetDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const assetId = Number(id);
  const { asOfTimestamp, isSimulation, activeOffset, toDisplayDate } = useTimeTravel();
  const [tab, setTab] = useState<TabId>("overview");
  const [activeSensors, setActiveSensors] = useState<Set<string>>(new Set());
  const [sensorsInitialized, setSensorsInitialized] = useState(false);
  const [chartRange, setChartRange] = useState("60");

  const { data: asset } = useQuery({
    queryKey: ["asset", assetId, asOfTimestamp],
    queryFn: () => api.getAsset(assetId, asOfTimestamp || undefined),
    placeholderData: (prev) => prev,
  });

  const pred = asset?.prediction;
  const predictedClass = pred?.PREDICTED_CLASS || "NORMAL";

  if (!sensorsInitialized && asset) {
    let defaultSensors: string[];
    if (predictedClass !== "NORMAL") {
      defaultSensors = getContributingSensors(predictedClass, asset?.ASSET_TYPE);
    } else {
      const probs = pred?.CLASS_PROBABILITIES;
      let parsed = probs;
      if (typeof parsed === "string") try { parsed = JSON.parse(parsed); } catch { parsed = null; }
      if (parsed && typeof parsed === "object") {
        const nonNormal = Object.entries(parsed as Record<string, number>)
          .filter(([k]) => k !== "NORMAL")
          .sort((a, b) => b[1] - a[1]);
        if (nonNormal.length > 0 && nonNormal[0][1] > 0) {
          defaultSensors = getContributingSensors(nonNormal[0][0], asset?.ASSET_TYPE);
        } else {
          defaultSensors = SHARED_SENSORS;
        }
      } else {
        defaultSensors = SHARED_SENSORS;
      }
    }
    setActiveSensors(new Set(defaultSensors));
    setSensorsInitialized(true);
  }

  const telemetryStart = useMemo(() => {
    if (chartRange === "all") return undefined;
    const days = parseInt(chartRange);
    const ref = asOfTimestamp || "2026-03-13T00:00:00";
    const d = new Date(ref);
    d.setDate(d.getDate() - days);
    return d.toISOString().slice(0, 19);
  }, [chartRange, asOfTimestamp]);

  const sensorListStr = useMemo(() => {
    return activeSensors.size > 0 ? Array.from(activeSensors).join(",") : undefined;
  }, [activeSensors]);

  const { data: telemetry } = useQuery({
    queryKey: ["telemetry", assetId, asOfTimestamp, sensorListStr, telemetryStart],
    queryFn: () => api.getTelemetry(assetId, telemetryStart, asOfTimestamp || undefined, sensorListStr),
    enabled: tab === "trends" && sensorsInitialized,
    placeholderData: (prev) => prev,
  });

  const { data: predHistory } = useQuery({
    queryKey: ["pred-history", assetId, asOfTimestamp],
    queryFn: () => api.getPredictionHistory(assetId, asOfTimestamp || undefined),
    enabled: tab === "overview" || tab === "trends",
    placeholderData: (prev) => prev,
  });

  const { data: maintenance } = useQuery({
    queryKey: ["maintenance", assetId],
    queryFn: () => api.getMaintenance(assetId),
    enabled: tab === "maintenance",
    placeholderData: (prev) => prev,
  });

  const risk = pred?.RISK_LEVEL || "HEALTHY";
  const badge = RISK_BADGE[risk] || RISK_BADGE.HEALTHY;
  const BadgeIcon = badge.icon;

  const toggleSensor = (s: string) => {
    setActiveSensors((prev) => {
      const next = new Set(prev);
      if (next.has(s)) next.delete(s);
      else next.add(s);
      return next;
    });
  };

  const contributing = getContributingSensors(predictedClass, asset?.ASSET_TYPE);
  const availableSensors = getSensorsForType(asset?.ASSET_TYPE);

  const mlConfidenceTs = useMemo(() => {
    if (!predHistory || predHistory.length === 0) return null;
    for (const p of predHistory) {
      let probs = p.CLASS_PROBABILITIES;
      if (typeof probs === "string") try { probs = JSON.parse(probs); } catch { continue; }
      if (!probs || typeof probs !== "object") continue;
      const nonNormal = Object.entries(probs as Record<string, number>)
        .filter(([k]) => k !== "NORMAL")
        .reduce((max, [, v]) => Math.max(max, Number(v)), 0);
      if (nonNormal > 0.5) return p.AS_OF_TS;
    }
    return null;
  }, [predHistory]);

  const rulWarningTs = useMemo(() => {
    if (!predHistory || predHistory.length === 0) return null;
    for (const p of predHistory) {
      if (p.PREDICTED_RUL_DAYS != null && p.PREDICTED_RUL_DAYS <= 30) return p.AS_OF_TS;
    }
    return null;
  }, [predHistory]);

  const simulatedNowTs = useMemo(() => {
    if (!isSimulation || !asOfTimestamp || !telemetry || telemetry.length === 0) return null;
    const simDate = asOfTimestamp.slice(0, 10);
    const lastTs = telemetry[telemetry.length - 1]?.TS;
    if (!lastTs) return null;
    const lastDate = lastTs.slice(0, 10);
    if (simDate > lastDate) return lastTs;
    for (let i = telemetry.length - 1; i >= 0; i--) {
      if (telemetry[i].TS && telemetry[i].TS.slice(0, 10) <= simDate) return telemetry[i].TS;
    }
    return lastTs;
  }, [isSimulation, asOfTimestamp, telemetry]);

  const displayDate = (ts: string | undefined | null) => {
    if (!ts) return "—";
    return toDisplayDate(ts).slice(0, 10);
  };

  return (
    <div className="h-screen flex flex-col">
      <Header />

      <div className="px-4 py-3 flex items-center gap-4 border-b border-[var(--border)] bg-[var(--surface)]">
        <Link href="/" className="p-2 hover:bg-[var(--hover)] rounded-lg">
          <ArrowLeft size={18} className="text-[var(--muted)]" />
        </Link>
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-lg font-bold text-[var(--foreground)]">Asset {assetId}</h1>
            <span className="text-sm text-[var(--muted)]">{asset?.ASSET_TYPE}</span>
            <span className={clsx("flex items-center gap-1 px-2 py-0.5 text-xs rounded-full border", badge.bg)}>
              <BadgeIcon size={12} className={badge.color} />
              <span className={badge.color}>{risk}</span>
            </span>
          </div>
          <div className="text-xs text-[var(--muted)]">
            {asset?.STATION_NAME} · {asset?.MODEL_NAME} · {asset?.MANUFACTURER}
          </div>
        </div>
      </div>

      {risk === "FAILED" && (
        <div className="mx-4 mt-3 bg-red-700 text-white rounded-lg p-4 flex items-center gap-3 shadow-lg">
          <AlertOctagon size={24} className="shrink-0" />
          <div>
            <div className="font-bold text-sm">EQUIPMENT FAILURE</div>
            <div className="text-xs text-red-200">
              {predictedClass === "EQUIPMENT_FAILURE" ? (
                <>This asset has experienced a breakdown. Previous failure mode: <span className="font-semibold">{(predHistory || []).find((p: any) => p.PREDICTED_CLASS !== "NORMAL" && p.PREDICTED_CLASS !== "EQUIPMENT_FAILURE")?.PREDICTED_CLASS?.replace("_", " ") || "unknown"}</span>. Immediate dispatch required.</>
              ) : (
                <>Asset is in a failed state. Immediate maintenance dispatch required.</>
              )}
            </div>
          </div>
        </div>
      )}

      <div className="flex border-b border-[var(--border)]">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={clsx(
              "flex items-center gap-2 px-4 py-3 text-sm border-b-2 transition-colors",
              tab === t.id
                ? "border-indigo-500 text-indigo-600"
                : "border-transparent text-[var(--muted)] hover:text-[var(--foreground)]"
            )}
          >
            <t.icon size={14} />
            {t.label}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        {tab === "overview" && (
          <div className="grid grid-cols-3 gap-4">
            <div className="col-span-2 space-y-4">
              <div className="grid grid-cols-3 gap-3">
                <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                  <div className="text-xs text-[var(--muted)] mb-1">Predicted Failure</div>
                  <div className="text-lg font-bold text-[var(--foreground)]">{pred?.PREDICTED_CLASS || "NORMAL"}</div>
                </div>
                <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                  <div className="text-xs text-[var(--muted)] mb-1">RUL (days)</div>
                  <div className="text-lg font-bold text-[var(--foreground)]">{pred?.PREDICTED_RUL_DAYS?.toFixed(1) || "—"}</div>
                </div>
                <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                  <div className="text-xs text-[var(--muted)] mb-1">Model Version</div>
                  <div className="text-lg font-bold text-[var(--foreground)]">{pred?.MODEL_VERSION || "—"}</div>
                </div>
              </div>
              {pred?.CLASS_PROBABILITIES && (
                <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                  <div className="text-xs text-[var(--muted)] mb-3">Class Probabilities</div>
                  <div className="h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={Object.entries(
                          typeof pred.CLASS_PROBABILITIES === "string"
                            ? JSON.parse(pred.CLASS_PROBABILITIES)
                            : pred.CLASS_PROBABILITIES
                        ).map(([name, value]) => ({ name, value: Number(value) }))}
                        layout="vertical"
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="var(--chart-grid)" />
                        <XAxis type="number" domain={[0, 1]} stroke="var(--chart-axis)" tick={{ fontSize: 11 }} />
                        <YAxis type="category" dataKey="name" width={120} stroke="var(--chart-axis)" tick={{ fontSize: 11 }} />
                        <Tooltip
                          contentStyle={{ background: "var(--tooltip-bg)", border: "1px solid var(--tooltip-border)", borderRadius: 8 }}
                          formatter={(v: number) => [v.toFixed(4), "Probability"]}
                        />
                        <Bar dataKey="value" radius={[0, 4, 4, 0]} isAnimationActive={false}>
                          {Object.entries(
                            typeof pred.CLASS_PROBABILITIES === "string"
                              ? JSON.parse(pred.CLASS_PROBABILITIES)
                              : pred.CLASS_PROBABILITIES
                          ).map(([name], i) => (
                            <Cell
                              key={i}
                              fill={name === pred.PREDICTED_CLASS ? "#ef4444" : "#94a3b8"}
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )}
              {pred?.PREDICTED_RUL_DAYS != null && (
                <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                  <div className="text-xs text-[var(--muted)] mb-3">RUL Projection</div>
                  <div className="h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={(predHistory || []).filter((p: any) => p.PREDICTED_RUL_DAYS != null)}>
                        <CartesianGrid strokeDasharray="3 3" stroke="var(--chart-grid)" />
                        <XAxis dataKey="AS_OF_TS" stroke="var(--chart-axis)" tick={{ fontSize: 10 }} tickFormatter={(v) => displayDate(v)} />
                        <YAxis stroke="var(--chart-axis)" tick={{ fontSize: 11 }} label={{ value: "RUL (days)", angle: -90, position: "insideLeft", fill: "var(--chart-axis)", fontSize: 11 }} />
                        <Tooltip
                          contentStyle={{ background: "var(--tooltip-bg)", border: "1px solid var(--tooltip-border)", borderRadius: 8 }}
                          labelFormatter={(v) => toDisplayDate(String(v)).slice(0, 16)}
                        />
                        {isSimulation && pred?.AS_OF_TS && (
                          <ReferenceLine x={pred.AS_OF_TS} stroke="#3b82f6" strokeWidth={2} strokeDasharray="6 3" label={{ value: activeOffset || "Sim", position: "top", fill: "#3b82f6", fontSize: 10, fontWeight: 600 }} />
                        )}
                        {rulWarningTs && (
                          <ReferenceLine x={rulWarningTs} stroke="#f59e0b" strokeWidth={1.5} strokeDasharray="4 4" label={{ value: "RUL ≤30d", position: "insideTopRight", fill: "#f59e0b", fontSize: 10 }} />
                        )}
                        <ReferenceArea y1={0} y2={7} fill="#ef4444" fillOpacity={0.06} />
                        <ReferenceArea y1={7} y2={30} fill="#f59e0b" fillOpacity={0.04} />
                        <Line type="monotone" dataKey="PREDICTED_RUL_DAYS" stroke="#f59e0b" strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )}
            </div>
            <div className="space-y-4">
              <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                <div className="text-xs text-[var(--muted)] mb-2">Asset Info</div>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between"><span className="text-[var(--muted)]">Type</span><span className="text-[var(--foreground)]">{asset?.ASSET_TYPE}</span></div>
                  <div className="flex justify-between"><span className="text-[var(--muted)]">Model</span><span className="text-[var(--foreground)]">{asset?.MODEL_NAME}</span></div>
                  <div className="flex justify-between"><span className="text-[var(--muted)]">Manufacturer</span><span className="text-[var(--foreground)]">{asset?.MANUFACTURER}</span></div>
                  <div className="flex justify-between"><span className="text-[var(--muted)]">Installed</span><span className="text-[var(--foreground)]">{asset?.INSTALL_DATE}</span></div>
                  <div className="flex justify-between"><span className="text-[var(--muted)]">Station</span><span className="text-[var(--foreground)]">{asset?.STATION_NAME}</span></div>
                  <div className="flex justify-between"><span className="text-[var(--muted)]">Region</span><span className="text-[var(--foreground)]">{asset?.REGION}</span></div>
                </div>
              </div>
              <Link
                href={`/dispatch?asset=${assetId}`}
                className="block bg-indigo-600 hover:bg-indigo-500 text-white text-center rounded-lg py-3 text-sm font-medium"
              >
                Dispatch Service
              </Link>
            </div>
          </div>
        )}

        {tab === "trends" && (
          <div className="space-y-4">
            <div className="flex flex-wrap gap-3">
              {predictedClass !== "NORMAL" && (
                <div className="text-xs text-[var(--muted)] bg-[var(--surface-secondary)] border border-[var(--border)] rounded-lg px-3 py-2 flex-1">
                  <span className="font-semibold text-[var(--foreground)]">{predictedClass.replace("_", " ")}</span> predicted · RUL <span className="font-semibold text-[var(--foreground)]">{pred?.PREDICTED_RUL_DAYS?.toFixed(1) || "—"}d</span> · <span className={clsx("font-semibold", risk === "CRITICAL" ? "text-red-500" : risk === "WARNING" ? "text-amber-500" : "text-emerald-500")}>{risk}</span>
                  {isSimulation && activeOffset && <span className="ml-2 text-blue-500">(at {activeOffset})</span>}
                </div>
              )}
              {isSimulation && (
                <div className="text-xs bg-blue-50 text-blue-700 border border-blue-200 rounded-lg px-3 py-2">
                  Simulated time: <span className="font-semibold">{displayDate(asOfTimestamp)}</span>
                  {!simulatedNowTs && <span className="ml-1 text-blue-500">(beyond telemetry range)</span>}
                </div>
              )}
            </div>
            <div className="flex items-center gap-2 text-xs">
              <span className="text-[var(--muted)]">Range:</span>
              {[{ label: "30d", v: "30" }, { label: "60d", v: "60" }, { label: "90d", v: "90" }, { label: "All", v: "all" }].map((r) => (
                <button
                  key={r.v}
                  onClick={() => setChartRange(r.v)}
                  className={clsx(
                    "px-2 py-0.5 rounded-md border transition-colors",
                    chartRange === r.v
                      ? "bg-indigo-600 text-white border-transparent"
                      : "bg-[var(--card)] text-[var(--muted)] border-[var(--border)] hover:bg-[var(--hover)]"
                  )}
                >
                  {r.label}
                </button>
              ))}
            </div>
            <div className="flex gap-1.5 flex-wrap">
              {availableSensors.map((s) => {
                const isActive = activeSensors.has(s);
                const isContributing = contributing.includes(s);
                return (
                  <button
                    key={s}
                    onClick={() => toggleSensor(s)}
                    className={clsx(
                      "px-2.5 py-1 text-[11px] rounded-md border transition-colors",
                      isActive
                        ? "text-white border-transparent"
                        : "bg-[var(--surface-secondary)] text-[var(--muted)] border-[var(--border)] hover:bg-[var(--hover)]"
                    )}
                    style={isActive ? { background: SENSOR_COLORS[s] } : undefined}
                  >
                    {s.replace(/_/g, " ")}
                    {isContributing && predictedClass !== "NORMAL" && (
                      <span className="ml-1 text-[9px] opacity-70">*</span>
                    )}
                  </button>
                );
              })}
            </div>
            {Array.from(activeSensors).map((sensor) => {
              const bounds = NOMINAL_BOUNDS[sensor];
              const sensorData = (telemetry || []).filter((r: any) => r[sensor] != null);
              const findTs = (target: string | null) => {
                if (!target || sensorData.length === 0) return null;
                const d = target.slice(0, 10);
                for (const row of sensorData) {
                  if (row.TS && row.TS.slice(0, 10) >= d) return row.TS;
                }
                return null;
              };
              const sensorMlTs = contributing.includes(sensor) ? findTs(mlConfidenceTs) : null;
              const sensorRulTs = contributing.includes(sensor) ? findTs(rulWarningTs) : null;
              const sensorSimTs = isSimulation && asOfTimestamp && sensorData.length > 0 ? (() => {
                const simDate = asOfTimestamp.slice(0, 10);
                const lastTs = sensorData[sensorData.length - 1]?.TS;
                if (!lastTs) return null;
                if (simDate > lastTs.slice(0, 10)) return lastTs;
                for (let i = sensorData.length - 1; i >= 0; i--) {
                  if (sensorData[i].TS && sensorData[i].TS.slice(0, 10) <= simDate) return sensorData[i].TS;
                }
                return lastTs;
              })() : null;
              const values = sensorData.map((r: any) => Number(r[sensor]));
              let yMin = values.length > 0 ? Math.min(...values) : 0;
              let yMax = values.length > 0 ? Math.max(...values) : 100;
              if (bounds) {
                yMin = Math.min(yMin, bounds.min);
                yMax = Math.max(yMax, bounds.max);
              }
              const padding = (yMax - yMin) * 0.08 || 1;
              const domain: [number, number] = [
                Math.floor((yMin - padding) * 100) / 100,
                Math.ceil((yMax + padding) * 100) / 100,
              ];
              return (
                <div key={sensor} className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                  <div className="flex items-center gap-2 mb-2">
                    <div className="w-3 h-3 rounded-full" style={{ background: SENSOR_COLORS[sensor] }} />
                    <span className="text-xs font-medium text-[var(--foreground)]">{sensor.replace(/_/g, " ")}</span>
                    {bounds && (
                      <span className="text-[10px] text-[var(--muted)]">
                        Nominal: {bounds.min} – {bounds.max}
                      </span>
                    )}
                    {mlConfidenceTs && contributing.includes(sensor) && (
                      <span className="text-[10px] text-purple-500 ml-2">
                        ML Confidence &gt;50%: {displayDate(mlConfidenceTs)}
                      </span>
                    )}
                    {rulWarningTs && contributing.includes(sensor) && (
                      <span className="text-[10px] text-amber-500 ml-1">
                        · RUL ≤30d: {displayDate(rulWarningTs)}
                      </span>
                    )}
                  </div>
                  <div className="h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={sensorData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="var(--chart-grid)" />
                        <XAxis
                          dataKey="TS"
                          stroke="var(--chart-axis)"
                          tick={{ fontSize: 10 }}
                          interval={Math.max(0, Math.floor(sensorData.length / 8))}
                          tickFormatter={(v) => {
                            if (!v) return "";
                            return displayDate(String(v));
                          }}
                        />
                        <YAxis stroke="var(--chart-axis)" tick={{ fontSize: 11 }} domain={domain} />
                        <Tooltip
                          contentStyle={{ background: "var(--tooltip-bg)", border: "1px solid var(--tooltip-border)", borderRadius: 8 }}
                          labelFormatter={(v) => toDisplayDate(String(v)).slice(0, 16)}
                        />
                        {bounds && (
                          <ReferenceArea
                            y1={bounds.min}
                            y2={bounds.max}
                            fill="#22c55e"
                            fillOpacity={0.08}
                            stroke="#22c55e"
                            strokeOpacity={0.2}
                            strokeDasharray="3 3"
                          />
                        )}
                        {sensorMlTs && (
                          <ReferenceLine
                            x={sensorMlTs}
                            stroke="#8b5cf6"
                            strokeWidth={2}
                            strokeDasharray="6 6"
                            label={{ value: "ML Confidence >50%", position: "top", fill: "#8b5cf6", fontSize: 10 }}
                          />
                        )}
                        {sensorRulTs && (
                          <ReferenceLine
                            x={sensorRulTs}
                            stroke="#f59e0b"
                            strokeWidth={2}
                            strokeDasharray="6 6"
                            strokeDashoffset={6}
                            label={{ value: "RUL ≤30 days", position: "insideTopRight", fill: "#f59e0b", fontSize: 10 }}
                          />
                        )}
                        {sensorSimTs && (
                          <ReferenceLine
                            x={sensorSimTs}
                            stroke="#3b82f6"
                            strokeWidth={2}
                            strokeDasharray="6 3"
                            label={{ value: `${activeOffset || "Sim"}`, position: "insideTopRight", fill: "#3b82f6", fontSize: 10, fontWeight: 600 }}
                          />
                        )}
                        <Line
                          type="monotone"
                          dataKey={sensor}
                          stroke={SENSOR_COLORS[sensor]}
                          strokeWidth={1.5}
                          dot={false}
                          isAnimationActive={false}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              );
            })}
            {activeSensors.size === 0 && (
              <div className="text-sm text-[var(--muted)] text-center py-8">Select sensors above to view trends.</div>
            )}
            {pred?.PREDICTED_RUL_DAYS != null && (predHistory || []).length > 0 && (
              <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                <div className="flex items-center gap-2 mb-2">
                  <div className="w-3 h-3 rounded-full bg-amber-500" />
                  <span className="text-xs font-medium text-[var(--foreground)]">RUL Projection</span>
                  <span className="text-[10px] text-[var(--muted)]">{(predHistory || []).filter((p: any) => p.PREDICTED_RUL_DAYS != null).length} prediction points</span>
                </div>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={(predHistory || []).filter((p: any) => p.PREDICTED_RUL_DAYS != null)}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--chart-grid)" />
                      <XAxis dataKey="AS_OF_TS" stroke="var(--chart-axis)" tick={{ fontSize: 10 }} tickFormatter={(v) => displayDate(v)} />
                      <YAxis stroke="var(--chart-axis)" tick={{ fontSize: 11 }} label={{ value: "RUL (days)", angle: -90, position: "insideLeft", fill: "var(--chart-axis)", fontSize: 11 }} />
                      <Tooltip
                        contentStyle={{ background: "var(--tooltip-bg)", border: "1px solid var(--tooltip-border)", borderRadius: 8 }}
                        labelFormatter={(v) => toDisplayDate(String(v)).slice(0, 16)}
                      />
                      {isSimulation && pred?.AS_OF_TS && (
                        <ReferenceLine x={pred.AS_OF_TS} stroke="#3b82f6" strokeWidth={2} strokeDasharray="6 3" label={{ value: activeOffset || "Sim", position: "top", fill: "#3b82f6", fontSize: 10, fontWeight: 600 }} />
                      )}
                      {rulWarningTs && (
                        <ReferenceLine x={rulWarningTs} stroke="#f59e0b" strokeWidth={1.5} strokeDasharray="4 4" label={{ value: "RUL ≤30d", position: "insideTopRight", fill: "#f59e0b", fontSize: 10 }} />
                      )}
                      <ReferenceArea y1={0} y2={7} fill="#ef4444" fillOpacity={0.06} />
                      <ReferenceArea y1={7} y2={30} fill="#f59e0b" fillOpacity={0.04} />
                      <Line type="monotone" dataKey="PREDICTED_RUL_DAYS" stroke="#f59e0b" strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}
          </div>
        )}

        {tab === "maintenance" && (
          <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-[var(--table-header)]">
                <tr>
                  <th className="px-4 py-3 text-left text-xs text-[var(--muted)]">Date</th>
                  <th className="px-4 py-3 text-left text-xs text-[var(--muted)]">Type</th>
                  <th className="px-4 py-3 text-left text-xs text-[var(--muted)]">Description</th>
                  <th className="px-4 py-3 text-left text-xs text-[var(--muted)]">Duration</th>
                  <th className="px-4 py-3 text-left text-xs text-[var(--muted)]">Cost</th>
                </tr>
              </thead>
              <tbody>
                {(maintenance || []).map((m: any) => (
                  <tr key={m.LOG_ID} className="border-t border-[var(--table-border)]">
                    <td className="px-4 py-3 text-[var(--foreground)]">{m.TS?.slice(0, 10)}</td>
                    <td className="px-4 py-3">
                      <span className={clsx(
                        "px-2 py-0.5 text-xs rounded",
                        m.MAINTENANCE_TYPE === "EMERGENCY" ? "bg-[var(--red-surface)] text-red-500" :
                        m.MAINTENANCE_TYPE === "CORRECTIVE" ? "bg-[var(--amber-surface)] text-amber-500" :
                        "bg-[var(--badge-bg)] text-[var(--muted)]"
                      )}>
                        {m.MAINTENANCE_TYPE}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-[var(--foreground)] max-w-md truncate">{m.DESCRIPTION}</td>
                    <td className="px-4 py-3 text-[var(--muted)]">{m.DURATION_HRS}h</td>
                    <td className="px-4 py-3 text-[var(--muted)]">${m.COST?.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

    </div>
  );
}
