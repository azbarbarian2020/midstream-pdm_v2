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
  Customized,
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

const PUMP_SENSORS = [
  "SUCTION_PRESSURE", "DISCHARGE_PRESSURE", "FLOW_RATE", "MOTOR_CURRENT",
  "PUMP_SPEED", "BEARING_TEMP", "CASING_TEMP", "VIBRATION_RMS", "VALVE_POSITION", "LEAK_RATE"
];
const COMPRESSOR_SENSORS = [
  "VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW_RATE", "RPM", "POWER_DRAW",
  "DISCHARGE_TEMP", "INLET_TEMP", "COMPRESSION_RATIO", "OIL_PRESSURE"
];

function getSensorsForType(assetType: string | undefined) {
  if (assetType === "PUMP") return PUMP_SENSORS;
  if (assetType === "COMPRESSOR") return COMPRESSOR_SENSORS;
  return PUMP_SENSORS;
}

const SENSOR_COLORS: Record<string, string> = {
  SUCTION_PRESSURE: "#14b8a6",
  DISCHARGE_PRESSURE: "#3b82f6",
  FLOW_RATE: "#10b981",
  MOTOR_CURRENT: "#ec4899",
  PUMP_SPEED: "#8b5cf6",
  BEARING_TEMP: "#f59e0b",
  CASING_TEMP: "#f97316",
  VIBRATION_RMS: "#ef4444",
  VALVE_POSITION: "#06b6d4",
  LEAK_RATE: "#6366f1",
  VIBRATION: "#ef4444",
  TEMPERATURE: "#f59e0b",
  PRESSURE: "#3b82f6",
  RPM: "#8b5cf6",
  POWER_DRAW: "#ec4899",
  DISCHARGE_TEMP: "#e11d48",
  INLET_TEMP: "#0ea5e9",
  COMPRESSION_RATIO: "#84cc16",
  OIL_PRESSURE: "#a855f7",
};

const FAILURE_CONTRIBUTING_SENSORS: Record<string, Record<string, string[]>> = {
  BEARING_WEAR: {
    PUMP: ["VIBRATION_RMS", "BEARING_TEMP", "MOTOR_CURRENT", "CASING_TEMP"],
    COMPRESSOR: ["VIBRATION", "TEMPERATURE", "OIL_PRESSURE", "RPM", "POWER_DRAW"],
  },
  CAVITATION: {
    PUMP: ["SUCTION_PRESSURE", "VIBRATION_RMS", "FLOW_RATE", "DISCHARGE_PRESSURE"],
    COMPRESSOR: ["VIBRATION", "PRESSURE", "FLOW_RATE"],
  },
  VALVE_FAILURE: {
    PUMP: ["VALVE_POSITION", "FLOW_RATE", "DISCHARGE_PRESSURE", "MOTOR_CURRENT"],
    COMPRESSOR: ["PRESSURE", "FLOW_RATE", "DISCHARGE_TEMP", "COMPRESSION_RATIO"],
  },
  OVERHEATING: {
    PUMP: ["BEARING_TEMP", "CASING_TEMP", "MOTOR_CURRENT", "VIBRATION_RMS"],
    COMPRESSOR: ["TEMPERATURE", "DISCHARGE_TEMP", "POWER_DRAW", "OIL_PRESSURE"],
  },
  SEAL_LEAK: {
    PUMP: ["LEAK_RATE", "DISCHARGE_PRESSURE", "FLOW_RATE", "SUCTION_PRESSURE"],
    COMPRESSOR: ["VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW_RATE", "OIL_PRESSURE"],
  },
  OFFLINE: {
    PUMP: ["FLOW_RATE", "MOTOR_CURRENT", "PUMP_SPEED", "VIBRATION_RMS"],
    COMPRESSOR: ["VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW_RATE"],
  },
  FAILED: {
    PUMP: ["FLOW_RATE", "MOTOR_CURRENT", "PUMP_SPEED", "VIBRATION_RMS"],
    COMPRESSOR: ["VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW_RATE", "RPM", "POWER_DRAW"],
  },
};

function getContributingSensors(failureMode: string, assetType: string | undefined): string[] {
  const modeMap = FAILURE_CONTRIBUTING_SENSORS[failureMode];
  const defaultSensors = assetType === "PUMP" ? PUMP_SENSORS : COMPRESSOR_SENSORS;
  if (!modeMap) return defaultSensors.slice(0, 4);
  return modeMap[assetType || "PUMP"] || modeMap.PUMP || defaultSensors.slice(0, 4);
}

const NOMINAL_BOUNDS: Record<string, { min: number; max: number }> = {
  SUCTION_PRESSURE: { min: 20, max: 80 },
  DISCHARGE_PRESSURE: { min: 80, max: 250 },
  FLOW_RATE: { min: 100, max: 1000 },
  MOTOR_CURRENT: { min: 50, max: 300 },
  PUMP_SPEED: { min: 1500, max: 3600 },
  BEARING_TEMP: { min: 120, max: 180 },
  CASING_TEMP: { min: 100, max: 160 },
  VIBRATION_RMS: { min: 0.1, max: 0.5 },
  VALVE_POSITION: { min: 10, max: 90 },
  LEAK_RATE: { min: 0, max: 0.5 },
  VIBRATION: { min: 0.15, max: 0.55 },
  TEMPERATURE: { min: 130, max: 180 },
  PRESSURE: { min: 120, max: 200 },
  RPM: { min: 2000, max: 3200 },
  POWER_DRAW: { min: 80, max: 250 },
  DISCHARGE_TEMP: { min: 110, max: 160 },
  INLET_TEMP: { min: 80, max: 140 },
  COMPRESSION_RATIO: { min: 1.5, max: 4.0 },
  OIL_PRESSURE: { min: 10, max: 50 },
};

export default function AssetDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const assetId = Number(id);
  const { asOfTimestamp, isSimulation, activeOffset, toDisplayDate } = useTimeTravel();
  const [tab, setTab] = useState<TabId>("overview");
  const [activeSensors, setActiveSensors] = useState<Set<string>>(new Set());
  const [sensorsInitialized, setSensorsInitialized] = useState(false);
  const [chartRange, setChartRange] = useState("30");

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
      const topFeature = pred?.TOP_FEATURE_1;
      if (topFeature) {
        defaultSensors = [topFeature, ...(pred?.TOP_FEATURE_2 ? [pred.TOP_FEATURE_2] : []), ...(pred?.TOP_FEATURE_3 ? [pred.TOP_FEATURE_3] : [])];
      } else {
        defaultSensors = getSensorsForType(asset?.ASSET_TYPE).slice(0, 4);
      }
    }
    setActiveSensors(new Set(defaultSensors));
    setSensorsInitialized(true);
  }

  const chartCutoff = useMemo(() => {
    if (chartRange === "all") return undefined;
    const days = parseInt(chartRange);
    const ref = asOfTimestamp || "2026-03-25T23:55:00";
    const d = new Date(ref);
    d.setDate(d.getDate() - days);
    return d.toISOString().slice(0, 19);
  }, [chartRange, asOfTimestamp]);

  const telemetryStart = chartCutoff;

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

  const filteredPredHistory = useMemo(() => {
    if (!predHistory) return [];
    if (!chartCutoff) return predHistory;
    return predHistory.filter((p: any) => p.AS_OF_TS >= chartCutoff);
  }, [predHistory, chartCutoff]);

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
      if (p.CONFIDENCE != null && p.CONFIDENCE >= 0.5 && p.PREDICTED_CLASS && p.PREDICTED_CLASS !== "NORMAL") {
        return p.AS_OF_TS;
      }
    }
    return null;
  }, [predHistory]);

  const mlDetectedTs = useMemo(() => {
    if (!predHistory || predHistory.length === 0) return null;
    for (const p of predHistory) {
      if (p.PREDICTED_CLASS && p.PREDICTED_CLASS !== "NORMAL") return p.AS_OF_TS;
    }
    return null;
  }, [predHistory]);

  const rulWarningTs = useMemo(() => {
    if (!predHistory || predHistory.length === 0) return null;
    for (const p of predHistory) {
      if (p.PREDICTED_RUL_DAYS != null && p.PREDICTED_RUL_DAYS <= 14) return p.AS_OF_TS;
    }
    return null;
  }, [predHistory]);

  const rulCriticalTs = useMemo(() => {
    if (!predHistory || predHistory.length === 0) return null;
    for (const p of predHistory) {
      if (p.PREDICTED_RUL_DAYS != null && p.PREDICTED_RUL_DAYS <= 7) return p.AS_OF_TS;
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

      {(risk === "FAILED" || risk === "OFFLINE") && (
        <div className="mx-4 mt-3 bg-red-700 text-white rounded-lg p-4 flex items-center gap-3 shadow-lg">
          <AlertOctagon size={24} className="shrink-0" />
          <div>
            <div className="font-bold text-sm">EQUIPMENT {risk === "OFFLINE" ? "OFFLINE" : "FAILURE"}</div>
            <div className="text-xs text-red-200">
              This asset has experienced a failure ({predictedClass?.replace(/_/g, " ") || "unknown mode"}). RUL has reached zero. Immediate dispatch required.
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
              <div className="flex items-center justify-between">
                <div className="grid grid-cols-3 gap-3 flex-1">
                  <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                    <div className="text-xs text-[var(--muted)] mb-1">Predicted Failure</div>
                    <div className="text-lg font-bold text-[var(--foreground)]">{pred?.PREDICTED_CLASS || "NORMAL"}</div>
                  </div>
                  <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                    <div className="text-xs text-[var(--muted)] mb-1">RUL (days)</div>
                    <div className="text-lg font-bold text-[var(--foreground)]">{pred?.PREDICTED_RUL_DAYS?.toFixed(1) || "—"}</div>
                  </div>
                  <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                    <div className="text-xs text-[var(--muted)] mb-1">Confidence</div>
                    <div className="text-lg font-bold text-[var(--foreground)]">{pred?.CONFIDENCE ? `${(pred.CONFIDENCE * 100).toFixed(0)}%` : "—"}</div>
                  </div>
                </div>
              </div>
              <div className="flex items-center gap-2 text-xs">
                <span className="text-[var(--muted)]">Chart Range:</span>
                {[{ label: "30d", v: "30" }, { label: "60d", v: "60" }, { label: "All", v: "all" }].map((r) => (
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
              {pred?.TOP_FEATURE_1 && pred?.PREDICTED_CLASS && pred?.PREDICTED_CLASS !== 'NORMAL' && (
                <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                  <div className="text-xs text-[var(--muted)] mb-3">Sensor Contribution to Prediction</div>
                  <div className="py-2">
                    {(() => {
                      const humanize = (n: string) => n.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
                      const failureClass = pred.PREDICTED_CLASS;
                      const assetType = asset?.ASSET_TYPE || "PUMP";
                      const contributingSensors = FAILURE_CONTRIBUTING_SENSORS[failureClass]?.[assetType] || [];
                      const topFeature = pred.TOP_FEATURE_1;
                      return (
                        <div className="space-y-3">
                          <div className="flex items-center gap-2">
                            <span className="text-sm font-medium text-[var(--foreground)]">Primary Indicator:</span>
                            <span className="px-2 py-1 bg-red-500/20 text-red-400 rounded text-sm font-medium">
                              {humanize(topFeature)}
                            </span>
                          </div>
                          {contributingSensors.length > 1 && (
                            <div>
                              <span className="text-xs text-[var(--muted)]">Other contributing sensors for {humanize(failureClass)}:</span>
                              <div className="flex flex-wrap gap-1 mt-1">
                                {contributingSensors.filter(s => s !== topFeature).map((sensor, i) => (
                                  <span key={i} className="px-2 py-0.5 bg-amber-500/20 text-amber-400 rounded text-xs">
                                    {humanize(sensor)}
                                  </span>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      );
                    })()}
                  </div>
                </div>
              )}
              {pred?.PREDICTED_RUL_DAYS != null && (
                <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                  <div className="text-xs text-[var(--muted)] mb-3">RUL Projection</div>
                  <div className="h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={filteredPredHistory.filter((p: any) => p.PREDICTED_RUL_DAYS != null)}>
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
                        <ReferenceArea y1={0} y2={7} fill="#ef4444" fillOpacity={0.12} label={{ value: "CRITICAL ZONE", fill: "#ef4444", fontSize: 9, position: "insideRight" }} />
                        <ReferenceArea y1={7} y2={14} fill="#f59e0b" fillOpacity={0.08} label={{ value: "WARNING ZONE", fill: "#f59e0b", fontSize: 9, position: "insideRight" }} />
                        {mlConfidenceTs && (
                          <ReferenceLine x={mlConfidenceTs} stroke="#8b5cf6" strokeWidth={2.5} label={{ value: "Detected", position: "top", fill: "#8b5cf6", fontSize: 11, fontWeight: 700 }} />
                        )}
                        {rulWarningTs && (
                          <ReferenceLine x={rulWarningTs} stroke="#f59e0b" strokeWidth={2.5} label={{ value: "Warning", position: "insideTopRight", fill: "#f59e0b", fontSize: 11, fontWeight: 700 }} />
                        )}
                        {rulCriticalTs && (
                          <ReferenceLine x={rulCriticalTs} stroke="#ef4444" strokeWidth={2.5} label={{ value: "Critical", position: "insideBottomRight", fill: "#ef4444", fontSize: 11, fontWeight: 700 }} />
                        )}
                        <Line type="monotone" dataKey="PREDICTED_RUL_DAYS" stroke="#f59e0b" strokeWidth={2.5} dot={{ r: 3 }} isAnimationActive={false} />
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
              const findDataPoint = (target: string | null) => {
                if (!target || sensorData.length === 0) return null;
                const d = target.slice(0, 10);
                for (let i = 0; i < sensorData.length; i++) {
                  const row = sensorData[i];
                  if (row.TS && row.TS.slice(0, 10) >= d) {
                    return { ts: row.TS, index: i };
                  }
                }
                return null;
              };
              const mlPoint = findDataPoint(mlDetectedTs);
              const rulPoint = findDataPoint(rulWarningTs);
              const sensorMlTs = mlPoint?.ts || null;
              const sensorRulTs = rulPoint?.ts || null;
              const linesOverlap = mlPoint && rulPoint && mlPoint.index === rulPoint.index;
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
                  <div className="flex items-center flex-wrap gap-2 mb-2">
                    <div className="w-3 h-3 rounded-full" style={{ background: SENSOR_COLORS[sensor] }} />
                    <span className="text-xs font-medium text-[var(--foreground)]">{sensor.replace(/_/g, " ")}</span>
                    {bounds && (
                      <span className="text-[10px] text-[var(--muted)]">
                        Nominal: {bounds.min} – {bounds.max}
                      </span>
                    )}
                    {sensorMlTs && (
                      <span className="text-[10px] text-purple-500 ml-2">
                        Anomaly Detected: {displayDate(sensorMlTs)}
                      </span>
                    )}
                    {sensorRulTs && !linesOverlap && (
                      <span className="text-[10px] text-amber-500 ml-1">
                        · RUL ≤14d: {displayDate(sensorRulTs)}
                      </span>
                    )}
                    {linesOverlap && (
                      <span className="text-[10px] text-orange-500 ml-1">
                        (same as warning)
                      </span>
                    )}
                  </div>
                  <div className="h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={sensorData}>
                        <defs>
                          <linearGradient id={`gradientLine-${sensor}`} x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="#8b5cf6" />
                            <stop offset="50%" stopColor="#f97316" />
                            <stop offset="100%" stopColor="#f59e0b" />
                          </linearGradient>
                        </defs>
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
                        <Customized
                          component={({ xAxisMap, yAxisMap }: any) => {
                            if (!xAxisMap || !yAxisMap) return null;
                            const xAxis = Object.values(xAxisMap)[0] as any;
                            const yAxis = Object.values(yAxisMap)[0] as any;
                            if (!xAxis || !yAxis) return null;
                            const { x: xStart, width } = xAxis;
                            const { y: yStart, height } = yAxis;
                            const dataLen = sensorData.length;
                            if (dataLen === 0) return null;
                            const lines: React.ReactNode[] = [];
                            const calcX = (idx: number) => xStart + (idx / (dataLen - 1)) * width;
                            if (linesOverlap && mlPoint) {
                              const xPos = calcX(mlPoint.index);
                              lines.push(
                                <g key="overlap">
                                  <line x1={xPos} y1={yStart} x2={xPos} y2={yStart + height} stroke="#8b5cf6" strokeWidth={3} strokeDasharray="6 3" />
                                  <line x1={xPos + 3} y1={yStart} x2={xPos + 3} y2={yStart + height} stroke="#f59e0b" strokeWidth={3} />
                                  <text x={xPos} y={yStart - 4} fill="#f97316" fontSize={11} fontWeight={700} textAnchor="middle">Detected + Warning</text>
                                </g>
                              );
                            } else {
                              if (mlPoint) {
                                const xPos = calcX(mlPoint.index);
                                lines.push(
                                  <g key="ml">
                                    <line x1={xPos} y1={yStart} x2={xPos} y2={yStart + height} stroke="#8b5cf6" strokeWidth={2.5} />
                                    <text x={xPos} y={yStart - 4} fill="#8b5cf6" fontSize={11} fontWeight={700} textAnchor="middle">Detected</text>
                                  </g>
                                );
                              }
                              if (rulPoint) {
                                const xPos = calcX(rulPoint.index);
                                lines.push(
                                  <g key="rul">
                                    <line x1={xPos} y1={yStart} x2={xPos} y2={yStart + height} stroke="#f59e0b" strokeWidth={2.5} />
                                    <text x={xPos + 4} y={yStart - 4} fill="#f59e0b" fontSize={11} fontWeight={700} textAnchor="start">RUL ≤14d</text>
                                  </g>
                                );
                              }
                            }
                            return <g>{lines}</g>;
                          }}
                        />
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
            {pred?.PREDICTED_RUL_DAYS != null && filteredPredHistory.length > 0 && (
              <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-4">
                <div className="flex items-center gap-2 mb-2">
                  <div className="w-3 h-3 rounded-full bg-amber-500" />
                  <span className="text-xs font-medium text-[var(--foreground)]">RUL Projection</span>
                  <span className="text-[10px] text-[var(--muted)]">{filteredPredHistory.filter((p: any) => p.PREDICTED_RUL_DAYS != null).length} prediction points</span>
                </div>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={filteredPredHistory.filter((p: any) => p.PREDICTED_RUL_DAYS != null)}>
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
                      <ReferenceArea y1={0} y2={7} fill="#ef4444" fillOpacity={0.12} label={{ value: "CRITICAL ZONE", fill: "#ef4444", fontSize: 9, position: "insideRight" }} />
                      <ReferenceArea y1={7} y2={14} fill="#f59e0b" fillOpacity={0.08} label={{ value: "WARNING ZONE", fill: "#f59e0b", fontSize: 9, position: "insideRight" }} />
                      {mlConfidenceTs && (
                        <ReferenceLine x={mlConfidenceTs} stroke="#8b5cf6" strokeWidth={2.5} label={{ value: "Detected", position: "top", fill: "#8b5cf6", fontSize: 11, fontWeight: 700 }} />
                      )}
                      {rulWarningTs && (
                        <ReferenceLine x={rulWarningTs} stroke="#f59e0b" strokeWidth={2.5} label={{ value: "Warning", position: "insideTopRight", fill: "#f59e0b", fontSize: 11, fontWeight: 700 }} />
                      )}
                      {rulCriticalTs && (
                        <ReferenceLine x={rulCriticalTs} stroke="#ef4444" strokeWidth={2.5} label={{ value: "Critical", position: "insideBottomRight", fill: "#ef4444", fontSize: 11, fontWeight: 700 }} />
                      )}
                      <Line type="monotone" dataKey="PREDICTED_RUL_DAYS" stroke="#f59e0b" strokeWidth={2.5} dot={{ r: 3 }} isAnimationActive={false} />
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
