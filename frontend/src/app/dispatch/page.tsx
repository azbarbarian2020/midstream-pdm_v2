"use client";

import { useState, useEffect, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { useTimeTravel } from "@/components/TimeTravel/TimeTravelContext";
import { useChatContext } from "@/components/Chat/ChatContext";
import { Header } from "@/components/Header";
import { FleetMap } from "@/components/Map/FleetMap";
import { TechScheduleGantt } from "@/components/Schedule/TechScheduleGantt";
import {
  Play,
  ClipboardList,
  AlertOctagon,
  AlertTriangle,
  CheckCircle,
  MessageSquare,
  Clock,
  MapPin,
  Wrench,
  Calendar,
  User,
  ChevronDown,
  ChevronRight,
  RotateCcw,
  Sparkles,
} from "lucide-react";
import clsx from "clsx";
import type { RouteResult, RouteStop, Technician } from "@/lib/types";

const DAY_COLORS = [
  { bg: "bg-blue-50 border-blue-200", text: "text-blue-700", badge: "bg-blue-100 text-blue-700", line: "#3b82f6" },
  { bg: "bg-orange-50 border-orange-200", text: "text-orange-700", badge: "bg-orange-100 text-orange-700", line: "#f97316" },
  { bg: "bg-green-50 border-green-200", text: "text-green-700", badge: "bg-green-100 text-green-700", line: "#22c55e" },
  { bg: "bg-purple-50 border-purple-200", text: "text-purple-700", badge: "bg-purple-100 text-purple-700", line: "#8b5cf6" },
  { bg: "bg-pink-50 border-pink-200", text: "text-pink-700", badge: "bg-pink-100 text-pink-700", line: "#ec4899" },
];

function DispatchContent() {
  const queryClient = useQueryClient();
  const { asOfTimestamp, toDisplayDate, dataNow } = useTimeTravel();
  const { open } = useChatContext();
  const searchParams = useSearchParams();
  const assetParam = searchParams.get("asset");
  const initialAsset = assetParam ? parseInt(assetParam, 10) : NaN;
  const [techId, setTechId] = useState("TECH-001");
  const [primaryAssetId, setPrimaryAssetId] = useState(isNaN(initialAsset) ? 0 : initialAsset);
  const [defaultResolved, setDefaultResolved] = useState(!isNaN(initialAsset));
  const [horizonDays, setHorizonDays] = useState(3);
  const [maxStops, setMaxStops] = useState(8);
  const [routeResult, setRouteResult] = useState<RouteResult | null>(null);
  const [bundled, setBundled] = useState(false);
  const [showTechProfile, setShowTechProfile] = useState(false);
  const [showSchedule, setShowSchedule] = useState(false);
  const [focusDay, setFocusDay] = useState<number | null>(null);

  const { data: assets } = useQuery({
    queryKey: ["assets-dispatch", asOfTimestamp],
    queryFn: () => api.getAssets(asOfTimestamp || undefined),
  });

  const { data: technicians } = useQuery({
    queryKey: ["technicians"],
    queryFn: () => api.getTechnicians(),
  });

  const { data: techSchedules } = useQuery({
    queryKey: ["tech-schedules", techId],
    queryFn: () => api.getTechSchedules(techId, dataNow.slice(0, 10), "2026-03-21"),
    enabled: !!techId,
  });

  const { data: allSchedules } = useQuery({
    queryKey: ["all-tech-schedules", bundled],
    queryFn: () => api.getTechSchedules(undefined, dataNow.slice(0, 10), "2026-03-21"),
  });

  const selectedTech = technicians?.find((t: Technician) => t.TECH_ID === techId);

  useEffect(() => {
    if (!defaultResolved && assets?.length) {
      const atRisk = assets
        .filter((a: any) => a.RISK_LEVEL === "CRITICAL" || a.RISK_LEVEL === "WARNING")
        .sort((a: any, b: any) => (a.PREDICTED_RUL_DAYS ?? 999) - (b.PREDICTED_RUL_DAYS ?? 999));
      if (atRisk.length > 0) {
        setPrimaryAssetId(atRisk[0].ASSET_ID);
      }
      setDefaultResolved(true);
    }
  }, [assets, defaultResolved]);

  const suggestMutation = useMutation({
    mutationFn: () =>
      api.suggestTech({
        primary_asset_id: primaryAssetId,
        horizon_days: horizonDays,
        as_of_ts: asOfTimestamp || undefined,
      }),
    onSuccess: (data) => {
      if (data.recommendations?.length > 0) {
        setTechId(data.recommendations[0].tech_id);
        setRouteResult(null);
      }
    },
  });

  const planMutation = useMutation({
    mutationFn: () =>
      api.planRoute({
        tech_id: techId,
        primary_asset_id: primaryAssetId,
        horizon_days: horizonDays,
        max_stops: maxStops,
        as_of_ts: asOfTimestamp || undefined,
      }),
    onSuccess: (data) => {
      setRouteResult(data);
      setBundled(false);
      setFocusDay(null);
    },
  });

  const bundleMutation = useMutation({
    mutationFn: () =>
      api.bundleWorkOrders({
        tech_id: techId,
        stops: routeResult?.route || [],
      }),
    onSuccess: () => {
      setBundled(true);
      queryClient.invalidateQueries({ queryKey: ["assets"] });
      queryClient.invalidateQueries({ queryKey: ["assets-dispatch"] });
      queryClient.invalidateQueries({ queryKey: ["all-tech-schedules"] });
      queryClient.invalidateQueries({ queryKey: ["tech-schedules"] });
    },
  });

  const resetMutation = useMutation({
    mutationFn: () => api.resetDemo(),
    onSuccess: () => {
      setBundled(false);
      setRouteResult(null);
      queryClient.invalidateQueries({ queryKey: ["assets"] });
      queryClient.invalidateQueries({ queryKey: ["assets-dispatch"] });
      queryClient.invalidateQueries({ queryKey: ["all-tech-schedules"] });
      queryClient.invalidateQueries({ queryKey: ["tech-schedules"] });
    },
  });

  const handleExplainRoute = () => {
    if (!routeResult) return;
    const stops = routeResult.route
      .map((s: RouteStop, i: number) => `${i + 1}. Asset ${s.asset_id} (${s.asset_type}) - ${s.risk_level} - ${s.reason} [Day ${s.scheduled_day}]`)
      .join("\n");
    open(
      `Explain the reasoning behind this maintenance route plan:\n${stops}\nTotal: ${routeResult.total_stops} stops, ${routeResult.estimated_travel_miles} miles, ${routeResult.total_days} day(s). Why this order? What co-replacements should I consider?`
    );
  };

  const routeStops = routeResult?.route.map((s) => ({
    lat: s.lat,
    lon: s.lon,
    asset_id: s.asset_id,
    stop_number: s.stop_number,
    scheduled_day: s.scheduled_day,
  }));

  const routeHome = routeResult
    ? { lat: routeResult.home_lat, lon: routeResult.home_lon }
    : undefined;

  const primaryAssetData = assets?.find((a: any) => a.ASSET_ID === primaryAssetId);
  const primaryAssetLocation = primaryAssetData
    ? { lat: Number(primaryAssetData.LAT), lon: Number(primaryAssetData.LON), name: `Asset ${primaryAssetId}`, risk: primaryAssetData.RISK_LEVEL || "HEALTHY" }
    : null;

  const groupedByDay: Record<number, RouteStop[]> = {};
  routeResult?.route.forEach((s) => {
    if (!groupedByDay[s.scheduled_day]) groupedByDay[s.scheduled_day] = [];
    groupedByDay[s.scheduled_day].push(s);
  });

  return (
    <div className="h-screen flex flex-col">
      <Header />

      <div className="flex-1 flex min-h-0">
        <div className="w-[420px] border-r border-[var(--border)] flex flex-col overflow-y-auto bg-[var(--surface)]">
          <div className="p-4 space-y-4">
            <div className="flex items-center justify-between">
              <div className="text-sm font-medium text-[var(--foreground)]">Route Planner</div>
              <button
                onClick={() => resetMutation.mutate()}
                disabled={resetMutation.isPending}
                className="flex items-center gap-1 text-[10px] text-[var(--muted)] hover:text-red-500 px-2 py-1 rounded hover:bg-red-50 transition-colors"
                title="Reset demo data (clear bundled WOs and schedule entries)"
              >
                <RotateCcw size={10} />
                {resetMutation.isPending ? "Resetting..." : "Reset Demo"}
              </button>
            </div>

            <div className="space-y-3">
              <div>
                <label className="block text-xs text-[var(--muted)] mb-1">Technician</label>
                <select
                  value={techId}
                  onChange={(e) => { setTechId(e.target.value); setRouteResult(null); setFocusDay(null); }}
                  className="w-full bg-[var(--input-bg)] border border-[var(--input-border)] rounded-lg px-3 py-2 text-sm text-[var(--input-text)]"
                >
                  {(technicians || []).map((t: Technician) => (
                    <option key={t.TECH_ID} value={t.TECH_ID}>
                      {t.NAME} — {t.HOME_BASE_CITY} ({t.AVAILABILITY})
                    </option>
                  ))}
                </select>
                <button
                  onClick={() => suggestMutation.mutate()}
                  disabled={suggestMutation.isPending}
                  className="w-full flex items-center justify-center gap-2 bg-amber-500/10 hover:bg-amber-500/20 text-amber-700 border border-amber-300 rounded-lg py-1.5 text-xs font-medium transition-colors mt-2"
                >
                  <Sparkles size={12} />
                  {suggestMutation.isPending ? "Analyzing..." : "Suggest Best Technician"}
                </button>
                {suggestMutation.data?.recommendations && (
                  <div className="bg-amber-50 border border-amber-200 rounded-lg p-2 space-y-1.5">
                    {suggestMutation.data.predicted_class && suggestMutation.data.predicted_class !== "NORMAL" && (
                      <div className="text-[10px] text-amber-700 px-2 py-1 bg-amber-100 rounded">
                        Evaluating for <span className="font-semibold">{suggestMutation.data.predicted_class.replace(/_/g, " ")}</span> on {suggestMutation.data.asset_type}
                      </div>
                    )}
                    {suggestMutation.data.explanation && (
                      <div className="text-[10px] text-amber-800 px-2 py-1.5 bg-amber-100/60 rounded border border-amber-200 italic">
                        {suggestMutation.data.explanation}
                      </div>
                    )}
                    {suggestMutation.data.recommendations.map((rec: any, i: number) => (
                      <button
                        key={rec.tech_id}
                        onClick={() => { setTechId(rec.tech_id); setRouteResult(null); setFocusDay(null); }}
                        className={clsx(
                          "w-full text-left px-2 py-1.5 rounded text-xs transition-colors",
                          rec.tech_id === techId ? "bg-amber-200/60 font-medium" : "hover:bg-amber-100"
                        )}
                      >
                        <div className="flex items-center justify-between">
                          <span className="flex items-center gap-1.5">
                            <span className="w-4 h-4 rounded-full bg-amber-500 text-white flex items-center justify-center text-[9px] font-bold">{i + 1}</span>
                            <span className="text-[var(--foreground)]">{rec.name}</span>
                          </span>
                          <span className="text-[10px] text-[var(--muted)]">
                            {rec.score} pts
                          </span>
                        </div>
                        <div className="flex gap-2 mt-0.5 ml-6 text-[9px] text-[var(--muted)]">
                          <span>{rec.distance_miles}mi</span>
                          <span>{rec.available_hours}h free</span>
                          {rec.specialty_match && <span className="text-amber-700">{rec.specialty_match}</span>}
                          {!rec.cert_match && <span className="text-red-500">no cert</span>}
                        </div>
                        {rec.explanation && (
                          <div className="ml-6 mt-0.5 text-[9px] text-amber-600 italic">{rec.explanation}</div>
                        )}
                      </button>
                    ))}
                  </div>
                )}
              </div>

              {selectedTech && (
                <div className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg p-3">
                  <button
                    onClick={() => setShowTechProfile(!showTechProfile)}
                    className="flex items-center justify-between w-full text-left"
                  >
                    <div className="flex items-center gap-2">
                      {selectedTech.PHOTO_URL ? (
                        <img src={selectedTech.PHOTO_URL} alt={selectedTech.NAME} className="w-20 h-20 rounded-full object-cover border-2 border-indigo-200" />
                      ) : (
                        <div className="w-20 h-20 rounded-full bg-indigo-100 flex items-center justify-center">
                          <User size={32} className="text-indigo-600" />
                        </div>
                      )}
                      <div>
                        <div className="text-sm font-medium text-[var(--foreground)]">{selectedTech.NAME}</div>
                        <div className="text-[10px] text-[var(--muted)]">
                          {selectedTech.YEARS_EXPERIENCE}yr exp · {selectedTech.HOME_BASE_CITY}
                        </div>
                      </div>
                    </div>
                    {showTechProfile ? <ChevronDown size={14} className="text-[var(--muted)]" /> : <ChevronRight size={14} className="text-[var(--muted)]" />}
                  </button>
                  {showTechProfile && (
                    <div className="mt-3 pt-3 border-t border-[var(--border)] space-y-2">
                      <div className="text-xs text-[var(--foreground)]">{selectedTech.BIO}</div>
                      <div className="flex flex-wrap gap-1">
                        {(Array.isArray(selectedTech.CERTIFICATIONS) ? selectedTech.CERTIFICATIONS : String(selectedTech.CERTIFICATIONS || "").split(",")).map((c: string, i: number) => (
                          <span key={i} className="text-[10px] px-1.5 py-0.5 bg-indigo-50 text-indigo-700 rounded">{c.trim()}</span>
                        ))}
                      </div>
                      <div className="text-[10px] text-[var(--muted)]">{selectedTech.SPECIALTY_NOTES}</div>
                      {techSchedules && techSchedules.length > 0 && (
                        <div className="mt-2">
                          <div className="text-[10px] text-[var(--muted)] font-medium mb-1">Upcoming Schedule</div>
                          {techSchedules.map((s: any, i: number) => (
                            <div key={i} className="flex items-center gap-2 text-[10px] py-0.5">
                              <span className="text-[var(--muted)] w-16">{s.SCHEDULE_DATE ? (() => { const [y,m,d] = String(s.SCHEDULE_DATE).slice(0,10).split("-"); return new Date(+y, +m-1, +d).toLocaleDateString("en-US", { month: "short", day: "numeric" }); })() : ""}</span>
                              <span className={clsx(
                                "px-1 rounded",
                                s.BLOCK_TYPE === "ON_CALL" ? "bg-amber-50 text-amber-700" :
                                s.BLOCK_TYPE === "TRAVEL" ? "bg-blue-50 text-blue-700" :
                                "bg-red-50 text-red-700"
                              )}>{s.BLOCK_TYPE}</span>
                              <span className="text-[var(--foreground)] truncate">{s.NOTES}</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}

              <div>
                <label className="block text-xs text-[var(--muted)] mb-1">Primary Asset</label>
                <input
                  type="number"
                  value={primaryAssetId}
                  onChange={(e) => setPrimaryAssetId(Number(e.target.value))}
                  className="w-full bg-[var(--input-bg)] border border-[var(--input-border)] rounded-lg px-3 py-2 text-sm text-[var(--input-text)]"
                />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs text-[var(--muted)] mb-1">Horizon (days)</label>
                  <input
                    type="number"
                    value={horizonDays}
                    onChange={(e) => setHorizonDays(Number(e.target.value))}
                    className="w-full bg-[var(--input-bg)] border border-[var(--input-border)] rounded-lg px-3 py-2 text-sm text-[var(--input-text)]"
                  />
                </div>
                <div>
                  <label className="block text-xs text-[var(--muted)] mb-1">Max Stops</label>
                  <input
                    type="number"
                    value={maxStops}
                    onChange={(e) => setMaxStops(Number(e.target.value))}
                    className="w-full bg-[var(--input-bg)] border border-[var(--input-border)] rounded-lg px-3 py-2 text-sm text-[var(--input-text)]"
                  />
                </div>
              </div>
              <button
                onClick={() => planMutation.mutate()}
                disabled={planMutation.isPending}
                className="w-full flex items-center justify-center gap-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white rounded-lg py-2.5 text-sm font-medium"
              >
                <Play size={14} />
                {planMutation.isPending ? "Planning..." : "Plan Route"}
              </button>
            </div>
          </div>

          {routeResult && (
            <div className="p-4 pt-0 space-y-3">
              <div className="border-t border-[var(--border)] pt-3">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-xs text-[var(--muted)]">
                    {routeResult.total_stops} stops · {routeResult.estimated_travel_miles} mi · {routeResult.total_days} day{routeResult.total_days > 1 ? "s" : ""}
                  </span>
                  <span className="flex items-center gap-1.5 text-xs font-medium text-[var(--foreground)]">
                    {selectedTech?.PHOTO_URL && (
                      <img src={selectedTech.PHOTO_URL} alt="" className="w-10 h-10 rounded-full object-cover" />
                    )}
                    {routeResult.tech_name}
                  </span>
                </div>

                {Object.entries(groupedByDay).map(([dayStr, stops]) => {
                  const day = Number(dayStr);
                  const dc = DAY_COLORS[(day - 1) % DAY_COLORS.length];
                  const dayDate = stops[0]?.scheduled_date || "";
                  const dayTotal = stops.reduce((sum, s) => sum + s.estimated_repair_hours + s.travel_hours, 0);

                  return (
                    <div key={day} className="mb-3">
                      <div className="flex items-center gap-2 mb-1.5">
                        <button
                          onClick={() => setFocusDay(focusDay === day ? null : day)}
                          className={clsx(
                            "text-[10px] font-bold px-2 py-0.5 rounded cursor-pointer transition-all",
                            dc.badge,
                            focusDay === day && "ring-2 ring-offset-1 ring-current scale-110"
                          )}
                        >
                          DAY {day}
                        </button>
                        <span className="text-[10px] text-[var(--muted)]">
                          {dayDate} · {dayTotal.toFixed(1)}h total
                        </span>
                      </div>

                      <div className="space-y-2">
                        {stops.map((stop: RouteStop, i: number) => (
                          <div
                            key={stop.asset_id}
                            className={clsx(
                              "rounded-lg border p-3",
                              stop.risk_level === "CRITICAL"
                                ? "border-[var(--red-border)] bg-[var(--red-surface)]"
                                : stop.risk_level === "WARNING"
                                ? "border-[var(--amber-border)] bg-[var(--amber-surface)]"
                                : "border-[var(--card-border)] bg-[var(--card)]"
                            )}
                          >
                            <div className="flex items-center gap-2 mb-1">
                              <span className="w-5 h-5 bg-indigo-600 text-white rounded-full flex items-center justify-center text-[10px] font-bold">
                                {stop.stop_number}
                              </span>
                              <span className="font-medium text-sm text-[var(--foreground)]">Asset {stop.asset_id}</span>
                              <span className="text-xs text-[var(--muted)]">{stop.asset_type}</span>
                              {stop.risk_level === "CRITICAL" && <AlertOctagon size={12} className="text-red-500" />}
                              {stop.risk_level === "WARNING" && <AlertTriangle size={12} className="text-amber-500" />}
                            </div>
                            <div className="text-xs text-[var(--muted)]">
                              <MapPin size={10} className="inline mr-1" />{stop.station}
                            </div>
                            <div className="text-xs text-[var(--muted)] mt-1">{stop.reason}</div>
                            <div className="flex items-center gap-3 mt-2 text-[10px] text-[var(--muted)]">
                              <span className="flex items-center gap-1">
                                <Wrench size={10} />
                                {stop.estimated_repair_hours}h repair
                              </span>
                              {stop.leg_miles > 0 && (
                                <span className="flex items-center gap-1">
                                  <MapPin size={10} />
                                  {stop.leg_miles} mi
                                </span>
                              )}
                              {stop.travel_hours > 0 && (
                                <span className="flex items-center gap-1">
                                  <Clock size={10} />
                                  {stop.travel_hours}h travel
                                </span>
                              )}
                            </div>
                            {stop.parts_needed.length > 0 && (
                              <div className="flex flex-wrap gap-1 mt-2">
                                {stop.parts_needed.map((p, j) => (
                                  <span key={j} className="text-[10px] px-1.5 py-0.5 bg-[var(--badge-bg)] rounded text-[var(--muted)]">
                                    {p.name}
                                  </span>
                                ))}
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>

              <div className="flex gap-2">
                <button
                  onClick={() => bundleMutation.mutate()}
                  disabled={bundleMutation.isPending || bundled}
                  className={clsx(
                    "flex-1 flex items-center justify-center gap-2 rounded-lg py-2.5 text-sm font-medium",
                    bundled
                      ? "bg-[var(--emerald-surface)] text-emerald-600 border border-[var(--emerald-border)]"
                      : "bg-amber-500 hover:bg-amber-400 text-white"
                  )}
                >
                  {bundled ? (
                    <><CheckCircle size={14} /> Work Orders Created</>
                  ) : (
                    <><ClipboardList size={14} /> {bundleMutation.isPending ? "Creating..." : "Bundle Work Orders"}</>
                  )}
                </button>
                <button
                  onClick={handleExplainRoute}
                  className="flex items-center gap-1 px-3 py-2.5 rounded-lg text-sm font-medium bg-indigo-600/10 text-indigo-600 hover:bg-indigo-600/20 border border-indigo-300"
                  title="Ask Cortex to explain route reasoning"
                >
                  <MessageSquare size={14} /> Explain
                </button>
              </div>
            </div>
          )}
        </div>

        <div className="flex-1 min-w-0 flex flex-col">
          <div className={clsx("min-w-0 transition-all", showSchedule ? "flex-1" : "flex-1")}>
            <FleetMap
              assets={assets || []}
              routeStops={routeStops}
              routeHome={routeHome}
              techHome={selectedTech ? { lat: selectedTech.HOME_BASE_LAT, lon: selectedTech.HOME_BASE_LON, name: selectedTech.NAME } : null}
              primaryAsset={primaryAssetLocation}
              focusDay={focusDay}
            />
          </div>
          <div className="border-t border-[var(--border)]">
            <button
              onClick={() => setShowSchedule(!showSchedule)}
              className="w-full flex items-center justify-between px-4 py-2 text-xs font-medium text-[var(--foreground)] bg-[var(--surface)] hover:bg-[var(--hover)]"
            >
              <span className="flex items-center gap-2">
                <Calendar size={12} />
                Technician Schedule
              </span>
              {showSchedule ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
            </button>
            {showSchedule && (
              <div className="max-h-[280px] overflow-auto bg-[var(--surface)]">
                <TechScheduleGantt
                  schedules={allSchedules || []}
                  technicians={(technicians || []).map((t: Technician) => ({
                    TECH_ID: t.TECH_ID,
                    NAME: t.NAME,
                    HOME_BASE_CITY: t.HOME_BASE_CITY,
                  }))}
                />
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function DispatchPage() {
  return (
    <Suspense>
      <DispatchContent />
    </Suspense>
  );
}
