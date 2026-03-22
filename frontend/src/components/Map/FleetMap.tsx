"use client";

import { useState, useCallback, useMemo, useRef, useEffect } from "react";
import MapGL, { Marker, Popup, Source, Layer, NavigationControl } from "react-map-gl/maplibre";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import type { Asset } from "@/lib/types";
import { useRouter } from "next/navigation";
import { useSelectedAsset } from "@/components/Asset/SelectedAssetContext";

interface FleetMapProps {
  assets: Asset[];
  onAssetClick?: (asset: Asset) => void;
  onExplainClick?: (asset: Asset) => void;
  routeStops?: { lat: number; lon: number; asset_id: number; stop_number: number; scheduled_day?: number }[];
  routeHome?: { lat: number; lon: number };
  techHome?: { lat: number; lon: number; name: string } | null;
  primaryAsset?: { lat: number; lon: number; name: string; risk: string } | null;
  focusDay?: number | null;
}

const RISK_COLORS: Record<string, string> = {
  FAILED: "#7f1d1d",
  CRITICAL: "#ef4444",
  WARNING: "#f59e0b",
  HEALTHY: "#22c55e",
  OFFLINE: "#6b7280",
};

const ZOOM_THRESHOLD = 10.5;
const INITIAL_CENTER = { longitude: -102.3, latitude: 31.85 };
const INITIAL_ZOOM = 7.5;

interface StationGroup {
  key: string;
  lat: number;
  lon: number;
  name: string;
  assets: Asset[];
  criticalCount: number;
  warningCount: number;
  healthyCount: number;
  offlineCount: number;
  scheduledCount: number;
}

interface AssetWithOffset {
  asset: Asset;
  lng: number;
  lat: number;
  stationName: string;
}

export function FleetMap({ assets, onExplainClick, routeStops, routeHome, techHome, primaryAsset, focusDay }: FleetMapProps) {
  const router = useRouter();
  const { setSelectedAssetId } = useSelectedAsset();
  const mapRef = useRef<any>(null);
  const [viewState, setViewState] = useState({
    longitude: INITIAL_CENTER.longitude,
    latitude: INITIAL_CENTER.latitude,
    zoom: INITIAL_ZOOM,
  });
  const [hoveredStation, setHoveredStation] = useState<string | null>(null);
  const [hoveredAsset, setHoveredAsset] = useState<AssetWithOffset | null>(null);
  const [selectedAsset, setSelectedAsset] = useState<AssetWithOffset | null>(null);
  const [routeGeoJsons, setRouteGeoJsons] = useState<{ id: string; data: any; color: string; opacity: number; width: number; offset: number }[]>([]);
  const routeGenRef = useRef(0);

  const isZoomed = viewState.zoom >= ZOOM_THRESHOLD;

  const stations = useMemo<StationGroup[]>(() => {
    const grouped = new Map<string, Asset[]>();
    assets.forEach((a) => {
      const key = `${a.LAT},${a.LON}`;
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key)!.push(a);
    });
    return Array.from(grouped.entries()).map(([key, stationAssets]) => {
      const [lat, lon] = key.split(",").map(Number);
      return {
        key,
        lat,
        lon,
        name: stationAssets[0]?.STATION_NAME || "Station",
        assets: stationAssets,
        criticalCount: stationAssets.filter((a) => a.RISK_LEVEL === "CRITICAL" || a.RISK_LEVEL === "FAILED").length,
        warningCount: stationAssets.filter((a) => a.RISK_LEVEL === "WARNING").length,
        offlineCount: stationAssets.filter((a) => a.RISK_LEVEL === "OFFLINE").length,
        healthyCount: stationAssets.filter((a) => a.RISK_LEVEL !== "CRITICAL" && a.RISK_LEVEL !== "WARNING" && a.RISK_LEVEL !== "OFFLINE" && a.RISK_LEVEL !== "FAILED").length,
        scheduledCount: stationAssets.filter((a) => a.ASSIGNED_TECH_ID).length,
      };
    });
  }, [assets]);

  const assetPositions = useMemo<AssetWithOffset[]>(() => {
    if (!isZoomed) return [];
    const results: AssetWithOffset[] = [];
    const zoom = viewState.zoom;

    stations.forEach((station) => {
      const total = station.assets.length;
      const pixelRadius = Math.max(50, total * 12);
      const metersPerPixel = 156543.03392 * Math.cos(station.lat * Math.PI / 180) / Math.pow(2, zoom);
      const radiusMeters = pixelRadius * metersPerPixel;

      station.assets.forEach((asset, i) => {
        const angle = (2 * Math.PI * i) / total - Math.PI / 2;
        const dxMeters = radiusMeters * Math.cos(angle);
        const dyMeters = radiusMeters * Math.sin(angle);
        const dLat = dyMeters / 111320;
        const dLon = dxMeters / (111320 * Math.cos(station.lat * Math.PI / 180));

        results.push({
          asset,
          lng: station.lon + dLon,
          lat: station.lat - dLat,
          stationName: station.name,
        });
      });
    });
    return results;
  }, [isZoomed, stations, viewState.zoom]);

  const handleStationClick = useCallback((station: StationGroup) => {
    setViewState((v) => ({ ...v, longitude: station.lon, latitude: station.lat, zoom: 12 }));
  }, []);

  const handleAssetSelect = useCallback((ap: AssetWithOffset) => {
    setSelectedAsset(ap);
    setSelectedAssetId(ap.asset.ASSET_ID);
    setHoveredAsset(null);
  }, [setSelectedAssetId]);

  const resetView = useCallback(() => {
    setViewState({ longitude: INITIAL_CENTER.longitude, latitude: INITIAL_CENTER.latitude, zoom: INITIAL_ZOOM });
    setSelectedAsset(null);
    setSelectedAssetId(null);
    setHoveredStation(null);
  }, [setSelectedAssetId]);

  const stationColor = useCallback((s: StationGroup) => {
    const total = s.assets.length;
    const critPct = s.criticalCount / total;
    const warnPct = s.warningCount / total;
    if (critPct >= 0.4) return RISK_COLORS.CRITICAL;
    if (s.criticalCount > 0 && critPct >= 0.2) return "#f87171";
    if (warnPct + critPct > 0.5) return RISK_COLORS.WARNING;
    if (s.warningCount > 0 || s.criticalCount > 0) return "#a3e635";
    return RISK_COLORS.HEALTHY;
  }, []);

  useEffect(() => {
    if (!routeStops || routeStops.length < 1) {
      setRouteGeoJsons([]);
      return;
    }

    const gen = ++routeGenRef.current;
    const DAY_LINE_COLORS = ["#3b82f6", "#f97316", "#22c55e", "#8b5cf6", "#ec4899"];
    const totalDays = new Set(routeStops.map((s) => s.scheduled_day ?? 1)).size;
    const homePt: [number, number] = routeHome ? [routeHome.lon, routeHome.lat] : [routeStops[0].lon, routeStops[0].lat];
    const lineOffsets = [-3, 3, -6, 6, 0];

    const segments: { day: number; waypoints: [number, number][] }[] = [];
    let currentDay = routeStops[0]?.scheduled_day ?? 1;
    let currentWaypoints: [number, number][] = [homePt];

    routeStops.forEach((stop) => {
      const day = stop.scheduled_day ?? 1;
      const pt: [number, number] = [stop.lon, stop.lat];
      if (day !== currentDay) {
        segments.push({ day: currentDay, waypoints: currentWaypoints });
        currentDay = day;
        currentWaypoints = [homePt];
      }
      currentWaypoints.push(pt);
    });
    segments.push({ day: currentDay, waypoints: currentWaypoints });

    Promise.all(
      segments.map(async (seg, idx) => {
        const color = DAY_LINE_COLORS[(seg.day - 1) % DAY_LINE_COLORS.length];
        const isFocused = focusDay == null || focusDay === seg.day;
        const opacity = isFocused ? 0.9 : 0.15;
        const width = isFocused ? 4 : 2;
        const offset = totalDays > 1 ? lineOffsets[idx % lineOffsets.length] : 0;

        try {
          const coordStr = seg.waypoints.map((w) => `${w[0]},${w[1]}`).join(";");
          const resp = await fetch(`https://router.project-osrm.org/route/v1/driving/${coordStr}?overview=full&geometries=geojson`);
          const data = await resp.json();
          if (data.routes?.[0]) {
            return { id: `route-seg-${idx}`, data: { type: "Feature" as const, properties: {}, geometry: data.routes[0].geometry }, color, opacity, width, offset };
          }
        } catch {}
        return {
          id: `route-seg-${idx}`,
          data: { type: "Feature" as const, properties: {}, geometry: { type: "LineString" as const, coordinates: seg.waypoints } },
          color, opacity, width: width, offset,
        };
      })
    ).then((results) => {
      if (routeGenRef.current === gen) setRouteGeoJsons(results);
    });

    if (routeHome) {
      const allPts: [number, number][] = [[routeHome.lon, routeHome.lat]];
      const ptsToFit = focusDay != null ? routeStops.filter((s) => (s.scheduled_day ?? 1) === focusDay) : routeStops;
      ptsToFit.forEach((s) => allPts.push([s.lon, s.lat]));
      if (allPts.length > 0) {
        const lngs = allPts.map((p) => p[0]);
        const lats = allPts.map((p) => p[1]);
        setTimeout(() => {
          mapRef.current?.fitBounds(
            [[Math.min(...lngs) - 0.1, Math.min(...lats) - 0.1], [Math.max(...lngs) + 0.1, Math.max(...lats) + 0.1]],
            { padding: 60, duration: 800 }
          );
        }, 100);
      }
    }
  }, [routeStops, routeHome, focusDay]);

  useEffect(() => {
    if (!techHome || routeStops?.length) return;
    if (primaryAsset) {
      const lngs = [techHome.lon, primaryAsset.lon];
      const lats = [techHome.lat, primaryAsset.lat];
      setTimeout(() => {
        mapRef.current?.fitBounds(
          [[Math.min(...lngs) - 0.2, Math.min(...lats) - 0.2], [Math.max(...lngs) + 0.2, Math.max(...lats) + 0.2]],
          { padding: 80, duration: 800 }
        );
      }, 100);
    } else {
      setViewState((v) => ({ ...v, longitude: techHome.lon, latitude: techHome.lat, zoom: 9 }));
    }
  }, [techHome, routeStops, primaryAsset]);

  const DAY_LINE_COLORS = ["#3b82f6", "#f97316", "#22c55e", "#8b5cf6", "#ec4899"];

  return (
    <div className="relative w-full h-full">
      <MapGL
        ref={mapRef}
        {...viewState}
        onMove={(evt) => setViewState(evt.viewState)}
        mapLib={maplibregl}
        mapStyle={{
          version: 8,
          sources: {
            osm: { type: "raster", tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"], tileSize: 256, attribution: "&copy; OpenStreetMap contributors" },
          },
          glyphs: "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
          layers: [{ id: "osm-tiles", type: "raster", source: "osm", minzoom: 0, maxzoom: 19 }],
        }}
        style={{ width: "100%", height: "100%" }}
      >
        <NavigationControl position="top-right" />

        {routeGeoJsons.map((rg) => (
          <Source key={rg.id} id={rg.id} type="geojson" data={rg.data}>
            <Layer
              id={`${rg.id}-line`}
              type="line"
              paint={{ "line-color": rg.color, "line-width": rg.width, "line-opacity": rg.opacity, "line-offset": rg.offset }}
            />
          </Source>
        ))}

        {!isZoomed && stations.map((station) => (
          <Marker
            key={station.key}
            longitude={station.lon}
            latitude={station.lat}
            anchor="center"
            onClick={(e) => { e.originalEvent.stopPropagation(); handleStationClick(station); }}
          >
            <div
              className="flex flex-col items-center gap-0.5 cursor-pointer"
              onMouseEnter={() => setHoveredStation(station.key)}
              onMouseLeave={() => setHoveredStation(null)}
            >
              <div
                className="rounded-full border-2 border-white flex items-center justify-center font-bold text-white shadow-md"
                style={{
                  width: station.criticalCount > 0 ? 32 : station.warningCount > 0 ? 28 : 24,
                  height: station.criticalCount > 0 ? 32 : station.warningCount > 0 ? 28 : 24,
                  background: stationColor(station),
                  fontSize: station.criticalCount > 0 ? 11 : 9,
                  position: "relative",
                }}
              >
                {station.assets.length}
                {station.scheduledCount > 0 && (
                  <div className="absolute -top-1 -right-1 w-4 h-4 rounded-full bg-indigo-500 border-2 border-white flex items-center justify-center shadow-sm">
                    <svg width="8" height="8" viewBox="0 0 24 24" fill="white" stroke="none"><path d="M22.7 19l-9.1-9.1c.9-2.3.4-5-1.5-6.9-2-2-5-2.4-7.4-1.3L9 6 6 9 1.6 4.7C.4 7.1.9 10.1 2.9 12.1c1.9 1.9 4.6 2.4 6.9 1.5l9.1 9.1c.4.4 1 .4 1.4 0l2.3-2.3c.5-.4.5-1.1.1-1.4z"/></svg>
                  </div>
                )}
              </div>
              <div className="text-[10px] font-semibold text-black bg-white/95 px-1.5 py-0.5 rounded whitespace-nowrap shadow-sm pointer-events-none">
                {station.name}
              </div>
              {(station.criticalCount > 0 || station.warningCount > 0 || station.offlineCount > 0) && (
                <div className="text-[9px] text-black pointer-events-none whitespace-nowrap">
                  {[
                    station.offlineCount > 0 ? `${station.offlineCount}OFF` : null,
                    station.criticalCount > 0 ? `${station.criticalCount}C` : null,
                    station.warningCount > 0 ? `${station.warningCount}W` : null,
                    `${station.healthyCount}H`,
                  ].filter(Boolean).join(" / ")}
                </div>
              )}
            </div>
          </Marker>
        ))}

        {hoveredStation && !isZoomed && (() => {
          const station = stations.find((s) => s.key === hoveredStation);
          if (!station) return null;
          return (
            <Popup
              longitude={station.lon}
              latitude={station.lat}
              closeButton={false}
              closeOnClick={false}
              anchor="bottom"
              offset={[0, -15] as [number, number]}
            >
              <div className="font-sans min-w-[170px]">
                <div className="font-bold text-xs mb-1 pb-1 border-b border-gray-200">{station.name}</div>
                {station.assets
                  .sort((a, b) => {
                    const order: Record<string, number> = { FAILED: -1, OFFLINE: 0, CRITICAL: 1, WARNING: 2, HEALTHY: 3 };
                    return (order[a.RISK_LEVEL || "HEALTHY"] ?? 2) - (order[b.RISK_LEVEL || "HEALTHY"] ?? 2);
                  })
                  .map((a) => {
                    const risk = a.RISK_LEVEL || "HEALTHY";
                    const color = RISK_COLORS[risk] || RISK_COLORS.HEALTHY;
                    return (
                      <div key={a.ASSET_ID} className="flex items-center gap-1.5 py-0.5">
                        <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ background: color }} />
                        <span className="font-semibold min-w-[18px] text-[11px]">{a.ASSET_ID}</span>
                        <span className="text-[10px] text-black">{a.ASSET_TYPE === "PUMP" ? "P" : "C"}</span>
                        <span className="flex-1 text-[10px] text-black">{a.PREDICTED_CLASS === "NORMAL" ? "" : a.PREDICTED_CLASS?.replace("_", " ")}</span>
                        <span className="text-[10px] text-gray-800">{risk === "HEALTHY" ? "" : `${a.PREDICTED_RUL_DAYS?.toFixed(1)}d`}</span>
                        {a.ASSIGNED_TECH_ID ? (
                          <span className="text-indigo-500 text-[9px]" title={a.ASSIGNED_TECH_NAME || a.ASSIGNED_TECH_ID}>✓</span>
                        ) : risk !== "HEALTHY" ? (
                          <span className="text-red-600 text-[9px]">⚠</span>
                        ) : null}
                      </div>
                    );
                  })}
              </div>
            </Popup>
          );
        })()}

        {isZoomed && stations.map((station) => (
          <Marker key={`label-${station.key}`} longitude={station.lon} latitude={station.lat} anchor="center">
            <div className="text-[11px] font-bold text-black bg-white/95 px-2 py-0.5 rounded whitespace-nowrap shadow-md pointer-events-none">
              {station.name}
            </div>
          </Marker>
        ))}

        {isZoomed && assetPositions.map((ap) => {
          const risk = ap.asset.RISK_LEVEL || "HEALTHY";
          const color = RISK_COLORS[risk] || RISK_COLORS.HEALTHY;
          const isPump = ap.asset.ASSET_TYPE === "PUMP";
          const size = risk === "FAILED" ? 30 : risk === "CRITICAL" ? 28 : risk === "WARNING" ? 24 : 20;

          return (
            <Marker
              key={`asset-${ap.asset.ASSET_ID}`}
              longitude={ap.lng}
              latitude={ap.lat}
              anchor="center"
              onClick={(e) => { e.originalEvent.stopPropagation(); handleAssetSelect(ap); }}
            >
              <div
                className="flex items-center justify-center text-white font-bold cursor-pointer relative"
                style={{
                  width: size,
                  height: size,
                  borderRadius: isPump ? "50%" : 4,
                  background: color,
                  border: "2px solid white",
                  fontSize: 10,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.4)",
                }}
                onMouseEnter={() => setHoveredAsset(ap)}
                onMouseLeave={() => setHoveredAsset(null)}
              >
                {isPump ? "P" : "C"}
                {ap.asset.ASSIGNED_TECH_ID && (
                  <div className="absolute -top-1.5 -right-1.5 w-3 h-3 rounded-full bg-indigo-500 border border-white flex items-center justify-center shadow-sm">
                    <svg width="6" height="6" viewBox="0 0 24 24" fill="white" stroke="none"><path d="M22.7 19l-9.1-9.1c.9-2.3.4-5-1.5-6.9-2-2-5-2.4-7.4-1.3L9 6 6 9 1.6 4.7C.4 7.1.9 10.1 2.9 12.1c1.9 1.9 4.6 2.4 6.9 1.5l9.1 9.1c.4.4 1 .4 1.4 0l2.3-2.3c.5-.4.5-1.1.1-1.4z"/></svg>
                  </div>
                )}
              </div>
            </Marker>
          );
        })}

        {hoveredAsset && !selectedAsset && (
          <Popup
            longitude={hoveredAsset.lng}
            latitude={hoveredAsset.lat}
            closeButton={false}
            closeOnClick={false}
            anchor="bottom"
            offset={[0, -15] as [number, number]}
          >
            <div className="font-sans whitespace-nowrap">
              <div className="font-bold text-xs">Asset {hoveredAsset.asset.ASSET_ID} · {hoveredAsset.asset.ASSET_TYPE}</div>
              <div className="flex items-center gap-1.5 mt-1">
                <span
                  className="text-white text-[10px] font-semibold px-1.5 rounded-full"
                  style={{ background: RISK_COLORS[hoveredAsset.asset.RISK_LEVEL || "HEALTHY"] }}
                >
                  {hoveredAsset.asset.RISK_LEVEL || "HEALTHY"}
                </span>
                <span className="text-[11px] text-black">
                  {hoveredAsset.asset.PREDICTED_CLASS === "NORMAL" ? "Normal" : hoveredAsset.asset.PREDICTED_CLASS?.replace("_", " ")}
                </span>
              </div>
              {hoveredAsset.asset.RISK_LEVEL !== "HEALTHY" && (
                <div className="text-[11px] text-gray-800 mt-0.5">RUL: <strong>{hoveredAsset.asset.PREDICTED_RUL_DAYS?.toFixed(1)}</strong> days</div>
              )}
              {hoveredAsset.asset.ASSIGNED_TECH_ID ? (
                <div className="text-[10px] text-indigo-600 mt-0.5 font-semibold">✓ Assigned: {hoveredAsset.asset.ASSIGNED_TECH_NAME || hoveredAsset.asset.ASSIGNED_TECH_ID}</div>
              ) : hoveredAsset.asset.RISK_LEVEL !== "HEALTHY" ? (
                <div className="text-[10px] text-red-600 mt-0.5 font-semibold">⚠ Unassigned</div>
              ) : null}
              <div className="text-[10px] text-gray-500 mt-0.5">Click for details</div>
            </div>
          </Popup>
        )}

        {selectedAsset && (
          <Popup
            longitude={selectedAsset.lng}
            latitude={selectedAsset.lat}
            closeButton={true}
            closeOnClick={false}
            anchor="bottom"
            offset={[0, -15] as [number, number]}
            onClose={() => { setSelectedAsset(null); setSelectedAssetId(null); }}
          >
            <div className="font-sans min-w-[200px]">
              <div className="font-bold text-[13px] mb-1">Asset {selectedAsset.asset.ASSET_ID} · {selectedAsset.asset.ASSET_TYPE}</div>
              <div className="text-[11px] text-gray-800 mb-0.5">{selectedAsset.stationName}</div>
              <div className="flex items-center gap-1.5 my-1.5">
                <span
                  className="text-white text-[10px] font-semibold px-2 py-0.5 rounded-full"
                  style={{ background: RISK_COLORS[selectedAsset.asset.RISK_LEVEL || "HEALTHY"] }}
                >
                  {selectedAsset.asset.RISK_LEVEL || "HEALTHY"}
                </span>
                <span className="text-[11px] text-black">{selectedAsset.asset.PREDICTED_CLASS || "NORMAL"}</span>
              </div>
              <div className="text-[11px] text-gray-800 mb-2">
                RUL: <strong>{selectedAsset.asset.PREDICTED_RUL_DAYS?.toFixed(1) ?? "—"}</strong> days · Model: {selectedAsset.asset.MODEL_NAME || "—"}
              </div>
              <div className="flex gap-1.5">
                <button
                  onClick={() => router.push(`/asset/${selectedAsset.asset.ASSET_ID}`)}
                  className="flex-1 text-[11px] py-1 px-2 border border-gray-300 rounded-md cursor-pointer bg-white font-semibold hover:bg-gray-50"
                >
                  View Details
                </button>
                <button
                  onClick={() => { if (onExplainClick) onExplainClick(selectedAsset.asset); }}
                  className="flex-1 text-[11px] py-1 px-2 border border-indigo-500 rounded-md cursor-pointer bg-indigo-50 text-indigo-700 font-semibold hover:bg-indigo-100"
                >
                  Explain
                </button>
              </div>
            </div>
          </Popup>
        )}

        {routeHome && (
          <Marker longitude={routeHome.lon} latitude={routeHome.lat} anchor="center" style={{ zIndex: 1000 }}>
            <div
              className="tech-home-icon w-[30px] h-[30px] rounded-full bg-indigo-500 border-[3px] border-white flex items-center justify-center shadow-lg"
              title="Home Base"
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="white" stroke="none"><path d="M12 2L2 12h3v8h6v-6h2v6h6v-8h3L12 2z"/></svg>
            </div>
          </Marker>
        )}

        {routeStops?.map((stop) => {
          const day = stop.scheduled_day ?? 1;
          const isFocused = focusDay == null || focusDay === day;
          const dayColor = DAY_LINE_COLORS[(day - 1) % DAY_LINE_COLORS.length];
          return (
            <Marker key={`stop-${stop.stop_number}`} longitude={stop.lon} latitude={stop.lat} anchor="center">
              <div
                className="w-[22px] h-[22px] rounded-full border-2 border-white flex items-center justify-center text-[11px] font-bold text-white shadow-md transition-opacity"
                style={{ background: dayColor, opacity: isFocused ? 1 : 0.25 }}
              >
                {stop.stop_number}
              </div>
            </Marker>
          );
        })}

        {techHome && !routeStops?.length && (
          <Marker longitude={techHome.lon} latitude={techHome.lat} anchor="center" style={{ zIndex: 1000 }}>
            <div className="tech-home-icon w-8 h-8 rounded-full bg-indigo-500 border-[3px] border-white flex items-center justify-center shadow-lg">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="white" stroke="none"><path d="M12 2L2 12h3v8h6v-6h2v6h6v-8h3L12 2z"/></svg>
            </div>
          </Marker>
        )}

        {primaryAsset && !routeStops?.length && (
          <Marker longitude={primaryAsset.lon} latitude={primaryAsset.lat} anchor="center">
            <div
              className="w-8 h-8 rounded-full border-[3px] border-white flex items-center justify-center shadow-lg animate-pulse"
              style={{ background: RISK_COLORS[primaryAsset.risk] || RISK_COLORS.HEALTHY }}
              title={primaryAsset.name}
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="white" stroke="none"><path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5s1.12-2.5 2.5-2.5 2.5 1.12 2.5 2.5-1.12 2.5-2.5 2.5z"/></svg>
            </div>
          </Marker>
        )}
      </MapGL>

      <button
        onClick={resetView}
        className="absolute top-2 left-2 z-10 bg-white hover:bg-gray-50 text-gray-700 text-xs font-semibold px-3 py-1.5 rounded-md shadow border border-gray-200 flex items-center gap-1.5"
        title="Reset to fleet view"
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/><line x1="8" y1="11" x2="14" y2="11"/></svg>
        Fleet View
      </button>
    </div>
  );
}
