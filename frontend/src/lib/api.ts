const API_BASE = "";

async function fetchJSON<T>(url: string, options?: RequestInit): Promise<T> {
  const resp = await fetch(`${API_BASE}${url}`, {
    ...options,
    headers: { "Content-Type": "application/json", ...options?.headers },
  });
  if (!resp.ok) {
    throw new Error(`API error: ${resp.status} ${resp.statusText}`);
  }
  return resp.json();
}

export const api = {
  getAssets: (asOfTs?: string) =>
    fetchJSON<any[]>(`/api/assets${asOfTs ? `?as_of_ts=${encodeURIComponent(asOfTs)}` : ""}`),

  getAsset: (id: number, asOfTs?: string) =>
    fetchJSON<any>(`/api/assets/${id}${asOfTs ? `?as_of_ts=${encodeURIComponent(asOfTs)}` : ""}`),

  getTelemetry: (id: number, start?: string, end?: string, sensors?: string) => {
    const params = new URLSearchParams();
    if (start) params.set("start", start);
    if (end) params.set("end", end);
    if (sensors) params.set("sensors", sensors);
    const qs = params.toString();
    return fetchJSON<any[]>(`/api/assets/${id}/telemetry${qs ? `?${qs}` : ""}`);
  },

  getStations: () => fetchJSON<any[]>("/api/stations"),

  getPredictions: (asOfTs?: string) =>
    fetchJSON<any[]>(`/api/predictions${asOfTs ? `?as_of_ts=${encodeURIComponent(asOfTs)}` : ""}`),

  getPredictionHistory: (id: number, asOfTs?: string) =>
    fetchJSON<any[]>(`/api/predictions/${id}${asOfTs ? `?as_of_ts=${encodeURIComponent(asOfTs)}` : ""}`),

  getKPIs: (asOfTs?: string) =>
    fetchJSON<any>(`/api/kpis${asOfTs ? `?as_of_ts=${encodeURIComponent(asOfTs)}` : ""}`),

  getMaintenance: (id: number) => fetchJSON<any[]>(`/api/maintenance/${id}`),

  createWorkOrder: (data: any) =>
    fetchJSON<any>("/api/work-orders", { method: "POST", body: JSON.stringify(data) }),

  createThread: () =>
    fetchJSON<{ thread_id: string }>("/api/agent/thread", { method: "POST", body: "{}" }),

  sendMessage: async (threadId: string, message: string, parentMessageId?: string, context?: any, asOfTs?: string, offsetLabel?: string) => {
    const resp = await fetch(`${API_BASE}/api/agent/message`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        thread_id: threadId,
        message,
        parent_message_id: parentMessageId || "0",
        context,
        as_of_ts: asOfTs,
        offset_label: offsetLabel,
      }),
    });
    return resp;
  },

  planRoute: (data: { tech_id: string; primary_asset_id: number; horizon_days?: number; max_stops?: number; as_of_ts?: string; allow_overtime?: boolean }) =>
    fetchJSON<any>("/api/dispatch/plan-route", { method: "POST", body: JSON.stringify(data) }),

  bundleWorkOrders: (data: { tech_id: string; stops: any[] }) =>
    fetchJSON<any>("/api/dispatch/bundle-wo", { method: "POST", body: JSON.stringify(data) }),

  resetDemo: () =>
    fetchJSON<any>("/api/dispatch/reset-demo", { method: "POST" }),

  getTechnicians: () => fetchJSON<any[]>("/api/technicians"),

  getTechnician: (techId: string) => fetchJSON<any>(`/api/technicians/${techId}`),

  getTechSchedules: (techId?: string, startDate?: string, endDate?: string) => {
    const params = new URLSearchParams();
    if (techId) params.set("tech_id", techId);
    if (startDate) params.set("start_date", startDate);
    if (endDate) params.set("end_date", endDate);
    const qs = params.toString();
    return fetchJSON<any[]>(`/api/tech-schedules${qs ? `?${qs}` : ""}`);
  },

  getTechAvailability: (startDate: string, endDate: string) =>
    fetchJSON<any[]>(`/api/tech-availability?start_date=${startDate}&end_date=${endDate}`),

  suggestTech: (data: { primary_asset_id: number; horizon_days?: number; as_of_ts?: string }) =>
    fetchJSON<any>("/api/dispatch/suggest-tech", { method: "POST", body: JSON.stringify(data) }),

  health: () => fetchJSON<any>("/api/health"),
};
