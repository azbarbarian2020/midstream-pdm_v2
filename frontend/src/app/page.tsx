"use client";

import { useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { useTimeTravel } from "@/components/TimeTravel/TimeTravelContext";
import { useChatContext } from "@/components/Chat/ChatContext";
import { Header } from "@/components/Header";
import { KPICards } from "@/components/KPI/KPICards";
import { FleetMap } from "@/components/Map/FleetMap";
import { AlertList } from "@/components/Map/AlertList";
import type { Asset } from "@/lib/types";

export default function DashboardPage() {
  const { asOfTimestamp } = useTimeTravel();
  const { open } = useChatContext();

  const { data: assets, isLoading: assetsLoading } = useQuery({
    queryKey: ["assets", asOfTimestamp],
    queryFn: () => api.getAssets(asOfTimestamp || undefined),
  });

  const { data: kpis, isLoading: kpisLoading } = useQuery({
    queryKey: ["kpis", asOfTimestamp],
    queryFn: () => api.getKPIs(asOfTimestamp || undefined),
  });

  const handleExplain = useCallback((asset: Asset) => {
    open(undefined, {
      asset_id: asset.ASSET_ID,
      asset_type: asset.ASSET_TYPE,
      predicted_class: asset.PREDICTED_CLASS || "UNKNOWN",
      rul_days: asset.PREDICTED_RUL_DAYS || 0,
    });
  }, [open]);

  return (
    <div className="h-screen flex flex-col">
      <Header />

      <div className="px-4 py-3">
        <KPICards kpis={kpis} isLoading={kpisLoading} />
      </div>

      <div className="flex-1 flex gap-4 px-4 pb-4 min-h-0">
        <div className="flex-1 min-w-0">
          <FleetMap
            assets={assets || []}
            onExplainClick={handleExplain}
          />
        </div>
        <div className="w-72 flex-shrink-0">
          <AlertList assets={assets || []} />
        </div>
      </div>
    </div>
  );
}
