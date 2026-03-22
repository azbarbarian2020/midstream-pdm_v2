"use client";

import { createContext, useContext, useState, ReactNode } from "react";

interface SelectedAssetContextType {
  selectedAssetId: number | null;
  setSelectedAssetId: (id: number | null) => void;
}

const SelectedAssetContext = createContext<SelectedAssetContextType | undefined>(undefined);

export function SelectedAssetProvider({ children }: { children: ReactNode }) {
  const [selectedAssetId, setSelectedAssetId] = useState<number | null>(null);

  return (
    <SelectedAssetContext.Provider value={{ selectedAssetId, setSelectedAssetId }}>
      {children}
    </SelectedAssetContext.Provider>
  );
}

export function useSelectedAsset() {
  const context = useContext(SelectedAssetContext);
  if (context === undefined) {
    throw new Error("useSelectedAsset must be used within a SelectedAssetProvider");
  }
  return context;
}
