"use client";

import { createContext, useContext, useState, useCallback, useMemo, useEffect, type ReactNode } from "react";

interface TimeTravelState {
  asOfTimestamp: string | null;
  activeOffset: string | null;
  isSimulation: boolean;
  setOffset: (label: string) => void;
  setCustomTime: (ts: string) => void;
  reset: () => void;
  toDisplayDate: (dataTs: string) => string;
  dataNow: string;
  isLoading: boolean;
}

function getInitialDate(): string {
  return "2026-03-18T00:00:00";
}

const TimeTravelContext = createContext<TimeTravelState>({
  asOfTimestamp: null,
  activeOffset: null,
  isSimulation: false,
  setOffset: () => {},
  setCustomTime: () => {},
  reset: () => {},
  toDisplayDate: (d) => d,
  dataNow: getInitialDate(),
  isLoading: true,
});

export function useTimeTravel() {
  return useContext(TimeTravelContext);
}

function addDays(baseTs: string, days: number): string {
  if (!baseTs) return getInitialDate();
  try {
    const d = new Date(baseTs);
    if (isNaN(d.getTime())) return getInitialDate();
    d.setDate(d.getDate() + days);
    return d.toISOString().slice(0, 10) + "T00:00:00";
  } catch {
    return getInitialDate();
  }
}

function computeOffsets(dataNowTs: string): Record<string, string> {
  return {
    "Now":  dataNowTs,
    "+24h": addDays(dataNowTs, 1),
    "+72h": addDays(dataNowTs, 3),
    "+7d":  addDays(dataNowTs, 7),
  };
}

export function shiftDate(dataTs: string): string {
  try {
    const d = new Date(dataTs);
    if (isNaN(d.getTime())) return dataTs;
    return d.toISOString().slice(0, 19).replace("T", " ");
  } catch {
    return dataTs;
  }
}

export function TimeTravelProvider({ children }: { children: ReactNode }) {
  const initialDate = getInitialDate();
  const [dataNowTs, setDataNowTs] = useState<string>(initialDate);
  const [isLoading, setIsLoading] = useState(true);
  const [asOfTimestamp, setAsOfTimestamp] = useState<string | null>(initialDate);
  const [activeOffset, setActiveOffset] = useState<string | null>("Now");

  useEffect(() => {
    fetch("/api/time-anchor")
      .then((res) => res.json())
      .then((data) => {
        const nowTs = data.now?.replace(" ", "T") || getInitialDate();
        setDataNowTs(nowTs);
        setAsOfTimestamp(nowTs);
        setIsLoading(false);
      })
      .catch(() => {
        setIsLoading(false);
      });
  }, []);

  const offsets = useMemo(() => computeOffsets(dataNowTs), [dataNowTs]);

  const toDisplayDate = useCallback((dataTs: string) => {
    return shiftDate(dataTs);
  }, []);

  const setOffset = useCallback((label: string) => {
    const ts = offsets[label];
    if (ts) {
      setAsOfTimestamp(ts);
      setActiveOffset(label);
    }
  }, [offsets]);

  const setCustomTime = useCallback((ts: string) => {
    setAsOfTimestamp(ts);
  }, []);

  const reset = useCallback(() => {
    setAsOfTimestamp(dataNowTs);
    setActiveOffset("Now");
  }, [dataNowTs]);

  return (
    <TimeTravelContext.Provider
      value={{
        asOfTimestamp,
        activeOffset,
        isSimulation: asOfTimestamp !== null && asOfTimestamp !== dataNowTs,
        setOffset,
        setCustomTime,
        reset,
        toDisplayDate,
        dataNow: dataNowTs,
        isLoading,
      }}
    >
      {children}
    </TimeTravelContext.Provider>
  );
}
