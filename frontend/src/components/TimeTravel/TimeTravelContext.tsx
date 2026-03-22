"use client";

import { createContext, useContext, useState, useCallback, useMemo, type ReactNode } from "react";

interface TimeTravelState {
  asOfTimestamp: string | null;
  activeOffset: string | null;
  isSimulation: boolean;
  setOffset: (label: string) => void;
  setCustomTime: (ts: string) => void;
  reset: () => void;
  toDisplayDate: (dataTs: string) => string;
  dataNow: string;
}

const TimeTravelContext = createContext<TimeTravelState>({
  asOfTimestamp: null,
  activeOffset: null,
  isSimulation: false,
  setOffset: () => {},
  setCustomTime: () => {},
  reset: () => {},
  toDisplayDate: (d) => d,
  dataNow: "",
});

export function useTimeTravel() {
  return useContext(TimeTravelContext);
}

const DATA_NOW_TS = "2026-03-13T00:00:00";

const OFFSETS: Record<string, string> = {
  "Now":  DATA_NOW_TS,
  "+24h": "2026-03-14T00:00:00",
  "+72h": "2026-03-16T00:00:00",
  "+7d":  "2026-03-20T00:00:00",
};

function computeOffsetMs(): number {
  const dataNow = new Date("2026-03-13T00:00:00");
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return today.getTime() - dataNow.getTime();
}

export function shiftDate(dataTs: string, offsetMs: number): string {
  const d = new Date(dataTs);
  d.setTime(d.getTime() + offsetMs);
  return d.toISOString().slice(0, 19).replace("T", " ");
}

export function TimeTravelProvider({ children }: { children: ReactNode }) {
  const [asOfTimestamp, setAsOfTimestamp] = useState<string | null>(DATA_NOW_TS);
  const [activeOffset, setActiveOffset] = useState<string | null>("Now");

  const offsetMs = useMemo(() => computeOffsetMs(), []);

  const toDisplayDate = useCallback((dataTs: string) => {
    return shiftDate(dataTs, offsetMs);
  }, [offsetMs]);

  const setOffset = useCallback((label: string) => {
    const ts = OFFSETS[label];
    if (ts) {
      setAsOfTimestamp(ts);
      setActiveOffset(label);
    }
  }, []);

  const setCustomTime = useCallback((ts: string) => {
    setAsOfTimestamp(ts);
  }, []);

  const reset = useCallback(() => {
    setAsOfTimestamp(DATA_NOW_TS);
    setActiveOffset("Now");
  }, []);

  return (
    <TimeTravelContext.Provider
      value={{
        asOfTimestamp,
        activeOffset,
        isSimulation: asOfTimestamp !== null && asOfTimestamp !== DATA_NOW_TS,
        setOffset,
        setCustomTime,
        reset,
        toDisplayDate,
        dataNow: DATA_NOW_TS,
      }}
    >
      {children}
    </TimeTravelContext.Provider>
  );
}
