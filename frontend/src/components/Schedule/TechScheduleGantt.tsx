"use client";

import clsx from "clsx";

interface ScheduleBlock {
  TECH_ID: string;
  TECH_NAME: string;
  SCHEDULE_DATE: string;
  BLOCK_TYPE: string;
  ASSET_ID: number | null;
  STATION_NAME: string | null;
  ESTIMATED_HOURS: number;
  NOTES: string;
  IS_BASELINE: boolean;
}

interface TechScheduleGanttProps {
  schedules: ScheduleBlock[];
  technicians: { TECH_ID: string; NAME: string; HOME_BASE_CITY: string }[];
  startDate?: string;
  numDays?: number;
}

const BLOCK_STYLES: Record<string, { bg: string; text: string }> = {
  WORK_ORDER: { bg: "bg-red-100", text: "text-red-700" },
  TRAVEL: { bg: "bg-blue-100", text: "text-blue-700" },
  ON_CALL: { bg: "bg-amber-100", text: "text-amber-700" },
  PTO: { bg: "bg-gray-200", text: "text-gray-600" },
};

export function TechScheduleGantt({ schedules, technicians, startDate, numDays = 7 }: TechScheduleGanttProps) {
  const baseDate = startDate ? new Date(startDate + "T00:00:00") : new Date();
  baseDate.setHours(0, 0, 0, 0);
  
  const dates: string[] = [];
  for (let i = 0; i < numDays; i++) {
    const d = new Date(baseDate);
    d.setDate(d.getDate() + i);
    dates.push(d.toISOString().slice(0, 10));
  }

  const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const todayStr = new Date().toISOString().slice(0, 10);

  const scheduleMap: Record<string, Record<string, ScheduleBlock[]>> = {};
  schedules.forEach((s) => {
    if (!scheduleMap[s.TECH_ID]) scheduleMap[s.TECH_ID] = {};
    if (!scheduleMap[s.TECH_ID][s.SCHEDULE_DATE]) scheduleMap[s.TECH_ID][s.SCHEDULE_DATE] = [];
    scheduleMap[s.TECH_ID][s.SCHEDULE_DATE].push(s);
  });

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs border-collapse">
        <thead>
          <tr>
            <th className="sticky left-0 bg-[var(--surface)] z-10 px-3 py-2 text-left text-[var(--muted)] border-b border-[var(--border)] w-36">
              Technician
            </th>
            {dates.map((d) => {
              const dt = new Date(d + "T12:00:00");
              const dayName = dayNames[dt.getDay()];
              const isToday = d === todayStr;
              return (
                <th
                  key={d}
                  className={clsx(
                    "px-2 py-2 text-center border-b border-l border-[var(--border)] min-w-[90px]",
                    isToday ? "bg-indigo-50 text-indigo-700" : "text-[var(--muted)]"
                  )}
                >
                  <div className="font-medium">{dayName}</div>
                  <div className="text-[10px]">{d.slice(5)}</div>
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {technicians.map((tech) => {
            const techSchedule = scheduleMap[tech.TECH_ID] || {};
            return (
              <tr key={tech.TECH_ID} className="border-b border-[var(--border)] hover:bg-[var(--hover)]">
                <td className="sticky left-0 bg-[var(--surface)] z-10 px-3 py-2">
                  <div className="font-medium text-[var(--foreground)]">{tech.NAME}</div>
                  <div className="text-[10px] text-[var(--muted)]">{tech.HOME_BASE_CITY}</div>
                </td>
                {dates.map((d) => {
                  const blocks = techSchedule[d] || [];
                  const totalHours = blocks.reduce((s, b) => s + b.ESTIMATED_HOURS, 0);

                  return (
                    <td key={d} className="px-1 py-1.5 border-l border-[var(--border)] align-top">
                      {blocks.length === 0 ? (
                        <div className="h-8 flex items-center justify-center">
                          <span className="text-[10px] text-emerald-500">Available</span>
                        </div>
                      ) : (
                        <div className="space-y-0.5">
                          {blocks.map((b, i) => {
                            const style = BLOCK_STYLES[b.BLOCK_TYPE] || BLOCK_STYLES.WORK_ORDER;
                            return (
                              <div
                                key={i}
                                className={clsx(
                                  "rounded px-1.5 py-0.5 truncate",
                                  style.bg,
                                  style.text,
                                  !b.IS_BASELINE && "ring-1 ring-indigo-400"
                                )}
                                title={b.NOTES || b.BLOCK_TYPE}
                              >
                                <div className="font-medium truncate">
                                  {b.BLOCK_TYPE === "WORK_ORDER" && b.ASSET_ID ? `A${b.ASSET_ID}` : b.BLOCK_TYPE}
                                </div>
                                <div className="text-[9px] opacity-70">{b.ESTIMATED_HOURS}h</div>
                              </div>
                            );
                          })}
                          {totalHours < 8 && (
                            <div className="text-[9px] text-emerald-500 text-center">
                              {(8 - totalHours).toFixed(0)}h free
                            </div>
                          )}
                        </div>
                      )}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
