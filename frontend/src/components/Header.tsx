"use client";

import Link from "next/link";
import Image from "next/image";
import { useTheme } from "@/components/Theme/ThemeContext";
import { TimeTravelBar } from "@/components/TimeTravel/TimeTravelBar";
import { Sun, Moon } from "lucide-react";

export function Header() {
  const { theme, toggleTheme } = useTheme();

  return (
    <header className="flex items-center justify-between px-4 py-2 border-b border-[var(--border)] bg-[var(--surface)]/80 backdrop-blur">
      <div className="flex items-center gap-4">
        <Link href="/" className="flex items-center gap-2">
          <Image src="/logo.png" alt="Predictive Asset Navigator" width={36} height={36} />
          <span className="text-base font-bold tracking-tight text-[var(--foreground)]">
            Predictive Asset Navigator
          </span>
        </Link>
      </div>
      <div className="flex items-center gap-3">
        <TimeTravelBar />
        <button
          onClick={toggleTheme}
          className="p-2 rounded-lg hover:bg-[var(--hover)] text-[var(--muted)] transition-colors"
          title={`Switch to ${theme === "light" ? "dark" : "light"} mode`}
        >
          {theme === "light" ? <Moon size={16} /> : <Sun size={16} />}
        </button>
      </div>
    </header>
  );
}
