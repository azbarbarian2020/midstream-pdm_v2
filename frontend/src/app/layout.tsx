import type { Metadata } from "next";
import { QueryProvider } from "@/components/QueryProvider";
import { TimeTravelProvider } from "@/components/TimeTravel/TimeTravelContext";
import { ThemeProvider } from "@/components/Theme/ThemeContext";
import { ChatProvider } from "@/components/Chat/ChatContext";
import { ChatPanel } from "@/components/Chat/ChatPanel";
import { SelectedAssetProvider } from "@/components/Asset/SelectedAssetContext";
import "./globals.css";

export const metadata: Metadata = {
  title: "Predictive Asset Navigator",
  description: "AI-powered predictive maintenance for midstream pipeline operations",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" data-theme="light" suppressHydrationWarning>
      <body className="min-h-screen">
        <QueryProvider>
          <TimeTravelProvider>
            <ThemeProvider>
              <SelectedAssetProvider>
                <ChatProvider>
                  {children}
                  <ChatPanel />
                </ChatProvider>
              </SelectedAssetProvider>
            </ThemeProvider>
          </TimeTravelProvider>
        </QueryProvider>
      </body>
    </html>
  );
}
