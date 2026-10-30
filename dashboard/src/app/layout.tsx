import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "PulseEngine | Distributed Notification Infrastructure",
  description:
    "Mission Control Telemetry & Dynamic Segmentation Dashboard for PulseEngine. High-throughput event-driven notification engine.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen bg-[#07090e] text-slate-100 antialiased selection:bg-emerald-500/20 selection:text-emerald-300">
        {children}
      </body>
    </html>
  );
}
