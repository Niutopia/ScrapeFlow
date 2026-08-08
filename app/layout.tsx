import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "ScrapeFlow — 全自动媒体整理",
  description: "自动识别、刮削、补源、AList 核对与清理本地媒体。",
  icons: { icon: [{ url: "/scrapeflow-mark.svg", type: "image/svg+xml", sizes: "any" }] },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="zh-CN"><body>{children}</body></html>;
}
