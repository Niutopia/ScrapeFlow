import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const sans = Geist({ variable: "--font-sans", subsets: ["latin"] });
const mono = Geist_Mono({ variable: "--font-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: "ScrapeFlow — 本地媒体操作台",
  description: "在本机安全完成 AList 解压、TMDB 识别、媒体整理与故障恢复。",
  icons: { icon: [{ url: "/scrapeflow-mark.svg", type: "image/svg+xml", sizes: "any" }] },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="zh-CN"><body className={`${sans.variable} ${mono.variable}`}>{children}</body></html>;
}
