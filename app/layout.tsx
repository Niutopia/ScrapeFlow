import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "ScrapeFlow - 目标货架启动门",
  description: "先登记待刮削来源，选择电影、番剧或美剧后再整理、回读与清理。",
  icons: { icon: [{ url: "/scrapeflow-mark.svg", type: "image/svg+xml", sizes: "any" }] },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="zh-CN"><body>{children}</body></html>;
}
