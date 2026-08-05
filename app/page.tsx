import type { Metadata } from "next";
import { ScrapeFlowApp } from "./scrapeflow-app";

export const metadata: Metadata = {
  title: "ScrapeFlow — 任务列表",
  description: "创建、查看和处理本地媒体整理任务。",
};

export default function Home() {
  return <ScrapeFlowApp />;
}
