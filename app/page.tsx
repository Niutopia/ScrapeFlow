import type { Metadata } from "next";
import { ScrapeFlowApp } from "./scrapeflow-app";

export const metadata: Metadata = {
  title: "ScrapeFlow — 自动任务列表",
  description: "提交来源路径后自动识别、整理、补源、核对和清理。",
};

export default function Home() {
  return <ScrapeFlowApp />;
}
