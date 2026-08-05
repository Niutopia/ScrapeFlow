import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "export",
  turbopack: {
    root: process.env.SCRAPEFLOW_TURBOPACK_ROOT || process.cwd(),
  },
};

export default nextConfig;
