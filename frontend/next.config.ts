import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  serverExternalPackages: ["snowflake-sdk"],
  images: {
    unoptimized: true,
  },
};

export default nextConfig;
