import type { Config } from "tailwindcss";
import typography from "@tailwindcss/typography";

const config: Config = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ["var(--font-geist-sans)", "system-ui", "sans-serif"],
        mono: ["var(--font-geist-mono)", "ui-monospace", "monospace"],
        display: ["var(--font-display)", "var(--font-geist-sans)", "system-ui", "sans-serif"],
      },
      colors: {
        ballpark: {
          navy: "#0a0e12",
          panel: "#12181f",
          input: "#0c1118",
          green: "#1a3d32",
          accent: "#2d9f6c",
          "accent-muted": "rgba(45, 159, 108, 0.2)",
          clay: "#c4754a",
          chalk: "#e8e4dc",
        },
        surface: {
          DEFAULT: "#12181f",
          raised: "#161d26",
          border: "rgba(232, 228, 220, 0.1)",
        },
      },
      boxShadow: {
        panel: "0 4px 24px rgba(0, 0, 0, 0.35), 0 0 0 1px rgba(232, 228, 220, 0.06)",
        glow: "0 0 32px rgba(45, 159, 108, 0.12)",
      },
    },
  },
  plugins: [typography],
};

export default config;
