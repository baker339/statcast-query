import type { Metadata } from "next";
import { IBM_Plex_Mono, Inter, Lexend } from "next/font/google";
import "./globals.css";

const inter = Inter({
  subsets: ["latin"],
  variable: "--font-geist-sans",
});

const lexend = Lexend({
  subsets: ["latin"],
  variable: "--font-display",
  weight: ["500", "600"],
});

const ibmMono = IBM_Plex_Mono({
  weight: ["400", "500"],
  subsets: ["latin"],
  variable: "--font-geist-mono",
});

export const metadata: Metadata = {
  title: "Stats Masterson",
  description:
    "Ask baseball questions in plain language — Statcast and FanGraphs data, table-first answers.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${inter.variable} ${ibmMono.variable} ${lexend.variable}`}>
      <body className="min-h-screen font-sans antialiased">{children}</body>
    </html>
  );
}
