import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "geo-line-ranker example",
  description: "Minimal Phase 1 frontend for deterministic geo/line ranking."
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

