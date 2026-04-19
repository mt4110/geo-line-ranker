import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "geo-line-ranker example",
  description: "Phase 6 example frontend for deterministic geo/line ranking."
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
