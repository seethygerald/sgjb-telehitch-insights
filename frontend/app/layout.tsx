import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Telehitch Insights",
  description: "Near-real-time Telehitch request map for Singapore and SG-JB rides.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
