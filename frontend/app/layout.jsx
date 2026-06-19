import "leaflet/dist/leaflet.css";
import "./globals.css";
import Link from "next/link";

export const metadata = {
  title: "AirSight Jabodetabek",
  description: "Real-time air quality & traffic monitoring for Greater Jakarta",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <header className="topbar">
          <div className="topbar-inner">
            <Link href="/" className="brand">
              Air<span className="dot">Sight</span>
            </Link>
            <span className="subtle">Jabodetabek · Air Quality &amp; Traffic</span>
          </div>
        </header>
        <main className="container">{children}</main>
      </body>
    </html>
  );
}
