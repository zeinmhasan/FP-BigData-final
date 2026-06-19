"use client";

import { MapContainer, TileLayer, CircleMarker, Popup } from "react-leaflet";
import Link from "next/link";
import {
  aqiColor,
  aqiLevel,
  congestionColor,
  currentAqi,
  currentPm25,
  prettyName,
  round,
} from "../lib/aqi";

const JAKARTA_CENTER = [-6.25, 106.83];

export default function MapView({ stations = [], traffic = [] }) {
  return (
    <MapContainer
      center={JAKARTA_CENTER}
      zoom={10}
      scrollWheelZoom={true}
      style={{ height: "100%", width: "100%" }}
    >
      <TileLayer
        attribution='&copy; OpenStreetMap contributors'
        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
      />

      {stations.map((s) => {
        if (s.lat == null || s.lon == null) return null;
        const aqi = currentAqi(s);
        const level = aqiLevel(aqi);
        return (
          <CircleMarker
            key={`s-${s.station}`}
            center={[s.lat, s.lon]}
            radius={13}
            pathOptions={{
              color: "#0b0e12",
              weight: 1.5,
              fillColor: aqiColor(aqi),
              fillOpacity: 0.9,
            }}
          >
            <Popup>
              <strong>{prettyName(s.station)}</strong>
              <br />
              AQI: {round(aqi, 0)} — {level.label}
              <br />
              PM2.5: {round(currentPm25(s), 0)}
              <br />
              <Link href={`/stations/${s.station}`}>View details →</Link>
            </Popup>
          </CircleMarker>
        );
      })}

      {traffic.map((t) => {
        if (t.lat == null || t.lon == null) return null;
        return (
          <CircleMarker
            key={`t-${t.corridor}`}
            center={[t.lat, t.lon]}
            radius={7}
            pathOptions={{
              color: "#0b0e12",
              weight: 1,
              fillColor: congestionColor(t.congestion_level),
              fillOpacity: 0.85,
            }}
          >
            <Popup>
              <strong>{prettyName(t.corridor)}</strong>
              <br />
              {t.congestion_level}
              <br />
              Speed: {round(t.avg_speed_kmh, 0)} km/h
            </Popup>
          </CircleMarker>
        );
      })}
    </MapContainer>
  );
}
