"use client";

import { useEffect, useState, useCallback } from "react";
import dynamic from "next/dynamic";
import Link from "next/link";
import { api } from "../lib/api";
import {
  AQI_LEVELS,
  aqiLevel,
  congestionColor,
  currentAqi,
  currentPm25,
  prettyName,
  round,
  uvLevel,
} from "../lib/aqi";

// Leaflet must only render in the browser
const MapView = dynamic(() => import("../components/MapView"), {
  ssr: false,
  loading: () => <div className="loading">Loading map…</div>,
});

const REFRESH_MS = 60000;

function Badge({ aqi }) {
  const level = aqiLevel(aqi);
  return (
    <span className="badge" style={{ background: level.color, color: level.text }}>
      {level.label}
    </span>
  );
}

function AqiLegend() {
  return (
    <div className="legend">
      {AQI_LEVELS.map((l) => (
        <span key={l.label} className="legend-item">
          <span className="legend-dot" style={{ background: l.color }} />
          {l.label === "Unhealthy for Sensitive Groups" ? "USG" : l.label}
        </span>
      ))}
    </div>
  );
}

function timeAgo(iso) {
  if (!iso) return "–";
  const secs = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  return `${Math.floor(secs / 3600)}h ago`;
}

export default function OverviewPage() {
  const [overview, setOverview] = useState(null);
  const [stations, setStations] = useState([]);
  const [traffic, setTraffic] = useState([]);
  const [forecasts, setForecasts] = useState({}); // station -> {forecast_aqi, delta}
  const FORECAST_H = 6;
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async () => {
    setRefreshing(true);
    try {
      const [ov, st, tr] = await Promise.all([
        api.overview(),
        api.stations(),
        api.traffic(),
      ]);
      setOverview(ov);
      setStations(st.stations || []);
      setTraffic(tr.corridors || []);
      setError(null);
      // ML forecasts are best-effort — don't block the dashboard on them
      try {
        const batch = await api.forecastBatch(FORECAST_H);
        const map = {};
        (batch.predictions || []).forEach((p) => {
          if (p.station) map[p.station] = p;
        });
        setForecasts(map);
      } catch {
        /* ignore */
      }
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, REFRESH_MS);
    return () => clearInterval(id);
  }, [load]);

  if (loading) return <div className="loading">Loading dashboard…</div>;
  if (error)
    return (
      <div className="error">
        Failed to reach backend: {error}
        <br />
        <span className="subtle">Is the backend running on the configured URL?</span>
      </div>
    );

  return (
    <>
      <div className="overview-head">
        <h2 className="section-title" style={{ margin: 0 }}>
          City Overview
        </h2>
        <button className="refresh-btn" onClick={load} disabled={refreshing}>
          {refreshing ? "Refreshing…" : "↻ Refresh"}
        </button>
      </div>

      <div className="cards">
        <div className="card">
          <div className="label">City Average AQI</div>
          <div className="value">{round(overview.city_avg_aqi, 0)}</div>
          <div className="meta">
            <Badge aqi={overview.city_avg_aqi} />
          </div>
        </div>
        <div className="card">
          <div className="label">Worst Station</div>
          <div className="value">{prettyName(overview.worst_station?.station)}</div>
          <div className="meta">AQI {round(overview.worst_station?.avg_aqi, 0)}</div>
        </div>
        <div className="card">
          <div className="label">Best Station</div>
          <div className="value">{prettyName(overview.best_station?.station)}</div>
          <div className="meta">AQI {round(overview.best_station?.avg_aqi, 0)}</div>
        </div>
        <div className="card">
          <div className="label">Stations / Corridors</div>
          <div className="value">
            {overview.station_count} / {overview.corridor_count}
          </div>
          <div className="meta">{overview.total_aqi_readings} AQI readings</div>
        </div>
      </div>

      {overview.weather && (
        <>
          <h2 className="section-title">Weather &amp; UV</h2>
          <div className="cards">
            <div className="card">
              <div className="label">Avg Temperature</div>
              <div className="value">
                {round(overview.weather.avg_temp_c, 1)}
                <span className="label" style={{ display: "inline" }}> °C</span>
              </div>
            </div>
            <div className="card">
              <div className="label">Avg Humidity</div>
              <div className="value">
                {round(overview.weather.avg_humidity_pct, 0)}
                <span className="label" style={{ display: "inline" }}> %</span>
              </div>
            </div>
            <div className="card">
              <div className="label">Avg Wind</div>
              <div className="value">
                {round(overview.weather.avg_wind_speed_ms, 1)}
                <span className="label" style={{ display: "inline" }}> m/s</span>
              </div>
            </div>
            <div className="card">
              <div className="label">Peak UV Index</div>
              <div className="value" style={{ color: uvLevel(overview.weather.max_uv_index).color }}>
                {round(overview.weather.max_uv_index, 1)}
              </div>
              <div className="meta">{uvLevel(overview.weather.max_uv_index).label}</div>
            </div>
          </div>
        </>
      )}

      <h2 className="section-title">Live Map</h2>
      <div className="map-wrap">
        <MapView stations={stations} traffic={traffic} />
      </div>
      <div className="map-foot">
        <AqiLegend />
        <span className="subtle">
          Large dots = AQI stations · small dots = traffic corridors · pipeline last run:{" "}
          {timeAgo(overview.pipeline_last_run)}
        </span>
      </div>

      <div className="grid-2" style={{ marginTop: 28 }}>
        <div>
          <h2 className="section-title">Stations</h2>
          <div className="panel">
            <table>
              <thead>
                <tr>
                  <th>Station</th>
                  <th>AQI</th>
                  <th>PM2.5</th>
                  <th>+{FORECAST_H}h</th>
                  <th>Category</th>
                </tr>
              </thead>
              <tbody>
                {stations.map((s) => {
                  const aqi = currentAqi(s);
                  const fc = forecasts[s.station];
                  const fAqi = fc?.forecast_aqi;
                  const delta = fc?.delta;
                  const arrow = delta == null ? "" : delta > 1 ? " ▲" : delta < -1 ? " ▼" : " ▬";
                  return (
                    <tr
                      key={s.station}
                      className="row-link"
                      onClick={() => (window.location.href = `/stations/${s.station}`)}
                    >
                      <td>
                        <Link href={`/stations/${s.station}`}>{prettyName(s.station)}</Link>
                      </td>
                      <td>{round(aqi, 0)}</td>
                      <td>{round(currentPm25(s), 0)}</td>
                      <td style={{ color: fAqi != null ? aqiLevel(fAqi).color : "var(--muted)" }}>
                        {fAqi != null ? round(fAqi, 0) : "–"}
                        <span style={{ fontSize: 11 }}>{arrow}</span>
                      </td>
                      <td>
                        <Badge aqi={aqi} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        <div>
          <h2 className="section-title">Traffic Corridors</h2>
          <div className="panel">
            <table>
              <thead>
                <tr>
                  <th>Corridor</th>
                  <th>Speed</th>
                  <th>Congestion</th>
                </tr>
              </thead>
              <tbody>
                {traffic.map((t) => (
                  <tr key={t.corridor}>
                    <td>{prettyName(t.corridor)}</td>
                    <td>{round(t.avg_speed_kmh, 0)} km/h</td>
                    <td>
                      <span
                        className="badge"
                        style={{ background: congestionColor(t.congestion_level), color: "#0b0e12" }}
                      >
                        {t.congestion_level}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="subtle" style={{ marginTop: 20 }}>
        Auto-refreshes every {REFRESH_MS / 1000}s · cache last refresh:{" "}
        {timeAgo(overview.last_refresh)}
      </div>
    </>
  );
}
