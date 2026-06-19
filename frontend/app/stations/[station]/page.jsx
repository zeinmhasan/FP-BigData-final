"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { api } from "../../../lib/api";
import { aqiLevel, currentAqi, prettyName, round, uvLevel, windDir } from "../../../lib/aqi";
import StationChart from "../../../components/StationChart";
import ForecastChart from "../../../components/ForecastChart";

function Pollutant({ k, v, unit }) {
  return (
    <div className="pill">
      <div className="k">{k}</div>
      <div className="v">
        {round(v, 0)}
        {unit ? <span className="k"> {unit}</span> : null}
      </div>
    </div>
  );
}

export default function StationDetailPage() {
  const params = useParams();
  const station = params.station;

  const [detail, setDetail] = useState(null);
  const [forecast, setForecast] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!station) return;
    async function load() {
      try {
        const d = await api.station(station);
        setDetail(d);
        try {
          setForecast(await api.forecast(station));
        } catch {
          setForecast(null); // forecast is best-effort
        }
      } catch (e) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [station]);

  if (loading) return <div className="loading">Loading {prettyName(station)}…</div>;
  if (error)
    return (
      <div className="error">
        {error}
        <br />
        <Link className="back-link" href="/">
          ← Back to overview
        </Link>
      </div>
    );

  const latest = detail.latest || {};
  const curAqi = currentAqi(latest);
  const level = aqiLevel(curAqi);
  const lp = (k) => latest[`latest_${k}`] ?? latest[`avg_${k}`];
  const peak = forecast?.peak;
  const peakLevel = peak ? aqiLevel(peak.aqi_predicted) : null;
  const peakWhen = peak
    ? peak.horizon_h === 0
      ? "now"
      : `in ${peak.horizon_h}h`
    : null;

  return (
    <>
      <div style={{ margin: "20px 0 6px" }}>
        <Link className="back-link" href="/">
          ← Back to overview
        </Link>
      </div>
      <h1 style={{ margin: "4px 0 2px" }}>{prettyName(station)}</h1>
      <div className="subtle">
        Latest aggregate · {latest.reading_count || 0} readings · updated{" "}
        {latest.last_updated || "–"}
      </div>

      <div className="cards" style={{ marginTop: 18 }}>
        <div className="card">
          <div className="label">Current AQI</div>
          <div className="value" style={{ color: level.color }}>
            {round(curAqi, 0)}
          </div>
          <div className="meta">
            <span className="badge" style={{ background: level.color, color: level.text }}>
              {level.label}
            </span>
          </div>
        </div>
        <div className="card">
          <div className="label">Range (min – max)</div>
          <div className="value">
            {round(latest.min_aqi, 0)} – {round(latest.max_aqi, 0)}
          </div>
          <div className="meta">across the window</div>
        </div>
        <div className="card predict-card">
          <div className="label">Forecast Peak (next 24h)</div>
          {peak ? (
            <>
              <div className="value" style={{ color: peakLevel.color }}>
                {round(peak.aqi_predicted, 0)}
              </div>
              <div className="meta">
                <span
                  className="badge"
                  style={{ background: peakLevel.color, color: peakLevel.text }}
                >
                  {peak.category}
                </span>{" "}
                <span className="subtle">expected {peakWhen}</span>
              </div>
            </>
          ) : (
            <div className="meta">Forecast unavailable</div>
          )}
        </div>
      </div>

      <h2 className="section-title">AQI Forecast · next 24 hours</h2>
      <div className="chart-wrap">
        {forecast ? (
          <ForecastChart data={forecast.forecast} />
        ) : (
          <div className="subtle">Forecast unavailable.</div>
        )}
      </div>

      <h2 className="section-title">Weather &amp; UV</h2>
      {latest.temp_c != null || latest.uv_index != null ? (
        <div className="pollutant-grid">
          <div className="pill">
            <div className="k">Temperature</div>
            <div className="v">
              {round(latest.temp_c, 1)}
              <span className="k"> °C</span>
            </div>
          </div>
          <div className="pill">
            <div className="k">Feels Like</div>
            <div className="v">
              {round(latest.feels_like_c, 1)}
              <span className="k"> °C</span>
            </div>
          </div>
          <div className="pill">
            <div className="k">Humidity</div>
            <div className="v">
              {round(latest.humidity_pct, 0)}
              <span className="k"> %</span>
            </div>
          </div>
          <div className="pill">
            <div className="k">Wind</div>
            <div className="v">
              {round(latest.wind_speed_ms, 1)}
              <span className="k"> m/s {windDir(latest.wind_deg)}</span>
            </div>
          </div>
          <div className="pill">
            <div className="k">UV Index</div>
            <div className="v" style={{ color: uvLevel(latest.uv_index).color }}>
              {round(latest.uv_index, 1)}
              <span className="k"> {uvLevel(latest.uv_index).label}</span>
            </div>
          </div>
          <div className="pill">
            <div className="k">Conditions</div>
            <div className="v" style={{ fontSize: 15, textTransform: "capitalize" }}>
              {latest.weather_desc || latest.weather_main || "–"}
            </div>
          </div>
        </div>
      ) : (
        <div className="subtle">No weather data yet for this station.</div>
      )}

      <h2 className="section-title">Pollutant Breakdown</h2>
      <div className="pollutant-grid">
        <Pollutant k="PM2.5" v={lp("pm25")} />
        <Pollutant k="PM10" v={lp("pm10")} />
        <Pollutant k="NO₂" v={lp("no2")} />
        <Pollutant k="O₃" v={lp("o3")} />
        <Pollutant k="SO₂" v={lp("so2")} />
        <Pollutant k="CO" v={lp("co")} />
      </div>

      <h2 className="section-title">AQI &amp; PM2.5 Trend</h2>
      <div className="chart-wrap">
        <StationChart data={detail.timeseries || []} />
      </div>
    </>
  );
}
