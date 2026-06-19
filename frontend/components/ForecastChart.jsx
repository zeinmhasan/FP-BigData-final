"use client";

import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
} from "recharts";
import { aqiColor } from "../lib/aqi";

function fmtHour(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
}

export default function ForecastChart({ data = [] }) {
  const series = data.map((d) => ({
    t: d.horizon_h === 0 ? "now" : `+${d.horizon_h}h`,
    time: fmtHour(d.valid_time),
    AQI: d.aqi_predicted,
    horizon: d.horizon_h,
  }));

  if (series.length === 0) {
    return <div className="loading">No forecast available.</div>;
  }

  return (
    <ResponsiveContainer width="100%" height={300}>
      <AreaChart data={series} margin={{ top: 8, right: 16, left: -8, bottom: 0 }}>
        <defs>
          <linearGradient id="aqiFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#3aa0ff" stopOpacity={0.55} />
            <stop offset="100%" stopColor="#3aa0ff" stopOpacity={0.05} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#2c3744" />
        <XAxis dataKey="t" tick={{ fill: "#8b98a5", fontSize: 11 }} minTickGap={20} />
        <YAxis tick={{ fill: "#8b98a5", fontSize: 11 }} />
        {/* US AQI category thresholds */}
        <ReferenceLine y={50} stroke="#00e400" strokeDasharray="2 4" strokeOpacity={0.5} />
        <ReferenceLine y={100} stroke="#f4d03f" strokeDasharray="2 4" strokeOpacity={0.5} />
        <ReferenceLine y={150} stroke="#ff7e00" strokeDasharray="2 4" strokeOpacity={0.5} />
        <ReferenceLine y={200} stroke="#ff0000" strokeDasharray="2 4" strokeOpacity={0.5} />
        <Tooltip
          contentStyle={{
            background: "#1a2129",
            border: "1px solid #2c3744",
            borderRadius: 8,
            color: "#e6edf3",
          }}
          formatter={(v) => [Math.round(v), "AQI"]}
          labelFormatter={(label, p) =>
            p && p[0] ? `${label} · ${p[0].payload.time}` : label
          }
        />
        <Area
          type="monotone"
          dataKey="AQI"
          stroke="#3aa0ff"
          strokeWidth={2}
          fill="url(#aqiFill)"
          dot={false}
          activeDot={{ r: 4 }}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
