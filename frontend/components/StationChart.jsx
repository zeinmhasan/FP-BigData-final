"use client";

import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";

function fmtTime(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  return d.toLocaleString("en-GB", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export default function StationChart({ data = [] }) {
  const series = data.map((d) => ({
    t: fmtTime(d.timestamp),
    AQI: d.aqi,
    "PM2.5": d.pm25,
  }));

  if (series.length === 0) {
    return <div className="loading">No timeseries data yet for this station.</div>;
  }

  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={series} margin={{ top: 8, right: 16, left: -8, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#2c3744" />
        <XAxis dataKey="t" tick={{ fill: "#8b98a5", fontSize: 11 }} minTickGap={28} />
        <YAxis tick={{ fill: "#8b98a5", fontSize: 11 }} />
        <Tooltip
          contentStyle={{
            background: "#1a2129",
            border: "1px solid #2c3744",
            borderRadius: 8,
            color: "#e6edf3",
          }}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        <Line type="monotone" dataKey="AQI" stroke="#3aa0ff" strokeWidth={2} dot={false} />
        <Line type="monotone" dataKey="PM2.5" stroke="#f4d03f" strokeWidth={2} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}
