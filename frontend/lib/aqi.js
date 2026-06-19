// US EPA AQI categories, colors, and helpers — shared across the UI.

export const AQI_LEVELS = [
  { max: 50,  label: "Good",                           color: "#00e400", text: "#0b3d0b" },
  { max: 100, label: "Moderate",                        color: "#f4d03f", text: "#4d3b00" },
  { max: 150, label: "Unhealthy for Sensitive Groups",  color: "#ff7e00", text: "#4d2600" },
  { max: 200, label: "Unhealthy",                       color: "#ff0000", text: "#ffffff" },
  { max: 300, label: "Very Unhealthy",                  color: "#8f3f97", text: "#ffffff" },
  { max: Infinity, label: "Hazardous",                  color: "#7e0023", text: "#ffffff" },
];

export function aqiLevel(aqi) {
  if (aqi === null || aqi === undefined || Number.isNaN(aqi)) {
    return { label: "Unknown", color: "#9aa0a6", text: "#ffffff" };
  }
  return AQI_LEVELS.find((l) => aqi <= l.max) || AQI_LEVELS[AQI_LEVELS.length - 1];
}

export function aqiColor(aqi) {
  return aqiLevel(aqi).color;
}

const CONGESTION_COLORS = {
  "Free Flow": "#00e400",
  Light: "#a8d600",
  Moderate: "#f4d03f",
  Heavy: "#ff7e00",
  Severe: "#ff0000",
};

export function congestionColor(level) {
  return CONGESTION_COLORS[level] || "#9aa0a6";
}

// WHO UV index bands
const UV_LEVELS = [
  { max: 2, label: "Low", color: "#3ea72d" },
  { max: 5, label: "Moderate", color: "#f4d03f" },
  { max: 7, label: "High", color: "#f18b00" },
  { max: 10, label: "Very High", color: "#e53210" },
  { max: Infinity, label: "Extreme", color: "#b567a4" },
];

export function uvLevel(uv) {
  if (uv === null || uv === undefined || Number.isNaN(uv)) {
    return { label: "—", color: "#9aa0a6" };
  }
  return UV_LEVELS.find((l) => uv <= l.max) || UV_LEVELS[UV_LEVELS.length - 1];
}

const COMPASS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"];

export function windDir(deg) {
  if (deg === null || deg === undefined) return "";
  return COMPASS[Math.round(deg / 45) % 8];
}

export function prettyName(slug) {
  if (!slug) return "";
  return slug
    .split("-")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

// A station's "current" value = latest reading if present, else the window avg.
export function currentAqi(s) {
  if (!s) return null;
  return s.latest_aqi ?? s.avg_aqi ?? null;
}

export function currentPm25(s) {
  if (!s) return null;
  return s.latest_pm25 ?? s.avg_pm25 ?? null;
}

export function round(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(value)) return "–";
  return Number(value).toFixed(digits);
}
