// Thin client for the AirSight backend. Runs in the browser, so it targets
// NEXT_PUBLIC_BACKEND_URL (http://localhost:8000 by default).

export const BACKEND_URL =
  process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";

async function getJSON(path) {
  const res = await fetch(`${BACKEND_URL}${path}`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`${path} → ${res.status}`);
  }
  return res.json();
}

export const api = {
  overview: () => getJSON("/api/overview"),
  stations: () => getJSON("/api/stations"),
  station: (name) => getJSON(`/api/stations/${name}`),
  traffic: () => getJSON("/api/traffic"),
  predict: (name) => getJSON(`/api/predict/${name}`),
  forecast: (name) => getJSON(`/api/forecast/${name}`),
  forecastBatch: (horizon = 6) => getJSON(`/api/forecast/batch?horizon=${horizon}`),
};
