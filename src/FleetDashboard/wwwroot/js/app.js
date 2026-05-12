const BASE = {
  lat: 52.2297,
  lon: 21.0122
};

const GRID_RANGE_DEGREES = 0.06;
const mapEl = document.getElementById("map");
const roverLayers = new Map();
const gridLabels = [];

const els = {
  connection: document.getElementById("connection-state"),
  mapStatus: document.getElementById("map-status"),
  total: document.getElementById("total-count"),
  healthy: document.getElementById("healthy-count"),
  warning: document.getElementById("warning-count"),
  stale: document.getElementById("stale-count"),
  dead: document.getElementById("dead-count"),
  lastUpdate: document.getElementById("last-update"),
  roverList: document.getElementById("rover-list"),
  alertsList: document.getElementById("alerts-list")
};

function setupGrid() {
  mapEl.innerHTML = `
    <div class="grid-axis lat-axis"></div>
    <div class="grid-axis lon-axis"></div>
    <div class="base-marker" title="Base station">
      <span></span>
      <strong>BASE</strong>
    </div>
    <div class="grid-label base-label">52.2297, 21.0122</div>
  `;

  const ticks = [-0.06, -0.04, -0.02, 0.02, 0.04, 0.06];
  for (const tick of ticks) {
    const y = percentFromLat(BASE.lat + tick);
    const x = percentFromLon(BASE.lon + tick);

    const latLabel = document.createElement("div");
    latLabel.className = "grid-label lat-label";
    latLabel.style.top = `${y}%`;
    latLabel.textContent = `${(BASE.lat + tick).toFixed(4)} lat`;
    mapEl.appendChild(latLabel);
    gridLabels.push(latLabel);

    const lonLabel = document.createElement("div");
    lonLabel.className = "grid-label lon-label";
    lonLabel.style.left = `${x}%`;
    lonLabel.textContent = `${(BASE.lon + tick).toFixed(4)} lon`;
    mapEl.appendChild(lonLabel);
    gridLabels.push(lonLabel);
  }
}

function statusClass(status) {
  return (status || "stale").toLowerCase();
}

function aqiClass(aqi) {
  if (aqi <= 50) return "good";
  if (aqi <= 100) return "moderate";
  if (aqi <= 150) return "unhealthy";
  return "hazard";
}

function percentFromLon(lon) {
  const offset = lon - BASE.lon;
  return Math.max(3, Math.min(97, 50 + (offset / GRID_RANGE_DEGREES) * 50));
}

function percentFromLat(lat) {
  const offset = lat - BASE.lat;
  return Math.max(3, Math.min(97, 50 - (offset / GRID_RANGE_DEGREES) * 50));
}

function formatTime(value) {
  if (!value) return "--";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(value));
}

function formatAge(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const seconds = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  return `${Math.round(seconds / 60)}m ago`;
}

function setConnection(ok, text) {
  els.connection.textContent = text;
  els.connection.className = `connection-state ${ok ? "ok" : "error"}`;
}

function renderMap(roverStates) {
  const activeIds = new Set(roverStates.map(rover => rover.roverId));

  for (const [roverId, layer] of roverLayers) {
    if (!activeIds.has(roverId)) {
      layer.remove();
      roverLayers.delete(roverId);
    }
  }

  for (const rover of roverStates) {
    const x = percentFromLon(rover.lon);
    const y = percentFromLat(rover.lat);
    const status = statusClass(rover.status);
    const aqi = aqiClass(rover.airQualityIndex);
    const heading = Number(rover.headingDegrees || 0);

    let layer = roverLayers.get(rover.roverId);
    if (!layer) {
      layer = document.createElement("button");
      layer.type = "button";
      layer.className = "rover-map-item";
      mapEl.appendChild(layer);
      roverLayers.set(rover.roverId, layer);
    }

    layer.className = `rover-map-item ${status} ${aqi}`;
    layer.style.left = `${x}%`;
    layer.style.top = `${y}%`;
    layer.innerHTML = `
      <span class="aqi-halo"></span>
      <span class="rover-arrow" style="transform: rotate(${heading}deg)"></span>
      <span class="rover-dot"></span>
      <span class="rover-label">${rover.roverId}</span>
    `;
    layer.title = `${rover.roverId}: ${rover.status}, AQI ${rover.airQualityIndex}, battery ${rover.batteryPercent.toFixed(1)}%, seen ${formatAge(rover.lastSeenUtc)}`;
  }
}

function renderStats(roverStates) {
  const counts = roverStates.reduce((acc, rover) => {
    acc[statusClass(rover.status)] = (acc[statusClass(rover.status)] || 0) + 1;
    return acc;
  }, {});

  els.total.textContent = roverStates.length;
  els.healthy.textContent = counts.healthy || 0;
  els.warning.textContent = counts.warning || 0;
  els.stale.textContent = counts.stale || 0;
  els.dead.textContent = counts.dead || 0;
  els.lastUpdate.textContent = formatTime(new Date());
}

function renderRovers(roverStates) {
  if (roverStates.length === 0) {
    els.roverList.innerHTML = '<p class="empty-state">No rover state in Redis yet.</p>';
    return;
  }

  els.roverList.innerHTML = roverStates.map(rover => {
    const cls = statusClass(rover.status);
    return `
      <article class="rover-item ${cls}">
        <div class="item-row">
          <span class="item-title">${rover.roverId}</span>
          <span class="badge ${cls}">${rover.status}</span>
        </div>
        <div class="item-meta">
          <span>AQI ${rover.airQualityIndex}</span>
          <span>Battery ${rover.batteryPercent.toFixed(1)}%</span>
          <span>Heading ${Math.round(rover.headingDegrees)} deg</span>
          <span>Seen ${formatAge(rover.lastSeenUtc)}</span>
        </div>
      </article>
    `;
  }).join("");
}

function renderAlerts(alerts) {
  if (alerts.length === 0) {
    els.alertsList.innerHTML = '<p class="empty-state">No alerts yet.</p>';
    return;
  }

  els.alertsList.innerHTML = alerts.map(alert => {
    const cls = (alert.alertType || "").toLowerCase();
    return `
      <article class="alert-item ${cls}">
        <div class="item-row">
          <span class="alert-type">${alert.alertType}</span>
          <span class="badge">${alert.roverId}</span>
        </div>
        <div class="item-meta">
          <span>AQI ${alert.airQualityIndex}</span>
          <span>Battery ${alert.batteryPercent.toFixed(1)}%</span>
          <span>${formatTime(alert.eventTime)}</span>
          <span>${formatAge(alert.eventTime)}</span>
        </div>
      </article>
    `;
  }).join("");
}

async function getJson(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error(`${url} returned ${response.status}`);
  }

  return response.json();
}

async function refresh() {
  try {
    const [roverStates, alerts] = await Promise.all([
      getJson("/api/fleet/state"),
      getJson("/api/alerts/latest?take=20")
    ]);

    renderMap(roverStates);
    renderStats(roverStates);
    renderRovers(roverStates);
    renderAlerts(alerts);

    els.mapStatus.textContent = roverStates.length === 0
      ? "Waiting for fleet state in Redis..."
      : `${roverStates.length} rovers on base grid, refreshed ${formatTime(new Date())}`;
    setConnection(true, "Live");
  } catch (error) {
    console.error(error);
    els.mapStatus.textContent = "Dashboard API is unavailable.";
    setConnection(false, "Offline");
  }
}

setupGrid();
refresh();
setInterval(refresh, 2000);
