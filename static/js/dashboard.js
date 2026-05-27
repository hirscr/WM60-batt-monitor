// WhatsMiner Controller Dashboard JavaScript

// Global state
const state = {
  miner: {
    history: { t: [], power: [], hashrate: [] },
    connected: false
  },
  battery: {
    history: { t: [], soc: [], pv: [], load: [], net: [] },
    connected: false
  },
  temp: {
    history: { t: [], fan: [], env: [], wm: [] }
  },
  errors: [],
  debugLogs: [],  // Keep last 200 log messages
  // Cache for stop-reason banner evaluation + cross-card reads (weather)
  lastMinerStatus: null,
  lastAutocontrolStatus: null,
  lastManualPct: null,          // last % the user manually applied (AC-off display source)
  lastBatteryStatus: null,
  // Pending power action state for transitional UI (Layer 8)
  pendingPowerAction: null,    // null | 'shutting_down' | 'powering_up'
  pendingPowerStart: 0,
  // Latest snapshot of /api/miner/op_status — drives pending-power resolution
  lastOpStatus: null,
};

// Debug console functions
function addDebugLog(message, level = 'info') {
  const timestamp = new Date().toLocaleTimeString();
  const logEntry = { timestamp, message, level };

  state.debugLogs.push(logEntry);
  if (state.debugLogs.length > 200) {
    state.debugLogs.shift();  // Keep only last 200
  }

  // Add to UI
  const debugContent = document.getElementById('debugContent');
  const line = document.createElement('div');
  line.className = `debug-line ${level}`;
  line.innerHTML = `<span class="debug-timestamp">${timestamp}</span>${message}`;
  debugContent.appendChild(line);

  // Auto-scroll to bottom
  debugContent.scrollTop = debugContent.scrollHeight;

  // Also log to console for debugging
  console.log(`[${level.toUpperCase()}]`, message);
}

function clearDebugConsole() {
  state.debugLogs = [];
  document.getElementById('debugContent').innerHTML = '';
  addDebugLog('Console cleared', 'info');
}

function copyDebugConsole() {
  // Get all debug log text
  const logs = state.debugLogs.map(log => {
    const timestamp = new Date(log.timestamp).toLocaleTimeString();
    return `[${timestamp}] [${log.level.toUpperCase()}] ${log.message}`;
  }).join('\n');

  // Copy to clipboard
  navigator.clipboard.writeText(logs).then(() => {
    // Show success message briefly
    addDebugLog('Debug logs copied to clipboard!', 'success');
  }).catch(err => {
    console.error('Failed to copy:', err);
    addDebugLog('Failed to copy to clipboard', 'error');
  });
}

function toggleDebugConsole() {
  const console = document.getElementById('debugConsole');
  const btn = document.getElementById('toggleDebugBtn');
  console.classList.toggle('collapsed');
  btn.textContent = console.classList.contains('collapsed') ? 'Expand' : 'Collapse';
}

// Unified Chart State
const chartState = {
  allData: [],  // All loaded data
  currentHours: 72,  // Currently loaded hours (default 3 days)
  maxLoadedHours: 72,  // Maximum hours loaded so far
  lastUpdateTime: null  // Last time data was fetched (manual loads only)
};

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
  console.log('[Dashboard] DOMContentLoaded event fired');
  addDebugLog('Dashboard initializing...', 'info');

  try {
    console.log('[Dashboard] Step 1: Initializing chart...');
    initUnifiedChart();
    console.log('[Dashboard] Step 2: Loading chart data (8760 hours / 365 days)...');

    // Load 1 year of data but display only 3 days (72 hours) by default
    loadChartData(8760).then(() => {
      console.log('[Dashboard] Chart data loaded successfully, now filtering to default 3 days (72h)');
      // After loading all data, filter to 3 days to match the default active button
      updateUnifiedChart(72);
    }).catch(err => {
      console.error('[Dashboard] Chart data load FAILED:', err);
      addDebugLog(`Chart data load failed: ${err.message}`, 'error');
    });

    console.log('[Dashboard] Step 3: Setting up event listeners...');
    setupEventListeners();
    console.log('[Dashboard] Step 4: Starting polling...');
    startPolling();
    addDebugLog('Dashboard initialized', 'success');
  } catch (err) {
    console.error('[Dashboard] Initialization ERROR:', err);
    addDebugLog(`Dashboard init failed: ${err.message}`, 'error');
  }
});

// Initialize unified Plotly chart
function initUnifiedChart() {
  const layout = {
    margin: { l: 60, r: 80, t: 30, b: 50 },
    xaxis: {
      type: 'date',
      title: 'Time',
      tickformat: '%m/%d %H:%M'
    },
    yaxis: {
      title: 'Power (W) / Fan Speed (RPM)',
      rangemode: 'tozero',
      range: [0, 12000]
    },
    yaxis2: {
      title: 'Hash Rate (TH/s) / Temp (°C) / SOC (%)',
      overlaying: 'y',
      side: 'right',
      rangemode: 'tozero',
      range: [0, 100]
    },
    legend: { orientation: 'h', y: -0.2 }
  };

  // Define all 9 traces
  const traces = [
    { x: [], y: [], name: 'Miner Power (W)', mode: 'lines', connectgaps: true, yaxis: 'y', line: { color: '#1772FF', width: 2 } },
    { x: [], y: [], name: 'PV Power In (W)', mode: 'lines', connectgaps: true, yaxis: 'y', line: { color: '#52c41a', width: 2 } },
    { x: [], y: [], name: 'EPS Power (W)', mode: 'lines', connectgaps: true, yaxis: 'y', line: { color: '#faad14', width: 2 } },
    { x: [], y: [], name: 'Net Power (W)', mode: 'lines', connectgaps: true, yaxis: 'y', line: { color: '#722ed1', width: 2 } },
    { x: [], y: [], name: 'Fan Speed (RPM)', mode: 'lines', connectgaps: true, yaxis: 'y', line: { color: '#13c2c2', width: 2 } },
    { x: [], y: [], name: 'Hash Rate (TH/s)', mode: 'lines', connectgaps: true, yaxis: 'y2', line: { color: '#eb2f96', width: 2 } },
    { x: [], y: [], name: 'Environment Temp (°C)', mode: 'lines', connectgaps: true, yaxis: 'y2', line: { color: '#fa8c16', width: 2 } },
    { x: [], y: [], name: 'Miner Temp (°C)', mode: 'lines', connectgaps: true, yaxis: 'y2', line: { color: '#f5222d', width: 2 } },
    { x: [], y: [], name: 'Battery SOC (%)', mode: 'lines', connectgaps: true, yaxis: 'y2', line: { color: '#a0d911', width: 2 } }
  ];

  Plotly.newPlot('unifiedChart', traces, layout, { responsive: true });

  // Setup checkbox listeners
  setupChartToggles();
}

// Load chart data from API
async function loadChartData(hours) {
  console.log(`[loadChartData] CALLED with hours=${hours}`);
  addDebugLog(`[API] Requesting ${hours} hours of chart data...`, 'info');

  const overlay = document.getElementById('chartLoadingOverlay');
  if (overlay) overlay.style.display = 'flex';

  try {
    const url = `/api/chart-data?hours=${hours}`;
    console.log(`[loadChartData] Fetching from: ${url}`);

    const res = await fetch(url, { cache: 'no-store' });
    console.log(`[loadChartData] Fetch response status: ${res.status} ${res.statusText}`);

    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    }

    const response = await res.json();
    console.log('[loadChartData] Response received:', {
      dataLength: response.data?.length || 0,
      hours: response.hours,
      hasError: !!response.error
    });

    if (response.error) {
      console.error('[loadChartData] Backend error:', response.error);
      addDebugLog(`[API] Backend error: ${response.error}`, 'error');
      return;
    }

    addDebugLog(`[API] Fetched ${response.data?.length || 0} data points (${response.hours || 0} hours)`, 'info');

    if (!response.data || response.data.length === 0) {
      addDebugLog('[API] ⚠️  NO DATA RETURNED! Check backend logs.', 'error');
      console.error('[API] Empty response:', response);
      return;
    }

    // Log first 3 data points
    console.log('[API] First 3 rows:', response.data.slice(0, 3));
    addDebugLog(`[API] First timestamp: ${response.data[0].timestamp}`, 'info');
    addDebugLog(`[API] Sample data: hash_rate=${response.data[0].hash_rate}, miner_power=${response.data[0].miner_power}`, 'info');

    chartState.allData = response.data;
    chartState.currentHours = hours;
    chartState.maxLoadedHours = Math.max(chartState.maxLoadedHours, hours);

    addDebugLog(`[API] ✓ Data loaded successfully!`, 'success');

    updateUnifiedChart(hours);
  } catch (e) {
    console.error('[loadChartData] EXCEPTION:', e);
    console.error('[loadChartData] Stack trace:', e.stack);
    addDebugLog(`[API] Failed to load chart data: ${e.message}`, 'error');
  } finally {
    if (overlay) overlay.style.display = 'none';
  }
}

// Update chart with filtered data
function updateUnifiedChart(hours) {
  console.log(`[updateUnifiedChart] ===== CALLED with hours=${hours} (type: ${typeof hours}) =====`);
  console.log(`[updateUnifiedChart] chartState.allData.length = ${chartState.allData?.length || 0}`);

  if (!chartState.allData || chartState.allData.length === 0) {
    addDebugLog('[Plotly] No data to display', 'error');
    console.error('[updateUnifiedChart] No data available!');
    return;
  }

  // Update the current hours state
  chartState.currentHours = hours;
  console.log(`[updateUnifiedChart] Updated chartState.currentHours to ${hours}`);

  addDebugLog(`[TimeRange] Filtering to most recent ${hours === 'all' ? 'all data' : hours + ' hours'}...`, 'info');

  // Find the MOST RECENT timestamp in the dataset with error checking
  console.log(`[updateUnifiedChart] Sample timestamps:`, chartState.allData.slice(0, 3).map(r => r.timestamp));

  const allTimestamps = chartState.allData.map(row => {
    const d = new Date(row.timestamp);
    if (isNaN(d.getTime())) {
      console.error(`[updateUnifiedChart] Invalid timestamp detected: "${row.timestamp}"`);
    }
    return d;
  });

  // Filter out invalid dates
  const validTimestamps = allTimestamps.filter(d => !isNaN(d.getTime()));
  console.log(`[updateUnifiedChart] Valid timestamps: ${validTimestamps.length} / ${allTimestamps.length}`);

  if (validTimestamps.length === 0) {
    addDebugLog('[Plotly] All timestamps are invalid! Check timestamp format.', 'error');
    console.error('[updateUnifiedChart] No valid timestamps found!');
    return;
  }

  const mostRecentTime = new Date(Math.max(...validTimestamps));

  console.log(`[updateUnifiedChart] Most recent data point: ${mostRecentTime.toISOString()}`);
  console.log(`[updateUnifiedChart] Current time: ${new Date().toISOString()}`);

  // Calculate cutoff time from MOST RECENT data point (not current time)
  const cutoff = hours === 'all' ? null : new Date(mostRecentTime.getTime() - hours * 3600 * 1000);

  if (cutoff) {
    console.log(`[updateUnifiedChart] Cutoff calculation: ${mostRecentTime.getTime()} - (${hours} * 3600 * 1000) = ${cutoff.getTime()}`);
    console.log(`[updateUnifiedChart] Cutoff time: ${cutoff.toISOString()} (${hours}h before most recent)`);
  } else {
    console.log(`[updateUnifiedChart] No cutoff (showing all data)`);
  }

  console.log(`[updateUnifiedChart] Starting filter...`);
  let filteredData = cutoff
    ? chartState.allData.filter(row => new Date(row.timestamp) >= cutoff)
    : chartState.allData;

  console.log(`[updateUnifiedChart] FILTER RESULT: ${filteredData.length} points (from ${chartState.allData.length} total)`);

  // CRITICAL: Sort data chronologically to ensure lines connect properly
  console.log(`[updateUnifiedChart] Sorting data chronologically...`);
  filteredData.sort((a, b) => {
    const timeA = new Date(a.timestamp).getTime();
    const timeB = new Date(b.timestamp).getTime();
    return timeA - timeB;
  });

  // Remove exact duplicate timestamps (keep first occurrence)
  const uniqueData = [];
  const seenTimestamps = new Set();
  let duplicates = 0;

  for (const row of filteredData) {
    const ts = row.timestamp;
    if (!seenTimestamps.has(ts)) {
      uniqueData.push(row);
      seenTimestamps.add(ts);
    } else {
      duplicates++;
    }
  }

  if (duplicates > 0) {
    console.log(`[updateUnifiedChart] Removed ${duplicates} duplicate timestamps`);
  }

  filteredData = uniqueData;

  if (filteredData.length > 0) {
    const first = new Date(filteredData[0].timestamp);
    const last = new Date(filteredData[filteredData.length - 1].timestamp);
    console.log(`[updateUnifiedChart] After sort: first=${first.toISOString()}, last=${last.toISOString()}`);
    console.log(`[updateUnifiedChart] Final clean data: ${filteredData.length} points`);
    addDebugLog(`[TimeRange] Showing: ${first.toLocaleString()} to ${last.toLocaleString()}`, 'info');
  }

  // Downsample by time span to keep render fast on long windows.
  // <=24h: full resolution. >24h: stride-N where N = ceil(hours/24).
  // For 'all', compute hours from data span.
  let effectiveHours;
  if (hours === 'all') {
    if (filteredData.length >= 2) {
      const firstTs = new Date(filteredData[0].timestamp).getTime();
      const lastTs = new Date(filteredData[filteredData.length - 1].timestamp).getTime();
      effectiveHours = (lastTs - firstTs) / 3.6e6;
    } else {
      effectiveHours = 0;
    }
  } else {
    effectiveHours = hours;
  }

  if (effectiveHours > 24 && filteredData.length > 0) {
    const N = Math.max(1, Math.ceil(effectiveHours / 24));
    const lastPoint = filteredData[filteredData.length - 1];
    const beforeLen = filteredData.length;
    filteredData = filteredData.filter((_, i) => i % N === 0);
    // Always include the most recent point
    if (filteredData.length && filteredData[filteredData.length - 1] !== lastPoint) {
      filteredData.push(lastPoint);
    }
    console.log(`[updateUnifiedChart] Decimation: stride=${N}, ${beforeLen} -> ${filteredData.length} points`);
    addDebugLog(`[Plotly] Decimated ${beforeLen}→${filteredData.length} points (stride ${N})`, 'info');
  }

  // Extract arrays for each series
  const timestamps = filteredData.map(row => new Date(row.timestamp));
  const miner_power = filteredData.map(row => row.miner_power);
  const pv_power = filteredData.map(row => row.pv_power);
  const eps_power = filteredData.map(row => row.eps_power);
  const net_power = filteredData.map(row => row.net_power);
  const fan_speed = filteredData.map(row => row.fan_speed);
  const hash_rate = filteredData.map(row => row.hash_rate);
  const env_temp = filteredData.map(row => row.env_temp);
  const miner_temp = filteredData.map(row => row.miner_temp);
  const battery_soc = filteredData.map(row => row.battery_soc);

  // Log trace data
  console.log(`[Plotly] Creating traces from ${timestamps.length} timestamps:`);
  console.log(`[Plotly]   Trace 0 (Miner Power): ${miner_power.filter(v => v !== null).length} non-null points`);
  console.log(`[Plotly]   Trace 1 (PV Power): ${pv_power.filter(v => v !== null).length} non-null points`);
  console.log(`[Plotly]   Trace 2 (EPS Power): ${eps_power.filter(v => v !== null).length} non-null points`);
  console.log(`[Plotly]   Trace 3 (Net Power): ${net_power.filter(v => v !== null).length} non-null points`);
  console.log(`[Plotly]   Trace 4 (Fan Speed): ${fan_speed.filter(v => v !== null).length} non-null points`);
  console.log(`[Plotly]   Trace 5 (Hash Rate): ${hash_rate.filter(v => v !== null).length} non-null points`);
  console.log(`[Plotly]   Trace 6 (Env Temp): ${env_temp.filter(v => v !== null).length} non-null points`);
  console.log(`[Plotly]   Trace 7 (Miner Temp): ${miner_temp.filter(v => v !== null).length} non-null points`);
  console.log(`[Plotly]   Trace 8 (Battery SOC): ${battery_soc.filter(v => v !== null).length} non-null points`);
  console.log(`[Plotly] First 3 timestamps:`, timestamps.slice(0, 3));
  console.log(`[Plotly] All traces use mode='lines' for connected display`);

  // Update all traces - MUST explicitly set mode to maintain line connections
  Plotly.update('unifiedChart', {
    x: [timestamps, timestamps, timestamps, timestamps, timestamps, timestamps, timestamps, timestamps, timestamps],
    y: [miner_power, pv_power, eps_power, net_power, fan_speed, hash_rate, env_temp, miner_temp, battery_soc],
    mode: ['lines', 'lines', 'lines', 'lines', 'lines', 'lines', 'lines', 'lines', 'lines'],
    connectgaps: [true, true, true, true, true, true, true, true, true]
  });

  // Set X-axis range to show FULL selected time window (not just the available data)
  // This makes data gaps visible and shows the true time range
  let xAxisStart, xAxisEnd;

  if (hours === 'all') {
    // For "All", show from earliest to most recent data
    if (filteredData.length > 0) {
      xAxisStart = new Date(filteredData[0].timestamp);
      xAxisEnd = new Date(filteredData[filteredData.length - 1].timestamp);
    }
  } else {
    // For specific time ranges, show the FULL requested window
    // End = most recent data point (not current time, since data may be slightly behind)
    xAxisEnd = mostRecentTime;
    // Start = hours before the most recent data point
    xAxisStart = new Date(mostRecentTime.getTime() - hours * 3600 * 1000);
  }

  if (xAxisStart && xAxisEnd) {
    console.log(`[Plotly] Setting x-axis to FULL ${hours === 'all' ? 'data range' : hours + ' hour window'}`);
    console.log(`[Plotly] X-axis range: ${xAxisStart.toISOString()} to ${xAxisEnd.toISOString()}`);
    console.log(`[Plotly] Data points: ${filteredData.length} (may not fill entire window if gaps exist)`);

    Plotly.relayout('unifiedChart', {
      'xaxis.range': [xAxisStart, xAxisEnd]
    });

    // Calculate how much of the window has data
    if (filteredData.length > 0 && hours !== 'all') {
      const dataStart = new Date(filteredData[0].timestamp);
      const dataEnd = new Date(filteredData[filteredData.length - 1].timestamp);
      const dataSpanHours = (dataEnd - dataStart) / (3600 * 1000);
      const coveragePct = (dataSpanHours / hours * 100).toFixed(1);
      console.log(`[Plotly] Data coverage: ${dataSpanHours.toFixed(1)}h out of ${hours}h (${coveragePct}% of window)`);
      addDebugLog(`[Plotly] Data covers ${coveragePct}% of ${hours}h window`, 'info');
    }

    addDebugLog(`[Plotly] ✓ Chart updated: ${filteredData.length} points in ${hours === 'all' ? 'all data' : hours + 'h window'}`, 'success');
  }
}

// Setup chart toggle checkboxes
function setupChartToggles() {
  const toggles = {
    'toggle_miner_power': 0,
    'toggle_pv_power': 1,
    'toggle_eps_power': 2,
    'toggle_net_power': 3,
    'toggle_fan_speed': 4,
    'toggle_hash_rate': 5,
    'toggle_env_temp': 6,
    'toggle_miner_temp': 7,
    'toggle_battery_soc': 8
  };

  Object.entries(toggles).forEach(([id, traceIndex]) => {
    const checkbox = document.getElementById(id);
    if (checkbox) {
      checkbox.addEventListener('change', (e) => {
        const visible = e.target.checked;
        Plotly.restyle('unifiedChart', { visible }, [traceIndex]);
      });
    }
  });
}

// Set time range
async function setTimeRange(hours) {
  console.log(`[setTimeRange] ===== CALLED with hours=${hours} (type: ${typeof hours}) =====`);
  console.log(`[setTimeRange] chartState.maxLoadedHours = ${chartState.maxLoadedHours}`);
  console.log(`[setTimeRange] chartState.currentHours = ${chartState.currentHours}`);
  console.log(`[setTimeRange] chartState.allData.length = ${chartState.allData?.length || 0}`);

  // Update button active state
  document.querySelectorAll('.time-buttons-row button').forEach(btn => btn.classList.remove('active'));
  event.target.classList.add('active');

  addDebugLog(`[Time Range] Switching to ${hours === 'all' ? 'all data' : hours + ' hours'}`, 'info');

  if (hours === 'all') {
    console.log(`[setTimeRange] Path: ALL`);
    if (chartState.maxLoadedHours < 8760) {
      addDebugLog(`[Time Range] Loading full history before showing all...`, 'info');
      await loadChartData(8760);
    }
    updateUnifiedChart('all');
    return;
  }

  // Check if we need to load more data
  console.log(`[setTimeRange] Comparison: ${hours} > ${chartState.maxLoadedHours}? ${hours > chartState.maxLoadedHours}`);

  if (hours > chartState.maxLoadedHours) {
    console.log(`[setTimeRange] Path: LOAD MORE DATA`);
    addDebugLog(`[Time Range] Need to load more data (have ${chartState.maxLoadedHours}h, need ${hours}h)`, 'info');
    await loadChartData(hours);
  } else {
    console.log(`[setTimeRange] Path: USE CACHED DATA - calling updateUnifiedChart(${hours})`);
    addDebugLog(`[Time Range] Using cached data`, 'info');
    updateUnifiedChart(hours);
  }

  console.log(`[setTimeRange] ===== DONE =====`);
}

// Turn off all series
function allSeriesOff() {
  const checkboxes = [
    'toggle_hash_rate', 'toggle_miner_power', 'toggle_fan_speed',
    'toggle_env_temp', 'toggle_miner_temp', 'toggle_battery_soc',
    'toggle_pv_power', 'toggle_eps_power', 'toggle_net_power'
  ];

  checkboxes.forEach((id, index) => {
    const checkbox = document.getElementById(id);
    if (checkbox) {
      checkbox.checked = false;
      Plotly.restyle('unifiedChart', { visible: false }, [index]);
    }
  });

  addDebugLog('All series turned off', 'info');
}

// Chart auto-refresh has been removed. The chart only re-fetches when the user
// clicks a time-range button. These shims remain as no-ops so any lingering
// callers (e.g. an HTML onclick) don't throw.
function startChartAutoRefresh() { /* no-op: auto-refresh disabled */ }
function stopChartAutoRefresh() { /* no-op: auto-refresh disabled */ }
function toggleAutoRefresh() { /* no-op: auto-refresh disabled */ }

// Update axis ranges
function updateAxisRanges() {
  const leftMax = parseInt(document.getElementById('leftAxisMax').value) || 12000;
  const rightMax = parseInt(document.getElementById('rightAxisMax').value) || 100;

  Plotly.relayout('unifiedChart', {
    'yaxis.range': [0, leftMax],
    'yaxis2.range': [0, rightMax]
  });

  addDebugLog(`Axis ranges updated: Left=${leftMax}, Right=${rightMax}`, 'info');
}

// OLD WORKING CODE: Helper functions from WM_Dashboard.html
function safeDateFromRow(row){
  // Prefer ISO timestamp if present
  if (row && row.ts) {
    const d = new Date(row.ts);
    if (!isNaN(d)) return d;
  }
  // Fall back to separate date + time (e.g., "2025-08-30" + "04:47:24")
  if (row && row.date && row.time) {
    // Interpret as local time
    const d = new Date(`${row.date}T${row.time}`);
    if (!isNaN(d)) return d;
  }
  // Otherwise, skip this row
  return null;
}

function pickHashrate(row){
  // Return TH/s as a Number, or null if not present.
  if (!row || typeof row !== 'object') return null;

  // Common flat forms
  const flatKeys = [
    "Hashrate", "Hash Rate", "hashrate_ths", "TH/s",
    "Hashrate (TH/s)", "Hash_Rate", "hash_rate"
  ];
  for (const k of flatKeys) {
    if (k in row) {
      const v = Number(row[k]);
      if (Number.isFinite(v)) return v;  // assume already TH/s
    }
  }

  // GH/s fields (most cgminer-style summaries)
  for (const k of ["GHS 5s", "GHS av", "GH/s", "GHS", "GHS 1m", "GHS 5m", "GHS 15m"]) {
    if (k in row) {
      try {
        const v = Number(row[k]);  // GH/s
        return (v >= 0) ? (v / 1000.0) : null;  // TH/s
      } catch (e) {}
    }
  }

  // MH/s fields
  for (const k of ["MHS 5s", "MHS av", "MH/s", "MHS", "MHS 1m", "MHS 5m", "MHS 15m"]) {
    if (k in row) {
      try {
        const v = Number(row[k]);  // MH/s
        return (v >= 0) ? (v / 1_000_000.0) : null;  // TH/s
      } catch (e) {}
    }
  }

  return null;
}

function pushMinerHistory(row){
  try {
    const d = safeDateFromRow(row);
    if (!d) return; // skip bad timestamp row

    state.miner.history.t.push(d);
    state.miner.history.power.push(('Power' in row) ? Number(row['Power']) : null);
    state.miner.history.hashrate.push(pickHashrate(row));
    state.temp.history.fan.push(pickFanRPM(row));
    state.temp.history.wm.push(pickWMTemp(row));
    state.temp.history.env.push(pickEnvTemp(row));
  } catch(e) {
    addDebugLog('pushMinerHistory error: ' + e, 'error');
  }
}

// Load miner history
async function loadMinerHistory(days = 3) {
  try {
    addDebugLog(`Loading ${days} days of miner data...`, 'info');
    const res = await fetch(`/api/miner/history?days=${days}`, { cache: 'no-store' });
    const response = await res.json();

    // Handle both old format (array) and new format (object with data/meta)
    const rows = Array.isArray(response) ? response : (response.data || []);
    const meta = response.meta || {};

    addDebugLog(`Miner API returned ${rows.length} rows`, 'info');
    if (rows.length === 0) {
      addDebugLog('Miner API returned NO data!', 'error');
      return;
    }

    // Reset history
    state.miner.history = { t: [], power: [], hashrate: [] };
    state.temp.history = { t: [], fan: [], env: [], wm: [] };

    // Parse data using OLD WORKING pushHistory logic
    for (const row of rows) {
      pushMinerHistory(row); // skips rows with bad/missing ts
    }

    const n = state.miner.history.t.length;
    addDebugLog(`Loaded ${rows.length} rows, ${n} plotted`, 'success');
    console.log(`[Dashboard] Loaded ${rows.length} miner rows, ${n} valid timestamps`);
  } catch (e) {
    addDebugLog('Miner history error: ' + e.message, 'error');
    addError('Miner history error: ' + e.message);
  }
}

function pushBatteryHistory(row){
  try {
    const d = safeDateFromRow(row);
    if (!d) return; // skip bad timestamp row

    state.battery.history.t.push(d);
    state.battery.history.soc.push(('soc_percent' in row) ? Number(row['soc_percent']) : null);
    state.battery.history.pv.push(('pv_power_w' in row) ? Number(row['pv_power_w']) : null);
    state.battery.history.load.push(('load_power_w' in row) ? Number(row['load_power_w']) : null);
    state.battery.history.net.push(('battery_net_w' in row) ? Number(row['battery_net_w']) : null);
  } catch(e) {
    addDebugLog('pushBatteryHistory error: ' + e, 'error');
  }
}

// Load battery history
async function loadBatteryHistory(days = 3) {
  try {
    addDebugLog(`Loading ${days} days of battery data...`, 'info');
    const res = await fetch(`/api/battery/history?days=${days}`, { cache: 'no-store' });
    const response = await res.json();

    // Handle both old format (array) and new format (object with data/meta)
    const rows = Array.isArray(response) ? response : (response.data || []);
    const meta = response.meta || {};

    addDebugLog(`Battery API returned ${rows.length} rows`, 'info');
    if (rows.length === 0) {
      addDebugLog('Battery API returned NO data!', 'error');
      return;
    }

    // Reset history
    state.battery.history = { t: [], soc: [], pv: [], load: [], net: [] };

    // Parse data using OLD WORKING pushHistory logic
    for (const row of rows) {
      pushBatteryHistory(row); // skips rows with bad/missing ts
    }

    const n = state.battery.history.t.length;
    addDebugLog(`Loaded ${rows.length} rows, ${n} plotted`, 'success');
    console.log(`[Dashboard] Loaded ${rows.length} battery rows, ${n} valid timestamps`);
  } catch (e) {
    addDebugLog('Battery history error: ' + e.message, 'error');
    addError('Battery history error: ' + e.message);
  }
}

// Update charts
function updateCharts() {
  // DEBUG: Log what we're sending to charts
  addDebugLog('=== Updating Charts ===', 'info');
  addDebugLog(`Battery timestamps: ${state.battery.history.t.length} points`, 'info');
  if (state.battery.history.t.length > 0) {
    addDebugLog(`  First 3 battery timestamps: ${state.battery.history.t.slice(0, 3).map(t => t.toISOString()).join(', ')}`, 'info');
    addDebugLog(`  First 3 SOC values: ${state.battery.history.soc.slice(0, 3).join(', ')}`, 'info');
    addDebugLog(`  First 3 PV values: ${state.battery.history.pv.slice(0, 3).join(', ')}`, 'info');
  }
  addDebugLog(`Miner timestamps: ${state.miner.history.t.length} points`, 'info');
  if (state.miner.history.t.length > 0) {
    addDebugLog(`  First 3 miner timestamps: ${state.miner.history.t.slice(0, 3).map(t => t.toISOString()).join(', ')}`, 'info');
    addDebugLog(`  First 3 power values: ${state.miner.history.power.slice(0, 3).join(', ')}`, 'info');
  }

  // Power Chart
  const powerVisible = {
    'Battery SOC (%)': document.getElementById('show_batt_soc').checked,
    'PV Power (W)': document.getElementById('show_pv_power').checked,
    'Battery Net (W)': document.getElementById('show_batt_net').checked,
    'Miner Power (W)': document.getElementById('show_miner_power').checked,
    'Hashrate (TH/s)': document.getElementById('show_hashrate').checked
  };

  Plotly.update('powerChart', {
    x: [
      state.battery.history.t,
      state.battery.history.t,
      state.battery.history.t,
      state.miner.history.t,
      state.miner.history.t
    ],
    y: [
      state.battery.history.soc,
      state.battery.history.pv,
      state.battery.history.net,
      state.miner.history.power,
      state.miner.history.hashrate
    ]
  });

  // Apply visibility
  Plotly.restyle('powerChart', 'visible', [
    powerVisible['Battery SOC (%)'],
    powerVisible['PV Power (W)'],
    powerVisible['Battery Net (W)'],
    powerVisible['Miner Power (W)'],
    powerVisible['Hashrate (TH/s)']
  ]);

  // Temperature Chart
  const tempVisible = {
    'Fan Speed (RPM)': document.getElementById('show_fan_speed').checked,
    'Env Temp (°C)': document.getElementById('show_env_temp').checked,
    'WM Temp (°C)': document.getElementById('show_wm_temp').checked
  };

  const tempTimes = state.temp.history.fan.map((_, i) => state.miner.history.t[i]);

  Plotly.update('tempChart', {
    x: [tempTimes, tempTimes, tempTimes],
    y: [state.temp.history.fan, state.temp.history.env, state.temp.history.wm]
  });

  // Apply visibility
  Plotly.restyle('tempChart', 'visible', [
    tempVisible['Fan Speed (RPM)'],
    tempVisible['Env Temp (°C)'],
    tempVisible['WM Temp (°C)']
  ]);
}

// Zoom chart
function zoomChart(chartType, hours) {
  const chartId = chartType === 'power' ? 'powerChart' : 'tempChart';

  if (hours === 'all') {
    Plotly.relayout(chartId, { 'xaxis.autorange': true });
  } else {
    const end = new Date();
    const start = new Date(end.getTime() - hours * 3600 * 1000);
    Plotly.relayout(chartId, { 'xaxis.range': [start, end] });
  }
}

// Update the stop-reason banner in the Miner Control card.
// Called after every miner or autocontrol status refresh.
function updateMinerStopReasonBanner() {
  const banner = document.getElementById('minerStopReason');
  if (!banner) return;

  const minerStatus = state.lastMinerStatus;
  const autoStatus = state.lastAutocontrolStatus;

  if (!minerStatus || !autoStatus) {
    banner.style.display = 'none';
    return;
  }

  // upfreq_complete defaults to 0 (conservative) when missing
  const upfreqComplete = (minerStatus.upfreq_complete != null) ? minerStatus.upfreq_complete : 0;
  const stopReason = autoStatus.stop_reason || 'normal';
  const resumeAtSoc = autoStatus.resume_at_soc;

  // Target power (watts) from autocontrol — used for the hide condition
  const targetPowerW = autoStatus.target_w;
  const power5s = minerStatus['Power 5s'];

  // Hide condition: upfreq is complete AND power is within 10% of target
  const upfreqDone = (upfreqComplete === 1);
  let powerOnTarget = false;
  if (upfreqDone && targetPowerW && targetPowerW > 0 && power5s != null) {
    powerOnTarget = (Math.abs(power5s - targetPowerW) / targetPowerW) <= 0.10;
  }

  if (upfreqDone && powerOnTarget) {
    banner.style.display = 'none';
    return;
  }

  // Show condition: upfreq not complete OR stop_reason != "normal"
  const shouldShow = (upfreqComplete === 0) || (stopReason !== 'normal');
  if (!shouldShow) {
    banner.style.display = 'none';
    return;
  }

  // Determine banner text and color
  let text = '';
  let bg = '';
  let color = '';

  if (stopReason === 'battery_stale') {
    text = 'Miner paused — battery telemetry stale. Waiting for fresh data.';
    bg = '#ffe0cc';
    color = '#7d2b00';
  } else if (stopReason === 'weather_disabled') {
    // Show expected vs deficit numbers if the gate snapshot is available.
    const gate = autoStatus.weather_gate || {};
    const exp = (gate.expected_kwh != null) ? gate.expected_kwh.toFixed(1) : '—';
    const def = (gate.deficit_kwh != null) ? gate.deficit_kwh.toFixed(1) : '—';
    const reason = gate.reason || 'weather_disabled';
    text = `Miner paused — weather gate (${reason}). Expected ${exp} kWh vs deficit ${def} kWh.`;
    bg = '#fff7e6';
    color = '#ad4e00';
  } else if (stopReason === 'emergency_unverified') {
    text = '⚠ EMERGENCY: miner failed to stop below emergency SOC. Retrying.';
    bg = '#f8d7da';
    color = '#721c24';
  } else if (stopReason === 'emergency_soc') {
    const resumeStr = (resumeAtSoc != null) ? resumeAtSoc + '%' : '—';
    text = 'Miner paused — battery below minimum SOC. Resuming at ' + resumeStr + '.';
    bg = '#fff3cd';
    color = '#856404';
  } else if (stopReason === 'manual_off') {
    text = 'Miner off (manual).';
    bg = '#e9ecef';
    color = '#495057';
  } else if (minerStatus.is_off === true && autoStatus.enabled && stopReason === 'normal') {
    text = 'Miner off — startup pending.';
    bg = '#e9ecef';
    color = '#495057';
  } else if (stopReason === 'ramping' || upfreqComplete === 0) {
    text = 'Miner ramping — upfreq not complete. Running at reduced hashrate.';
    bg = '#cce5ff';
    color = '#004085';
  } else {
    text = 'Miner at reduced output — reason unknown.';
    bg = '#fff9db';
    color = '#7d6608';
  }

  banner.textContent = text;
  banner.style.background = bg;
  banner.style.color = color;
  banner.style.display = 'block';
}

// Refresh status
async function refreshStatus() {
  console.log('[RefreshStatus] Starting status refresh...');
  try {
    // Miner status
    console.log('[RefreshStatus] Fetching miner status...');
    const minerRes = await fetch('/api/miner/status');
    const minerData = await minerRes.json();
    console.log('[RefreshStatus] Miner data received:', minerData);

    // Miner op_status — drives pending-power resolution. Fetch before
    // updateMinerStatus so the pending-resolution branch sees the latest op.
    try {
      const opRes = await fetch('/api/miner/op_status', { cache: 'no-store' });
      if (opRes.ok) {
        state.lastOpStatus = await opRes.json();
        console.log('[RefreshStatus] op_status received:', state.lastOpStatus);
      } else {
        state.lastOpStatus = null;
        console.warn('[RefreshStatus] op_status HTTP', opRes.status);
      }
    } catch (opErr) {
      state.lastOpStatus = null;
      console.warn('[RefreshStatus] op_status fetch failed:', opErr.message);
    }

    updateMinerStatus(minerData);

    // Battery status
    console.log('[RefreshStatus] Fetching battery status...');
    const battRes = await fetch('/api/battery/status');
    const battData = await battRes.json();
    console.log('[RefreshStatus] Battery data received:', battData);
    updateBatteryStatus(battData);

    // Auto-control status
    console.log('[RefreshStatus] Fetching auto-control status...');
    const autoRes = await fetch('/api/autocontrol/status');
    const autoData = await autoRes.json();
    console.log('[RefreshStatus] Auto-control data received:', autoData);
    updateAutoControlStatus(autoData);

    // Network devices
    console.log('[RefreshStatus] Fetching network devices...');
    const netRes = await fetch('/api/network/devices');
    const netData = await netRes.json();
    console.log('[RefreshStatus] Network data received:', netData);
    updateNetworkDevices(netData);

    // Braiins Pool status
    console.log('[RefreshStatus] Fetching Braiins Pool status...');
    await refreshBraiinsStatus();

  } catch (e) {
    console.error('[RefreshStatus] ERROR:', e);
    addDebugLog('Status refresh error: ' + e.message, 'error');
  }
}

// Returns the best available manual power percentage for display when AC is off.
// Prefers the in-session commanded value; falls back to Power Limit rounded to
// the nearest whole percent (normalises firmware imprecision, e.g. 1789→50%).
function _manualTargetPct() {
  if (state.lastManualPct != null) return state.lastManualPct;
  const ms = state.lastMinerStatus;
  if (ms && ms['Power Limit'] > 0) return Math.round(ms['Power Limit'] / 3600 * 100);
  return null;
}

// Update miner status
function updateMinerStatus(data) {
  console.log('[UpdateMinerStatus] Received data:', data);
  const { status, connection } = data;
  console.log('[UpdateMinerStatus] Status:', status, 'Connection:', connection);

  // Cache for banner evaluation
  state.lastMinerStatus = status;

  // When AC is off, keep target power display in sync with the commanded value
  const acStatus = state.lastAutocontrolStatus;
  if (acStatus && !acStatus.enabled) {
    const pct = _manualTargetPct();
    const targetPowerDisplay = document.getElementById('minerTargetPower');
    if (pct != null && targetPowerDisplay) {
      targetPowerDisplay.textContent = `${Math.round(3600 * (pct / 100))} W (${pct}%)`;
    }
  }

  addDebugLog(`[Frontend] Received miner update: connected=${connection.connected}`, 'info');

  // Connection status
  state.miner.connected = connection.connected;
  const dot = document.getElementById('minerDot');
  const statusText = document.getElementById('minerStatus');

  if (connection.connected) {
    dot.classList.add('connected');
    dot.classList.remove('disconnected');
    statusText.textContent = `Miner: Connected`;
    addDebugLog(`[Frontend] Miner data: ${status.Hashrate}TH/s, ${status.Power}W`, 'success');
  } else {
    dot.classList.add('disconnected');
    dot.classList.remove('connected');
    statusText.textContent = `Miner: Disconnected`;
    if (connection.error) {
      addError('Miner error: ' + connection.error);
    }
  }

  // Update UI — dual hashrate and power readings
  const hr5s = status['Hashrate 5s'];
  const hr5m = status['Hashrate'];
  const pw5s = status['Power 5s'];
  const pw5m = status['Power'];
  document.getElementById('minerHashrate5s').textContent = (hr5s !== null && hr5s !== undefined ? hr5s.toFixed(1) : '—') + ' TH/s';
  document.getElementById('minerHashrate').textContent = (hr5m !== null && hr5m !== undefined ? Number(hr5m).toFixed(1) : '—') + ' TH/s';
  document.getElementById('minerPower5s').textContent = (pw5s !== null && pw5s !== undefined ? Math.round(pw5s) : '—') + ' W';
  document.getElementById('minerPower').textContent = (pw5m !== null && pw5m !== undefined ? Math.round(pw5m) : '—') + ' W';
  document.getElementById('minerTemp').textContent = (pickWMTemp(status) || '—') + ' °C';

  // Fan: Show "0 RPM" if fan is off, not "— RPM"
  const fanSpeed = pickFanRPM(status);
  if (fanSpeed === null || fanSpeed === undefined) {
    document.getElementById('minerFan').textContent = '0 RPM';
  } else {
    document.getElementById('minerFan').textContent = fanSpeed + ' RPM';
  }

  // Update power toggle from USER INTENT (status.user_power_intent), not the
  // miner's observed on/off state. The miner can be physically off due to a
  // transient safety stop while the user still wants it available — in that
  // case the toggle stays checked (user did not click anything). The
  // stop-reason banner + live Power/Hashrate readings show the actual state.
  // Pending-power-action flow (set by the toggle click handler) freezes the
  // toggle visually until the backend resolves the op.
  const powerToggle = document.getElementById('minerPowerToggle');
  if (!state.pendingPowerAction) {
    powerToggle.checked = status.user_power_intent === true;
    powerToggle.disabled = false;
  }

  // AC toggle is disabled (and visually greyed) when user intent is OFF.
  // This is the frontend half of the constraint — the backend also rejects
  // POST /api/autocontrol/enable with HTTP 400 when intent is false.
  const acToggle = document.getElementById('autoControlToggle');
  const acLabel = acToggle ? acToggle.closest('label') : null;
  const targetPowerDisplay = document.getElementById('minerTargetPower');
  const intentOff = status.user_power_intent === false;
  if (acToggle) {
    acToggle.disabled = intentOff;
    if (acLabel) acLabel.classList.toggle('disabled-by-intent', intentOff);
  }
  if (intentOff && targetPowerDisplay && !state.pendingPowerAction) {
    targetPowerDisplay.textContent = '0 W (0%)';
  }

  // Resolve pending power actions by consuming /api/miner/op_status.
  // Authoritative backend op-state replaces the old MHS/Power heuristics
  // and the frontend retry loop. No retry fetches here — the backend owns
  // the lifecycle; the frontend only renders the outcome.
  if (state.pendingPowerAction) {
    const pending = {
      action: state.pendingPowerAction,
      start: state.pendingPowerStart,
    };
    const result = _resolvePendingPowerOutcome(
      pending,
      state.lastOpStatus,
      Date.now()
    );
    const wasPoweringUp = state.pendingPowerAction === 'powering_up';
    if (result.outcome === 'success') {
      _clearPendingPower(wasPoweringUp);
    } else if (result.outcome === 'failure') {
      const label = wasPoweringUp ? 'Power-up failed' : 'Shutdown failed';
      // On failure the toggle should reflect the actual end state, which is
      // the OPPOSITE of what the user requested (resume failed => still off).
      _clearPendingPower(!wasPoweringUp, `${label}: ${result.error}`);
    } else if (result.outcome === 'timeout') {
      const label = wasPoweringUp ? 'Power-up timed out' : 'Shutdown timed out';
      _clearPendingPower(!wasPoweringUp, label);
    }
    // outcome === 'pending' → leave indicator/toggle as-is
  }

  updateMinerStopReasonBanner();
}

// Update auto-control status
function updateAutoControlStatus(data) {
  console.log('[UpdateAutoControl] Received data:', data);

  // Cache for banner evaluation
  state.lastAutocontrolStatus = data;

  // Emergency SOC
  if (data.emergency_soc !== undefined && data.emergency_soc !== null) {
    document.getElementById('emergencySocInput').value = data.emergency_soc;
    document.getElementById('emergencySocCurrent').textContent = data.emergency_soc;
  }

  // Battery freshness indicator
  const freshnessEl = document.getElementById('batteryFreshnessStatus');
  if (freshnessEl) {
    if (data.battery_fresh) {
      freshnessEl.textContent = 'live';
      freshnessEl.style.color = '#52c41a';
    } else if (data.battery_age_seconds !== null && data.battery_age_seconds !== undefined) {
      freshnessEl.textContent = `STALE (${Math.round(data.battery_age_seconds)}s)`;
      freshnessEl.style.color = '#f5222d';
    } else {
      freshnessEl.textContent = 'no data';
      freshnessEl.style.color = '#faad14';
    }
  }

  const { enabled, target_pct, target_w, mode, current_state_description, sunset_time, is_past_sunset } = data;
  const autoToggle = document.getElementById('autoControlToggle');
  const powerInput = document.getElementById('powerPctInput');
  const applyBtn = document.querySelector('button[onclick="applyPowerPct()"]');
  const targetPowerDisplay = document.getElementById('minerTargetPower');

  // Update mode status display
  const modeStatusBox = document.getElementById('autoModeStatus');
  const modeText = document.getElementById('autoModeText');
  const statusText = document.getElementById('autoStatusText');
  const sunsetText = document.getElementById('autoSunsetText');
  const pastSunsetText = document.getElementById('autoPastSunsetText');

  if (enabled && mode) {
    // Show mode status box when auto-control is enabled
    modeStatusBox.style.display = 'block';
    modeText.textContent = mode.charAt(0).toUpperCase() + mode.slice(1);
    statusText.textContent = current_state_description || 'Active';
    sunsetText.textContent = sunset_time || 'Unknown';
    pastSunsetText.textContent = is_past_sunset ? 'After sunset' : 'Before sunset';
  } else {
    // Hide mode status box when auto-control is disabled
    modeStatusBox.style.display = 'none';
  }

  // Update checkbox state
  autoToggle.checked = enabled;

  // Update target power display
  if (enabled) {
    if (target_w !== null && target_w !== undefined) {
      targetPowerDisplay.textContent = `${target_w} W (${target_pct || 0}%)`;
    } else if (target_pct !== null && target_pct !== undefined) {
      const targetWatts = Math.round(3600 * (target_pct / 100));
      targetPowerDisplay.textContent = `${targetWatts} W (${target_pct}%)`;
    } else {
      targetPowerDisplay.textContent = '— W';
    }
  } else {
    const pct = _manualTargetPct();
    if (pct != null) {
      targetPowerDisplay.textContent = `${Math.round(3600 * (pct / 100))} W (${pct}%)`;
    }
  }

  // Update power input and button state
  if (enabled) {
    // Auto-control ON: Show target %, disable manual input
    powerInput.value = target_pct || '—';
    powerInput.disabled = true;
    if (applyBtn) applyBtn.disabled = true;
    addDebugLog(`[AutoControl] Enabled (${mode}), target power: ${target_pct}%`, 'info');
  } else {
    // Auto-control OFF: Enable manual input
    powerInput.disabled = false;
    if (applyBtn) applyBtn.disabled = false;
    addDebugLog('[AutoControl] Disabled, manual control available', 'info');
  }

  updateMinerStopReasonBanner();
}

// Update battery status
function updateBatteryStatus(data) {
  console.log('[UpdateBatteryStatus] Received data:', data);
  const { status, connection } = data;
  console.log('[UpdateBatteryStatus] Status:', status, 'Connection:', connection);

  // Cache the latest battery status so other cards (e.g. the weather gate)
  // can read SOC without an extra fetch.
  state.lastBatteryStatus = status || null;

  addDebugLog(`[Frontend] Received battery update: connected=${connection.connected}`, 'info');

  // Connection status
  state.battery.connected = connection.connected;
  const dot = document.getElementById('batteryDot');
  const statusText = document.getElementById('batteryStatus');

  if (connection.connected) {
    dot.classList.add('connected');
    dot.classList.remove('disconnected');
    statusText.textContent = `Battery: Connected`;
    addDebugLog(`[Frontend] Battery data: SOC=${status.soc_percent}%, PV=${status.pv_power_w}W, Load=${status.load_power_w}W`, 'success');
  } else {
    dot.classList.add('disconnected');
    dot.classList.remove('connected');
    statusText.textContent = `Battery: Disconnected`;
    if (connection.error) {
      addError('Battery error: ' + connection.error);
    }
  }

  // Update UI
  document.getElementById('batterySOC').textContent = (status.soc_percent != null ? status.soc_percent : '—') + ' %';
  document.getElementById('batteryPV').textContent = (status.pv_power_w != null ? status.pv_power_w : '—') + ' W';
  document.getElementById('batteryLoad').textContent = (status.load_power_w != null ? status.load_power_w : '—') + ' W';
  document.getElementById('batteryNet').textContent = (status.battery_net_w != null ? status.battery_net_w : '—') + ' W';

  // Extended fields (may not always be present)
  const voltEl = document.getElementById('batteryVoltage');
  if (voltEl) voltEl.textContent = (status.pack_voltage_v != null ? status.pack_voltage_v.toFixed(1) : '—') + ' V';
  const currEl = document.getElementById('batteryCurrent');
  if (currEl) currEl.textContent = (status.pack_current_a != null ? status.pack_current_a.toFixed(1) : '—') + ' A';

  // Unit SOC detail (collapsed by default)
  const unitSocEl = document.getElementById('batteryUnitSoc');
  if (unitSocEl && status.unit_soc) {
    unitSocEl.textContent = status.unit_soc;
  }
}

// Fetch and render Braiins Pool status
async function refreshBraiinsStatus() {
  const card = document.getElementById('braiinsCard');
  if (!card) return;

  try {
    const res = await fetch('/api/braiins/status', { cache: 'no-store' });

    if (res.status === 503) {
      // Service disabled — hide the card and let the grid reflow.
      card.style.display = 'none';
      return;
    }

    // Service is enabled — ensure card is visible.
    card.style.display = '';

    if (!res.ok) {
      updateBraiinsStatus(null, `dashboard fetch failed (HTTP ${res.status})`);
      return;
    }

    const data = await res.json();
    updateBraiinsStatus(data, null);

  } catch (e) {
    // Network-level error (e.g., controller itself is unreachable).
    // Keep whatever was last rendered; just update the status line.
    updateBraiinsStatus(null, 'dashboard fetch failed');
    console.warn('[Braiins] Fetch error:', e.message);
  }
}

// Helper: format a value or return an em-dash
function fmtOrDash(val) {
  if (val === null || val === undefined || Number.isNaN(val)) return '—';
  return val;
}

function renderBraiinsStatus(data) {
  // 5m hashrate
  const el5m = document.getElementById('braiinsHashrate5m');
  if (el5m) {
    const v = data.hashrate_5m_ths;
    el5m.textContent = (v != null ? v.toFixed(1) + ' TH/s' : '—');
  }

  // Avg hashrate (1h / 24h stacked)
  const el1h = document.getElementById('braiinsHashrate1h');
  const el24h = document.getElementById('braiinsHashrate24h');
  if (el1h) el1h.textContent = (data.hashrate_1h_ths != null ? '1h: ' + data.hashrate_1h_ths.toFixed(1) + ' TH/s' : '1h: —');
  if (el24h) el24h.textContent = (data.hashrate_24h_ths != null ? '24h: ' + data.hashrate_24h_ths.toFixed(1) + ' TH/s' : '24h: —');

  // Today BTC / USD
  const elTodayBtc = document.getElementById('braiinsTodayBtc');
  const elTodayUsd = document.getElementById('braiinsTodayUsd');
  if (elTodayBtc) {
    elTodayBtc.textContent = (data.today_btc != null ? data.today_btc.toFixed(8) + ' BTC' : '—');
  }
  if (elTodayUsd) {
    elTodayUsd.textContent = (data.today_usd != null ? '≈ $' + data.today_usd.toFixed(2) : '');
  }

  // All-time
  const elAllTime = document.getElementById('braiinsAllTimeBtc');
  if (elAllTime) {
    elAllTime.textContent = (data.all_time_btc != null ? data.all_time_btc.toFixed(8) + ' BTC' : '—');
  }
  const elAllTimeUsd = document.getElementById('braiinsAllTimeUsd');
  if (elAllTimeUsd) {
    elAllTimeUsd.textContent = (data.all_time_usd != null ? '≈ $' + data.all_time_usd.toFixed(2) : '');
  }

  // Balance
  const elBalance = document.getElementById('braiinsBalanceBtc');
  if (elBalance) {
    elBalance.textContent = (data.account_balance_btc != null ? data.account_balance_btc.toFixed(8) + ' BTC' : '—');
  }
  const elBalanceUsd = document.getElementById('braiinsBalanceUsd');
  if (elBalanceUsd) {
    elBalanceUsd.textContent = (data.account_balance_usd != null ? '≈ $' + data.account_balance_usd.toFixed(2) : '');
  }
}

function updateBraiinsStatus(data, dashboardError) {
  const statusEl = document.getElementById('braiinsStatusText');

  if (dashboardError) {
    // Can't reach the backend endpoint.
    if (statusEl) {
      statusEl.className = 'braiins-status-error';
      statusEl.textContent = dashboardError;
    }
    return;
  }

  if (!data) return;

  // Render tile values.
  renderBraiinsStatus(data);

  // Status line.
  if (!statusEl) return;

  if (data.error) {
    statusEl.className = 'braiins-status-error';
    const shortErr = String(data.error).slice(0, 60);
    statusEl.textContent = 'error — ' + shortErr;
    statusEl.title = data.error;  // Full text on hover.
  } else if (data.is_fresh) {
    statusEl.className = 'braiins-status-live';
    statusEl.textContent = 'live';
    statusEl.title = '';
  } else {
    const age = data.age_seconds != null ? Math.round(data.age_seconds) + 's' : 'unknown';
    statusEl.className = 'braiins-status-stale';
    statusEl.textContent = 'STALE (' + age + ')';
    statusEl.title = '';
  }
}

// Update network devices
function updateNetworkDevices(data) {
  const container = document.getElementById('networkDevices');
  const header = document.getElementById('networkDevicesHeader');
  const devices = data.devices || [];
  const scanning = data.scanning || false;

  addDebugLog(`[Frontend] Network update: scanning=${scanning}, ${devices.length} devices found`, 'info');

  // Update header with device count
  if (scanning) {
    header.textContent = 'Network Devices (scanning 192.168.86.1-254)';
  } else {
    header.textContent = `Network Devices (scanning 192.168.86.1-254) - ${devices.length} ${devices.length === 1 ? 'device' : 'devices'}`;
  }

  const dot = document.getElementById('networkDot');
  const statusText = document.getElementById('networkStatus');

  // Show "Scanning..." while scan is in progress
  if (scanning) {
    dot.classList.remove('connected', 'disconnected');
    statusText.textContent = 'Network: Scanning...';
    if (devices.length === 0) {
      container.textContent = 'Scanning network for devices...';
    }
    // If devices already exist from previous scan, keep showing them
    return;
  }

  // Scan complete - show results
  if (devices.length > 0) {
    dot.classList.add('connected');
    dot.classList.remove('disconnected');
    statusText.textContent = `Network: ${devices.length} device(s)`;

    container.innerHTML = devices.map(d => {
      // Format device label - ALWAYS show IP address
      let deviceLabel = d.ip;

      // Add hostname in parentheses if identified
      if (d.hostname && d.hostname !== 'unknown' && d.hostname !== d.ip) {
        deviceLabel += ` (${d.hostname})`;
      }

      // Show mining metrics only for mining devices
      const isMiner = d.type === 'whatsminer' || d.type === 'bitaxe';
      let details = '';
      if (isMiner && d.hashrate_ths) {
        details = ` - ${d.hashrate_ths.toFixed(1)} TH/s, ${d.power_w}W`;
      }

      return `
        <div class="device-item ${d.type}">
          ${deviceLabel}${details}
        </div>
      `;
    }).join('');
    addDebugLog(`[Frontend] Displaying devices: ${devices.map(d => d.ip).join(', ')}`, 'success');
  } else {
    dot.classList.remove('connected');
    dot.classList.add('disconnected');
    statusText.textContent = 'Network: Scan complete - No devices found';
    container.textContent = 'Scan complete - No devices found on network';
    addDebugLog('[Frontend] No network devices found', 'warning');
  }
}

// Resolve a pending power action against the latest /api/miner/op_status
// snapshot. Pure helper — no DOM, no fetch. Outcome:
//   { outcome: 'pending' }                       — keep waiting
//   { outcome: 'success' }                       — terminal op_state=idle
//   { outcome: 'failure', error }                — terminal op_state=error
//   { outcome: 'timeout' }                       — >90s with no matching op
//
// pending: { action: 'powering_up'|'shutting_down', start: epoch_ms }
// opStatus: latest /api/miner/op_status JSON (or null)
// nowMs: current time in ms (injectable for testability)
function _resolvePendingPowerOutcome(pending, opStatus, nowMs) {
  if (!pending || !pending.action) return { outcome: 'pending' };

  const expectedKind = pending.action === 'powering_up' ? 'resume' : 'stop';
  const elapsed = nowMs - pending.start;

  // Terminal-state matching: an op_status entry counts as "matching" only if
  // its op_kind agrees with the pending action AND its started_at is at or
  // after the pending.start (with a 2s clock-skew tolerance). Otherwise we
  // treat it as stale/unrelated and keep waiting.
  const opMatches = opStatus
    && opStatus.op_kind === expectedKind
    && typeof opStatus.started_at === 'number'
    && (opStatus.started_at * 1000) >= (pending.start - 2000);

  if (!opMatches) {
    // A different op is running/completed that started after our click.
    // The queue is FIFO — our op must have already completed for the next
    // one to start. Treat this as success rather than letting it time out.
    const newerOpRan = opStatus
      && typeof opStatus.started_at === 'number'
      && (opStatus.started_at * 1000) >= (pending.start - 2000)
      && (opStatus.op_state === 'idle' || opStatus.op_state === 'applying');
    if (newerOpRan) return { outcome: 'success' };

    if (elapsed > 90000) return { outcome: 'timeout' };
    return { outcome: 'pending' };
  }

  if (opStatus.op_state === 'idle') return { outcome: 'success' };
  if (opStatus.op_state === 'error') {
    return { outcome: 'failure', error: opStatus.error || 'unknown error' };
  }
  return { outcome: 'pending' };
}

// Pending power action helpers (Layer 8 transitional UI)
function _setPendingPowerIndicator(text, color) {
  let el = document.getElementById('powerActionIndicator');
  if (!el) {
    el = document.createElement('span');
    el.id = 'powerActionIndicator';
    el.style.cssText = 'margin-left:8px;font-size:0.85em;font-weight:500;';
    const toggle = document.getElementById('minerPowerToggle');
    if (toggle && toggle.parentNode) toggle.parentNode.appendChild(el);
  }
  el.textContent = text;
  el.style.color = color || '#6c757d';
}

function _clearPendingPower(checkedState, errorMsg) {
  state.pendingPowerAction = null;
  state.pendingPowerStart = 0;
  const toggle = document.getElementById('minerPowerToggle');
  if (toggle) {
    toggle.checked = checkedState;
    toggle.disabled = false;
  }
  const el = document.getElementById('powerActionIndicator');
  if (el) {
    el.textContent = errorMsg || '';
    el.style.color = errorMsg ? '#dc3545' : '';
    if (!errorMsg) setTimeout(() => { if (el) el.textContent = ''; }, 2000);
  }
}

// Setup event listeners
function setupEventListeners() {
  // Power toggle with transitional UI (Layer 8)
  document.getElementById('minerPowerToggle').addEventListener('change', async (e) => {
    const intendOn = e.target.checked;
    const endpoint = intendOn ? '/api/miner/power_on' : '/api/miner/power_off';
    state.pendingPowerAction = intendOn ? 'powering_up' : 'shutting_down';
    state.pendingPowerStart = Date.now();
    e.target.disabled = true;
    _setPendingPowerIndicator(intendOn ? 'Powering up…' : 'Shutting down…', '#6c757d');
    try {
      await fetch(endpoint, { method: 'POST' });
    } catch (err) {
      addError('Power toggle error: ' + err.message);
      _clearPendingPower(!intendOn);
    }
  });

  // Auto-control toggle.
  // The /enable endpoint can refuse (HTTP 400) when user_power_intent is OFF.
  // In that case revert the checkbox to its prior state and surface the
  // server-supplied message so the user sees why the click was rejected.
  document.getElementById('autoControlToggle').addEventListener('change', async (e) => {
    const wantEnable = e.target.checked;
    const endpoint = wantEnable ? '/api/autocontrol/enable' : '/api/autocontrol/disable';
    try {
      const resp = await fetch(endpoint, { method: 'POST' });
      if (!resp.ok) {
        // Revert the toggle — the backend refused.
        e.target.checked = !wantEnable;
        let msg = `Auto-control ${wantEnable ? 'enable' : 'disable'} failed (HTTP ${resp.status})`;
        try {
          const body = await resp.json();
          if (body && body.message) msg = body.message;
          else if (body && body.error) msg += `: ${body.error}`;
        } catch (_) { /* non-JSON body — keep default msg */ }
        addError(msg);
      }
    } catch (err) {
      // Network failure — revert and report.
      e.target.checked = !wantEnable;
      addError('Auto-control toggle error: ' + err.message);
    }
  });

  // Unified chart trace toggles are wired up in setupChartToggles() via
  // Plotly.restyle — no need to re-render the whole chart on each toggle.
}

// Apply power percent
async function applyPowerPct() {
  const powerInput = document.getElementById('powerPctInput');
  const applyBtn = document.querySelector('button[onclick="applyPowerPct()"]');
  const percent = parseInt(powerInput.value);

  console.log('[ApplyPowerPct] User entered:', percent);

  if (isNaN(percent) || percent < 0 || percent > 100) {
    addDebugLog('[Manual Control] Invalid input - must be 0-100%', 'error');
    return;
  }

  // Disable button while processing
  if (applyBtn) {
    applyBtn.disabled = true;
    applyBtn.textContent = 'Applying...';
  }

  try {
    console.log('[ApplyPowerPct] Sending command to set power to', percent, '%');
    addDebugLog(`[Manual Control] Setting power to ${percent}%...`, 'info');

    const response = await fetch('/api/miner/power_pct', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ percent })
    });

    const result = await response.json();
    console.log('[ApplyPowerPct] Server response:', result);

    if (result.ok) {
      addDebugLog(`[Manual Control] ✓ Power set to ${percent}%`, 'success');
      state.lastManualPct = percent;

      // Immediately update target power display
      const targetWatts = Math.round(3600 * (percent / 100));
      const targetPowerDisplay = document.getElementById('minerTargetPower');
      if (targetPowerDisplay) {
        targetPowerDisplay.textContent = `${targetWatts} W (${percent}%)`;
      }

      // Wait 3 seconds before re-enabling button (allow command to process)
      setTimeout(() => {
        if (applyBtn) {
          applyBtn.disabled = false;
          applyBtn.textContent = 'Apply';
        }
      }, 3000);
    } else {
      addDebugLog('[Manual Control] ✗ Failed to set power', 'error');
      // Re-enable immediately on error
      if (applyBtn) {
        applyBtn.disabled = false;
        applyBtn.textContent = 'Apply';
      }
    }
  } catch (err) {
    console.error('[ApplyPowerPct] Error:', err);
    addError('Power percent error: ' + err.message);
    // Re-enable immediately on error
    if (applyBtn) {
      applyBtn.disabled = false;
      applyBtn.textContent = 'Apply';
    }
  }
}

// Apply emergency SOC threshold
async function applyEmergencySoc() {
  const input = document.getElementById('emergencySocInput');
  const percent = parseInt(input.value);

  if (isNaN(percent) || percent < 5 || percent > 95) {
    addDebugLog('[EmergencySOC] Invalid value — must be 5-95%', 'error');
    return;
  }

  try {
    addDebugLog(`[EmergencySOC] Setting to ${percent}%...`, 'info');
    const response = await fetch('/api/autocontrol/emergency_soc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ percent })
    });
    const result = await response.json();
    if (response.ok) {
      document.getElementById('emergencySocCurrent').textContent = result.percent;
      addDebugLog(`[EmergencySOC] ✓ Set to ${result.percent}%`, 'success');
    } else {
      addDebugLog(`[EmergencySOC] ✗ Error: ${result.error}`, 'error');
    }
  } catch (e) {
    addDebugLog(`[EmergencySOC] Request failed: ${e.message}`, 'error');
  }
}

// Add error
function addError(msg) {
  console.error('[Dashboard]', msg);
  state.errors.push(`[${new Date().toLocaleTimeString()}] ${msg}`);

  // Log to debug console instead
  addDebugLog(msg, 'error');
}

// Clear errors
function clearErrors() {
  state.errors = [];
}

// Start polling
function startPolling() {
  console.log('[StartPolling] Starting polling intervals...');
  // Refresh status every 10 seconds
  setInterval(refreshStatus, 10000);

  // Poll backend logs every 3 seconds
  setInterval(pollBackendLogs, 3000);

  // Refresh the weather card every 60s — forecast is hourly upstream so a
  // tighter cadence is wasteful, but 60s keeps the freshness indicator honest.
  setInterval(refreshWeatherCard, 60000);

  // Initial refresh
  console.log('[StartPolling] Calling initial refreshStatus()...');
  refreshStatus();
  console.log('[StartPolling] Calling initial pollBackendLogs()...');
  pollBackendLogs();
  refreshWeatherCard();
}

// Poll backend logs
let lastLogCount = 0;
async function pollBackendLogs() {
  try {
    const res = await fetch('/api/system/logs?count=200');
    const data = await res.json();
    const logs = data.logs || [];

    // Only add new logs (avoid duplicates)
    if (logs.length > lastLogCount) {
      const newLogs = logs.slice(lastLogCount);
      for (const log of newLogs) {
        displayBackendLog(log);
      }
    }
    lastLogCount = logs.length;

  } catch (e) {
    console.error('[Dashboard] Failed to poll backend logs:', e);
  }
}

// Display backend log
function displayBackendLog(log) {
  const debugContent = document.getElementById('debugContent');
  const line = document.createElement('div');
  line.className = `debug-line ${log.level}`;

  // Format timestamp
  const ts = new Date(log.timestamp);
  const timeStr = ts.toLocaleTimeString();

  line.innerHTML = `<span class="debug-timestamp">${timeStr}</span>${log.message}`;
  debugContent.appendChild(line);

  // Auto-scroll to bottom
  debugContent.scrollTop = debugContent.scrollHeight;

  // Keep state in sync
  state.debugLogs.push(log);
  if (state.debugLogs.length > 200) {
    state.debugLogs.shift();
  }
}

// Helper functions
function pickFanRPM(row) {
  for (const k in row) {
    if (k.toLowerCase().includes('fan') || k.toLowerCase().includes('rpm')) {
      const v = parseInt(row[k]);
      if (!isNaN(v) && v > 0) return v;
    }
  }
  return null;
}

function pickWMTemp(row) {
  const keys = ["Temperature", "Chip Temp Avg", "temp"];
  for (const k of keys) {
    if (k in row) return parseFloat(row[k]);
  }
  return null;
}

function pickEnvTemp(row) {
  const keys = ["Env Temperature", "Env Temp", "Ambient", "Ambient Temp"];
  for (const k of keys) {
    if (k in row) return parseFloat(row[k]);
  }
  return null;
}

// ===== Weather Gate card =====

let weatherCardLoaded = false;

async function refreshWeatherCard() {
  try {
    const res = await fetch('/api/weather/status', { cache: 'no-store' });
    if (!res.ok) return;
    const data = await res.json();
    renderWeatherCard(data);
    // Refresh the prediction tracker on the same cycle. Failures are
    // logged but do not prevent the main weather card from rendering.
    renderPredictionTrackerToday(data);
    try {
      const history = await fetchPredictionHistory(7);
      renderPredictionHistoryRows(history);
    } catch (e) {
      console.error('[Weather] prediction_history fetch failed:', e);
    }
  } catch (e) {
    console.error('[Weather] status fetch failed:', e);
  }
}

async function fetchPredictionHistory(days) {
  const res = await fetch(`/api/weather/prediction_history?days=${days}`, { cache: 'no-store' });
  if (!res.ok) return [];
  const data = await res.json();
  return Array.isArray(data.rows) ? data.rows : [];
}

function renderPredictionTrackerToday(data) {
  const forecast = data.forecast || {};
  const gate = data.gate || {};

  // EG4 raw today (forecast carries the cache; gate carries the value used).
  setText('predTodayRaw',
    (forecast.eg4_today_kwh != null) ? forecast.eg4_today_kwh.toFixed(2) + ' kWh' : '—');
  setText('predTodayMultiplier',
    (gate.multiplier_applied != null) ? gate.multiplier_applied.toFixed(2) : '—');
  setText('predTodayExpected', kwhOrDash(gate.expected_kwh));
  setText('predTodaySource', gate.decision_source || '—');
}

function renderPredictionHistoryRows(rows) {
  const tbody = document.getElementById('predictionHistoryBody');
  if (!tbody) return;
  if (!rows || rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="4" class="prediction-empty">No history yet.</td></tr>';
    return;
  }
  const html = rows.map((r) => {
    const dateStr = r.date || '—';
    const rawStr = _kwhCellOrDash(r.eg4_today_kwh_raw);
    const actualStr = _kwhCellOrDash(r.actual_kwh);
    const ratioRaw = r.ratio_actual_to_eg4_raw;
    let ratioStr = '—';
    let ratioClass = '';
    if (ratioRaw && ratioRaw !== '') {
      const ratio = parseFloat(ratioRaw);
      if (!Number.isNaN(ratio)) {
        ratioStr = ratio.toFixed(2);
        if (ratio >= 0.7 && ratio <= 1.1) ratioClass = 'ratio-good';
        else if (ratio >= 0.5 && ratio <= 1.3) ratioClass = 'ratio-warn';
        else ratioClass = 'ratio-bad';
      }
    }
    // Curtailment indicator: when the day ended because the battery hit
    // 100% before sunset, paint the row yellow and surface the reason in
    // a tooltip. Other end_reason values (sunset, unknown) get no special
    // treatment — the row stays in its default style.
    let rowClass = '';
    let rowTitleAttr = '';
    if (r.actual_end_reason === 'battery_full') {
      rowClass = 'prediction-row-curtailed';
      rowTitleAttr = ' title="Solar curtailed — battery hit 100% before sunset."';
    }
    return `<tr class="${rowClass}"${rowTitleAttr}>
      <td>${dateStr}</td>
      <td>${rawStr}</td>
      <td>${actualStr}</td>
      <td class="${ratioClass}">${ratioStr}</td>
    </tr>`;
  }).join('');
  tbody.innerHTML = html;
}

function _kwhCellOrDash(raw) {
  if (raw === null || raw === undefined || raw === '') return '—';
  const v = parseFloat(raw);
  if (Number.isNaN(v)) return '—';
  return v.toFixed(2);
}

function renderWeatherCard(data) {
  const card = document.getElementById('weatherForecastCard');
  if (!card) return;
  const forecast = data.forecast || {};
  const gate = data.gate || {};
  const cfg = data.config || {};

  // Section A: forecast values + decision
  setText('weatherSunrise', formatLocalTime(forecast.sunrise));
  setText('weatherSunset', formatLocalTime(forecast.sunset));
  setText('weatherCloudCover',
    (forecast.cloud_cover_pct != null) ? forecast.cloud_cover_pct.toFixed(0) + '%' : '—');
  setText('weatherMaxForDay', kwhOrDash(gate.max_for_day_kwh));
  setText('weatherExpected', kwhOrDash(gate.expected_kwh));

  // SOC from autocontrol status if available — keeps Weather card in sync
  // without an extra fetch since the main loop already polls autocontrol.
  const auto = state.lastAutocontrolStatus || {};
  const socFromAuto = (auto.battery_fresh && state.lastBatteryStatus)
    ? state.lastBatteryStatus.soc_percent
    : null;
  setText('weatherSoc', (socFromAuto != null) ? socFromAuto.toFixed(1) + '%' : '—');
  setText('weatherDeficit', kwhOrDash(gate.deficit_kwh));
  setText('weatherEvaluatedAt', gate.evaluated_at || '—');

  const banner = document.getElementById('weatherDecisionBanner');
  if (banner) {
    let cls = 'weather-decision weather-decision-neutral';
    let txt = 'No evaluation yet today.';
    if (gate.reason === 'sufficient_solar_expected') {
      cls = 'weather-decision weather-decision-on';
      txt = 'Autocontrol enabled — forecast solar is sufficient.';
    } else if (gate.reason === 'insufficient_solar_expected') {
      cls = 'weather-decision weather-decision-off';
      txt = 'Weather-disabled — forecast solar is insufficient. Battery charging only.';
    } else if (gate.reason === 'recovered_soc_in_time') {
      cls = 'weather-decision weather-decision-recovered';
      txt = 'Recovered — SOC climbed back with enough daylight remaining.';
    } else if (gate.reason === 'recovery_window_too_short') {
      cls = 'weather-decision weather-decision-off';
      txt = 'SOC recovered but too late in day — staying disabled.';
    } else if (gate.reason === 'midnight_reset') {
      cls = 'weather-decision weather-decision-neutral';
      txt = 'Midnight reset — awaiting next pre-sunrise evaluation.';
    } else if (gate.reason === 'gate_master_disabled') {
      cls = 'weather-decision weather-decision-neutral';
      txt = 'Weather gate disabled by master switch.';
    } else if (gate.reason) {
      txt = `Status: ${gate.reason}`;
    }
    banner.className = cls;
    banner.textContent = txt;
  }

  const freshnessEl = document.getElementById('weatherFreshness');
  if (freshnessEl) {
    const age = forecast.age_seconds;
    const fresh = forecast.is_fresh;
    if (age == null) {
      freshnessEl.textContent = 'Forecast freshness: no data yet';
    } else {
      const ageStr = age >= 3600 ? `${(age / 3600).toFixed(1)}h` : `${Math.round(age)}s`;
      freshnessEl.textContent = `Forecast freshness: ${ageStr} old — ${fresh ? 'fresh' : 'STALE'}`;
    }
  }

  // Section A2: Tier promotion — sourced from autocontrol/status (the main
  // poll loop already fetches it). Falls back to '—' before the first poll.
  renderTierPromotion();

  // Section B: editable config — only populate the inputs once, otherwise
  // typing would get clobbered on every poll.
  if (!weatherCardLoaded) {
    setNumber('weatherCfgBatteryKwh', cfg.battery_total_kwh);
    setNumber('weatherCfgSummerKwh', cfg.summer_max_kwh);
    setNumber('weatherCfgWinterKwh', cfg.winter_max_kwh);
    setNumber('weatherCfgPreSunriseMin', cfg.pre_sunrise_window_minutes);
    setNumber('weatherCfgRecoverySoc', cfg.recovery_soc_threshold_pct);
    setNumber('weatherCfgRecoveryHours', cfg.recovery_min_hours_before_sunset);
    setNumber('weatherCfgEg4Mult', cfg.eg4_predict_multiplier);
    const enabledEl = document.getElementById('weatherCfgEnabled');
    if (enabledEl) enabledEl.checked = !!cfg.enabled;
    weatherCardLoaded = true;
  }
}

function renderTierPromotion() {
  const auto = state.lastAutocontrolStatus || {};
  const tp = auto.tier_promotion || {};
  const tier = tp.tier;

  // "Tier: 80% / 90% / 100%" — when the tier-promotion service is not
  // promoting (tier=null), the effective tier is the decile table cap (80%).
  let tierLabel = '80%';
  if (tier === 100) tierLabel = '100%';
  else if (tier === 90) tierLabel = '90%';
  setText('tierPromoTier', tierLabel);

  const baselineSoc = tp.tier_baseline_soc;
  setText(
    'tierPromoTierBaseline',
    (typeof baselineSoc === 'number') ? baselineSoc.toFixed(1) + '%' : '—'
  );

  setText('tierPromoCooldown90', formatCooldown(tp.cooldown_remaining_90_sec));
  setText('tierPromoCooldown100', formatCooldown(tp.cooldown_remaining_100_sec));

  const detail = document.getElementById('tierPromoDetail');
  if (!detail) return;
  detail.className = 'tier-promo-detail';
  if (tier === 100 || tier === 90) {
    detail.classList.add('tier-promo-active');
    const sunsetStr = (auto.weather_gate && auto.weather_gate.sunset_dt) || null;
    const hoursStr = hoursUntil(sunsetStr);
    const hoursPart = hoursStr ? ` — ${hoursStr} to sunset` : '';
    detail.textContent = `Promoted to ${tier}% — clear sky${hoursPart}.`;
  } else if (
    (tp.cooldown_remaining_90_sec && tp.cooldown_remaining_90_sec > 0) ||
    (tp.cooldown_remaining_100_sec && tp.cooldown_remaining_100_sec > 0)
  ) {
    detail.classList.add('tier-promo-cooldown');
    const c90 = tp.cooldown_remaining_90_sec || 0;
    const c100 = tp.cooldown_remaining_100_sec || 0;
    const active = c100 > c90 ? c100 : c90;
    const tierName = c100 > c90 ? '100%' : '90%';
    detail.textContent = `Cooldown: ${Math.ceil(active / 60)}m remaining (${tierName} re-promotion blocked).`;
  } else {
    detail.textContent = 'Tier promotion inactive — falling through to decile table.';
  }
}

function formatCooldown(sec) {
  if (sec == null || sec <= 0) return 'inactive';
  if (sec >= 60) return `${Math.ceil(sec / 60)}m remaining`;
  return `${sec}s remaining`;
}

function hoursUntil(isoOrNull) {
  if (!isoOrNull) return null;
  try {
    const dt = new Date(isoOrNull);
    if (isNaN(dt.getTime())) return null;
    const diffSec = (dt.getTime() - Date.now()) / 1000;
    if (diffSec <= 0) return null;
    const h = diffSec / 3600;
    return h >= 1 ? `${h.toFixed(1)}h` : `${Math.round(diffSec / 60)}m`;
  } catch {
    return null;
  }
}

async function saveWeatherConfig() {
  const status = document.getElementById('weatherSaveStatus');
  const body = {};

  const enabledEl = document.getElementById('weatherCfgEnabled');
  if (enabledEl) body.enabled = enabledEl.checked;

  const fields = [
    ['weatherCfgBatteryKwh', 'battery_total_kwh', parseFloat],
    ['weatherCfgSummerKwh', 'summer_max_kwh', parseFloat],
    ['weatherCfgWinterKwh', 'winter_max_kwh', parseFloat],
    ['weatherCfgPreSunriseMin', 'pre_sunrise_window_minutes', parseInt],
    ['weatherCfgRecoverySoc', 'recovery_soc_threshold_pct', parseInt],
    ['weatherCfgRecoveryHours', 'recovery_min_hours_before_sunset', parseFloat],
    ['weatherCfgEg4Mult', 'eg4_predict_multiplier', parseFloat],
  ];
  for (const [id, key, fn] of fields) {
    const el = document.getElementById(id);
    if (!el) continue;
    const raw = el.value;
    if (raw === '') continue;
    const val = fn(raw);
    if (Number.isNaN(val)) {
      if (status) status.textContent = `Invalid value for ${key}`;
      return;
    }
    body[key] = val;
  }

  try {
    const res = await fetch('/api/weather/config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (res.ok) {
      if (status) status.textContent = 'Saved.';
      weatherCardLoaded = false;  // reload from server next poll
      refreshWeatherCard();
    } else {
      if (status) status.textContent = `Error: ${data.error || res.status}`;
    }
  } catch (e) {
    if (status) status.textContent = `Error: ${e.message}`;
  }
}

async function forceEvaluateWeatherGate() {
  const btn = document.getElementById('weatherEvaluateBtn');
  if (btn) btn.disabled = true;
  try {
    const res = await fetch('/api/weather/evaluate_now', { method: 'POST' });
    const data = await res.json();
    addDebugLog(`[Weather] evaluate_now: ${JSON.stringify(data)}`, 'info');
    refreshWeatherCard();
  } catch (e) {
    addDebugLog(`[Weather] evaluate_now failed: ${e.message}`, 'error');
  } finally {
    if (btn) btn.disabled = false;
  }
}

// Small DOM helpers used by the weather card. Kept local because they are
// pure presentational sugar — nothing else in the file needs them.
function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function setNumber(id, value) {
  const el = document.getElementById(id);
  if (el && value != null) el.value = String(value);
}

function kwhOrDash(v) {
  return (v != null) ? `${v.toFixed(2)} kWh` : '—';
}

function formatLocalTime(isoOrNull) {
  if (!isoOrNull) return '—';
  try {
    const dt = new Date(isoOrNull);
    if (isNaN(dt.getTime())) return isoOrNull;
    return dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  } catch {
    return isoOrNull;
  }
}
