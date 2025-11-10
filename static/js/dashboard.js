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
  debugLogs: []  // Keep last 200 log messages
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
  autoRefreshEnabled: true,  // Auto-refresh on by default
  autoRefreshInterval: null,  // Interval timer
  lastUpdateTime: null,  // Last time data was fetched
  isUserInteracting: false  // Pause refresh during user interaction
};

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
  console.log('[Dashboard] DOMContentLoaded event fired');
  addDebugLog('Dashboard initializing...', 'info');

  try {
    console.log('[Dashboard] Step 1: Initializing chart...');
    initUnifiedChart();
    console.log('[Dashboard] Step 2: Loading chart data (2160 hours / 90 days)...');

    // Load 90 days of data but display only 3 days (72 hours) by default
    loadChartData(2160).then(() => {
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
    console.log('[Dashboard] Step 5: Starting auto-refresh...');
    startChartAutoRefresh();
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
    { x: [], y: [], name: 'Miner Power (W)', mode: 'lines', yaxis: 'y', line: { color: '#1772FF', width: 2 } },
    { x: [], y: [], name: 'PV Power In (W)', mode: 'lines', yaxis: 'y', line: { color: '#52c41a', width: 2 } },
    { x: [], y: [], name: 'EPS Power (W)', mode: 'lines', yaxis: 'y', line: { color: '#faad14', width: 2 } },
    { x: [], y: [], name: 'Net Power (W)', mode: 'lines', yaxis: 'y', line: { color: '#722ed1', width: 2 } },
    { x: [], y: [], name: 'Fan Speed (RPM)', mode: 'lines', yaxis: 'y', line: { color: '#13c2c2', width: 2 } },
    { x: [], y: [], name: 'Hash Rate (TH/s)', mode: 'lines', yaxis: 'y2', line: { color: '#eb2f96', width: 2 } },
    { x: [], y: [], name: 'Environment Temp (°C)', mode: 'lines', yaxis: 'y2', line: { color: '#fa8c16', width: 2 } },
    { x: [], y: [], name: 'Miner Temp (°C)', mode: 'lines', yaxis: 'y2', line: { color: '#f5222d', width: 2 } },
    { x: [], y: [], name: 'Battery SOC (%)', mode: 'lines', yaxis: 'y2', line: { color: '#a0d911', width: 2 } }
  ];

  Plotly.newPlot('unifiedChart', traces, layout, { responsive: true });

  // Setup checkbox listeners
  setupChartToggles();
}

// Load chart data from API
async function loadChartData(hours) {
  console.log(`[loadChartData] CALLED with hours=${hours}`);
  addDebugLog(`[API] Requesting ${hours} hours of chart data...`, 'info');

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
    mode: ['lines', 'lines', 'lines', 'lines', 'lines', 'lines', 'lines', 'lines', 'lines']
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
    console.log(`[setTimeRange] Path: ALL - calling updateUnifiedChart('all')`);
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

// Auto-refresh chart data
function startChartAutoRefresh() {
  if (chartState.autoRefreshInterval) {
    clearInterval(chartState.autoRefreshInterval);
  }

  console.log('[AutoRefresh] Starting auto-refresh with 20 second interval');
  addDebugLog('[AutoRefresh] Enabled, refreshing every 20 seconds', 'info');

  chartState.autoRefreshInterval = setInterval(async () => {
    if (!chartState.autoRefreshEnabled || chartState.isUserInteracting) {
      console.log('[AutoRefresh] Skipping - disabled or user interacting');
      return;
    }

    try {
      console.log('[AutoRefresh] Fetching latest data...');

      // Reload current time range to get fresh data
      await loadChartData(chartState.currentHours);

      chartState.lastUpdateTime = new Date();
      console.log(`[AutoRefresh] Updated at ${chartState.lastUpdateTime.toLocaleTimeString()}`);

    } catch (err) {
      console.error('[AutoRefresh] Failed:', err);
      addDebugLog(`[AutoRefresh] Update failed: ${err.message}`, 'error');
    }
  }, 20000); // 20 seconds
}

function stopChartAutoRefresh() {
  if (chartState.autoRefreshInterval) {
    clearInterval(chartState.autoRefreshInterval);
    chartState.autoRefreshInterval = null;
    console.log('[AutoRefresh] Stopped');
    addDebugLog('[AutoRefresh] Stopped', 'warning');
  }
}

function toggleAutoRefresh() {
  chartState.autoRefreshEnabled = !chartState.autoRefreshEnabled;

  const indicator = document.getElementById('autoRefreshIndicator');
  const toggleBtn = document.getElementById('autoRefreshToggle');

  if (chartState.autoRefreshEnabled) {
    startChartAutoRefresh();
    indicator.textContent = '● LIVE';
    indicator.style.color = '#52c41a';
    toggleBtn.textContent = 'Pause Auto-Refresh';
    addDebugLog('[AutoRefresh] Enabled', 'success');
  } else {
    stopChartAutoRefresh();
    indicator.textContent = '○ PAUSED';
    indicator.style.color = '#faad14';
    toggleBtn.textContent = 'Resume Auto-Refresh';
    addDebugLog('[AutoRefresh] Disabled', 'warning');
  }
}

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

// Refresh status
async function refreshStatus() {
  console.log('[RefreshStatus] Starting status refresh...');
  try {
    // Miner status
    console.log('[RefreshStatus] Fetching miner status...');
    const minerRes = await fetch('/api/miner/status');
    const minerData = await minerRes.json();
    console.log('[RefreshStatus] Miner data received:', minerData);
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

  } catch (e) {
    console.error('[RefreshStatus] ERROR:', e);
    addDebugLog('Status refresh error: ' + e.message, 'error');
  }
}

// Update miner status
function updateMinerStatus(data) {
  console.log('[UpdateMinerStatus] Received data:', data);
  const { status, connection } = data;
  console.log('[UpdateMinerStatus] Status:', status, 'Connection:', connection);

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

  // Update UI
  document.getElementById('minerHashrate').textContent = (status.Hashrate || '—') + ' TH/s';
  document.getElementById('minerPower').textContent = (status.Power || '—') + ' W';
  document.getElementById('minerTemp').textContent = (pickWMTemp(status) || '—') + ' °C';

  // Fan: Show "0 RPM" if fan is off, not "— RPM"
  const fanSpeed = pickFanRPM(status);
  if (fanSpeed === null || fanSpeed === undefined) {
    document.getElementById('minerFan').textContent = '0 RPM';
  } else {
    document.getElementById('minerFan').textContent = fanSpeed + ' RPM';
  }

  // Update power toggle
  const powerToggle = document.getElementById('minerPowerToggle');
  powerToggle.checked = status.is_mining !== false;
}

// Update auto-control status
function updateAutoControlStatus(data) {
  console.log('[UpdateAutoControl] Received data:', data);

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
  if (target_w !== null && target_w !== undefined) {
    targetPowerDisplay.textContent = `${target_w} W (${target_pct || 0}%)`;
  } else if (target_pct !== null && target_pct !== undefined) {
    // Calculate watts from percentage (max 3600W)
    const targetWatts = Math.round(3600 * (target_pct / 100));
    targetPowerDisplay.textContent = `${targetWatts} W (${target_pct}%)`;
  } else {
    targetPowerDisplay.textContent = '— W';
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
}

// Update battery status
function updateBatteryStatus(data) {
  console.log('[UpdateBatteryStatus] Received data:', data);
  const { status, connection } = data;
  console.log('[UpdateBatteryStatus] Status:', status, 'Connection:', connection);

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
  document.getElementById('batterySOC').textContent = (status.soc_percent || '—') + ' %';
  document.getElementById('batteryPV').textContent = (status.pv_power_w || '—') + ' W';
  document.getElementById('batteryLoad').textContent = (status.load_power_w || '—') + ' W';
  document.getElementById('batteryNet').textContent = (status.battery_net_w || '—') + ' W';
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

// Setup event listeners
function setupEventListeners() {
  // Power toggle
  document.getElementById('minerPowerToggle').addEventListener('change', async (e) => {
    const endpoint = e.target.checked ? '/api/miner/power_on' : '/api/miner/power_off';
    try {
      await fetch(endpoint, { method: 'POST' });
    } catch (err) {
      addError('Power toggle error: ' + err.message);
    }
  });

  // Auto-control toggle
  document.getElementById('autoControlToggle').addEventListener('change', async (e) => {
    const endpoint = e.target.checked ? '/api/autocontrol/enable' : '/api/autocontrol/disable';
    try {
      await fetch(endpoint, { method: 'POST' });
    } catch (err) {
      addError('Auto-control toggle error: ' + err.message);
    }
  });

  // Unified chart checkboxes (new IDs)
  ['toggle_hash_rate', 'toggle_miner_power', 'toggle_fan_speed', 'toggle_env_temp',
   'toggle_miner_temp', 'toggle_battery_soc', 'toggle_pv_power', 'toggle_eps_power',
   'toggle_net_power'].forEach(id => {
    const elem = document.getElementById(id);
    if (elem) {
      elem.addEventListener('change', () => {
        // Re-render the unified chart with current data
        updateUnifiedChart(chartState.currentHours);
      });
    }
  });
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

  // Initial refresh
  console.log('[StartPolling] Calling initial refreshStatus()...');
  refreshStatus();
  console.log('[StartPolling] Calling initial pollBackendLogs()...');
  pollBackendLogs();
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
