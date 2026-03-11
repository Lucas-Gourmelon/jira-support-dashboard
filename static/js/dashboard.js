const ASSIGNEE_TOP_N = null;

const syncBtn = document.getElementById("syncBtn");
const syncDot = document.getElementById("syncDot");
const syncText = document.getElementById("syncText");
const syncMeta = document.getElementById("syncMeta");
const errorLine = document.getElementById("errorLine");
const toggleClosed = document.getElementById("toggleClosed");

const runtimeLine = document.getElementById("runtimeLine");
const syncErrorText = document.getElementById("syncErrorText");
const syncLiveBody = document.getElementById("syncLiveBody");
const refreshInfoInterval = document.getElementById("refreshInfoInterval");

const kpiTotal = document.getElementById("kpiTotal");
const kpiOpen = document.getElementById("kpiOpen");
const kpiClosed = document.getElementById("kpiClosed");
const kpiOldestCreated = document.getElementById("kpiOldestCreated");
const kpiOldestCreatedHint = document.getElementById("kpiOldestCreatedHint");
const kpiOldestUpdated = document.getElementById("kpiOldestUpdated");
const kpiOldestUpdatedHint = document.getElementById("kpiOldestUpdatedHint");

const assigneeBody = document.getElementById("assigneeBody");
const oldestBody = document.getElementById("oldestBody");
const assigneeMeta = document.getElementById("assigneeMeta");
const timeByProjectBody = document.getElementById("timeByProjectBody");
const timeByProjectMeta = document.getElementById("timeByProjectMeta");
const timeByProjectWrap = document.getElementById("timeByProjectWrap");

const statusFamilyMeta = document.getElementById("statusFamilyMeta");
const statusFamilyChart = document.getElementById("statusFamilyChart");

const lastRefresh = document.getElementById("lastRefresh");
const syncInfoStatus = document.getElementById("syncInfoStatus");

let refreshTimer = null;
let refreshIntervalSeconds = 10;
let statusChartRendered = false;

let liveRefreshTimer = null;
let liveRefreshIntervalMs = 4000;
let lastRenderedLiveSignature = "";
let dashboardConfig = null;
let lastSyncStatus = null;

function fmtInt(n) {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString(undefined, {
    maximumFractionDigits: 0,
  });
}

function fmtHours1(n) {
  if (n === null || n === undefined) return "—";
  const v = Number(n);
  if (!Number.isFinite(v)) return "—";
  return v.toLocaleString(undefined, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
}

function fmtHours0(h) {
  if (h === null || h === undefined) return "—";
  const v = Number(h);
  if (!Number.isFinite(v)) return "—";
  return fmtInt(v);
}

function fmtDateTime(value) {
  if (!value) return "Never";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

function fmtTimeOnly(value) {
  if (!value) return "Never";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function setSyncPill(state, label, meta) {
  syncDot.classList.remove("good", "bad", "warn");
  syncDot.classList.add(state);
  syncText.textContent = label;
  syncMeta.textContent = meta || "—";
}

function setSyncInfoBadge(state, label) {
  if (!syncInfoStatus) return;
  syncInfoStatus.className = `sync-info-badge ${state}`;
  syncInfoStatus.textContent = label;
}

function clearError() {
  if (errorLine) {
    errorLine.textContent = "";
  }
}

function showError(msg) {
  if (errorLine) {
    errorLine.textContent = msg;
  }
}

function startAutoRefresh(seconds) {
  const parsed = Number(seconds);
  const safeSeconds = Number.isFinite(parsed) && parsed > 0 ? parsed : 10;

  refreshIntervalSeconds = safeSeconds;

  if (refreshTimer) {
    clearInterval(refreshTimer);
  }

  refreshTimer = setInterval(refreshDashboard, safeSeconds * 1000);
}

async function fetchJson(url) {
  const res = await fetch(url, { method: "GET", cache: "no-store" });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`${url} -> ${res.status}: ${txt}`);
  }
  return res.json();
}

async function fetchSyncStatus() {
  const data = await fetchJson("/sync/status");
  lastSyncStatus = data;
  return data;
}

async function fetchSyncLive() {
  return fetchJson("/sync/live?limit=3");
}

async function triggerSync() {
  const res = await fetch("/sync", { method: "POST" });

  if (res.status === 409) {
    await refreshSyncLiveOnly();
    await refreshDashboard();
    return;
  }

  if (!res.ok) {
    const txt = await res.text();
    alert("Sync error: " + txt);
    return;
  }
}

function ageSeverityClass(ageHours) {
  const h = Number(ageHours);
  if (!Number.isFinite(h)) return "age-normal";
  if (h < 48) return "age-normal";
  if (h < 168) return "age-warn";
  if (h < 720) return "age-warn-strong";
  return "age-bad";
}

function ageRowClass(ageHours) {
  const h = Number(ageHours);
  if (!Number.isFinite(h)) return "";
  if (h < 48) return "";
  if (h < 168) return "row-warn";
  if (h < 720) return "row-warn-strong";
  return "row-bad";
}

function applyAgeClass(el, ageHours) {
  el.classList.remove("age-normal", "age-warn", "age-warn-strong", "age-bad");
  el.classList.add(ageSeverityClass(ageHours));
}

function setKpiOldest(elValue, elHint, obj) {
  if (!obj || !obj.key) {
    elValue.textContent = "—";
    elHint.textContent = "—";
    applyAgeClass(elValue, null);
    return;
  }
  elValue.textContent = fmtHours0(obj.age_hours);
  elHint.textContent = `${obj.key} • ${fmtHours0(obj.age_hours)}h`;
  applyAgeClass(elValue, obj.age_hours);
}

function effortRowClass(totalHours) {
  const h = Number(totalHours);
  if (!Number.isFinite(h)) return "";
  if (h > 300) return "effort-strong";
  if (h > 100) return "effort-mild";
  return "";
}

function effortTextClass(totalHours) {
  const h = Number(totalHours);
  if (!Number.isFinite(h)) return "";
  if (h > 300) return "effort-strong-text";
  if (h > 100) return "effort-mild-text";
  return "";
}

function isUnassigned(label) {
  if (!label) return true;
  const s = String(label).trim().toLowerCase();
  return s === "unassigned";
}

function makeUnassignedBadge() {
  const span = document.createElement("span");
  span.className = "badge badge-unassigned";
  const dot = document.createElement("span");
  dot.className = "badge-dot";
  const txt = document.createElement("span");
  txt.textContent = "Unassigned";
  span.appendChild(dot);
  span.appendChild(txt);
  return span;
}

function updateScrollableCue(el) {
  if (!el) return;
  const hasOverflow = el.scrollHeight > el.clientHeight + 2;
  el.classList.toggle("has-overflow", hasOverflow);
}

function renderSyncInfo(config, sync) {
  const jql = config?.jira_jql || "N/A";
  const sqlitePath = config?.sqlite_path || "N/A";
  const pageSize = config?.jira_page_size ?? "N/A";
  const autoSyncInterval = config?.auto_sync_interval_seconds;
  const autoRefreshInterval = config?.auto_refresh_seconds;

  if (runtimeLine) {
    runtimeLine.textContent = `JQL: ${jql} • SQLite: ${sqlitePath} • Page size: ${pageSize} • Refresh: ${autoRefreshInterval ?? refreshIntervalSeconds} s`;
    runtimeLine.title = `JQL: ${jql}\nSQLite: ${sqlitePath}\nPage size: ${pageSize}\nRefresh: ${autoRefreshInterval ?? refreshIntervalSeconds} s`;
  }

  if (refreshInfoInterval) {
    refreshInfoInterval.textContent =
      autoRefreshInterval !== null && autoRefreshInterval !== undefined
        ? `${fmtInt(autoRefreshInterval)} s`
        : `${fmtInt(refreshIntervalSeconds)} s`;
  }

  if (syncErrorText) {
    const hasError = !!sync?.last_error;
    syncErrorText.textContent = hasError ? String(sync.last_error) : "—";
    syncErrorText.title = hasError ? String(sync.last_error) : "";
  }

  const lastRun = fmtTimeOnly(sync?.last_run_at);
  const autoSyncText =
    autoSyncInterval !== null && autoSyncInterval !== undefined
      ? `${fmtInt(autoSyncInterval)} s`
      : "—";

  if (sync?.is_running) {
    setSyncPill(
      "warn",
      "Sync: running",
      `started ${lastRun} • Auto ${autoSyncText}`,
    );
    return;
  }

  if (sync?.success === true) {
    setSyncPill(
      "good",
      "Sync: success",
      `${lastRun} • ${fmtInt(sync.upserted)} issues • ${fmtInt(sync.duration_ms)} ms • Auto ${autoSyncText}`,
    );
    return;
  }

  if (sync?.success === false) {
    setSyncPill(
      "bad",
      "Sync: failed",
      `${lastRun} • ${fmtInt(sync.duration_ms)} ms • Auto ${autoSyncText}`,
    );
    return;
  }

  setSyncPill("warn", "Sync: idle", `Last ${lastRun} • Auto ${autoSyncText}`);
}

function getStatusFamilyColor(label, index) {
  if (label === "Open") return "#6ea8ff";
  if (label === "Analyse Client") return "#ffb020";
  if (label === "Analyse Luxtrust") return "#b197fc";
  if (label === "Closed") return "#3ad07a";
  const palette = [
    "#94a3b8",
    "#f87171",
    "#34d399",
    "#fbbf24",
    "#38bdf8",
    "#fb7185",
    "#c084fc",
    "#4ade80",
    "#f472b6",
    "#a3e635",
  ];
  return palette[index % palette.length];
}

function renderStatusFamilyChart(data) {
  let families = Array.isArray(data?.families) ? data.families : [];

  if (!toggleClosed.checked) {
    families = families.filter((f) => f.label !== "Closed");
  }

  const total = families.reduce((sum, f) => sum + Number(f.count || 0), 0);

  statusFamilyMeta.textContent = `${fmtInt(total)} tickets • ${fmtInt(families.length)} groups`;

  if (!families.length || total <= 0) {
    if (statusChartRendered) {
      try {
        zingchart.exec("statusFamilyChart", "destroy");
      } catch (e) {}
      statusChartRendered = false;
    }

    statusFamilyChart.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:center;height:220px;color:#9ca3af;">
        No data
      </div>
    `;
    return;
  }

  const series = families.map((item, index) => {
    const count = Number(item.count || 0);
    const pct = total > 0 ? (count / total) * 100 : 0;
    const color = getStatusFamilyColor(item.label, index);

    return {
      values: [count],
      text: item.label,
      backgroundColor: color,
      lineColor: color,
      valueBox: {
        text: `${item.label}\n${pct.toFixed(1)}%`,
        placement: "out",
        color: color,
        fontSize: 12,
        fontWeight: "bold",
        offsetR: 8,
      },
      tooltip: {
        text: `${item.label}: ${fmtInt(count)} tickets (${pct.toFixed(1)}%)`,
        backgroundColor: "#111827",
        borderColor: "#273244",
        borderWidth: 1,
        color: "#e5e7eb",
      },
    };
  });

  const chartConfig = {
    type: "pie",
    backgroundColor: "transparent",
    plot: {
      borderColor: "#2a303b",
      borderWidth: 3,
      slice: 64,
      size: "70%",
      valueBox: {
        placement: "out",
        connected: true,
        connectorType: "straight",
        fontFamily: "Arial",
        fontSize: 12,
        fontWeight: "bold",
        color: "#e5e7eb",
      },
      tooltip: {
        fontSize: 11,
        fontFamily: "Arial",
        borderRadius: 8,
      },
      animation: statusChartRendered
        ? {
            effect: 0,
            speed: 0,
          }
        : {
            effect: 2,
            method: 5,
            speed: 900,
            sequence: 1,
            delay: 500,
          },
    },
    plotarea: {
      margin: "4 10 4 10",
    },
    series,
  };

  if (!statusChartRendered) {
    statusFamilyChart.innerHTML = "";

    zingchart.render({
      id: "statusFamilyChart",
      data: chartConfig,
      height: "100%",
      width: "100%",
    });

    statusChartRendered = true;
  } else {
    zingchart.exec("statusFamilyChart", "setdata", {
      data: chartConfig,
    });
  }
}

function renderSyncLive(live) {
  if (!syncLiveBody) return;

  const lines = Array.isArray(live?.lines) ? live.lines.slice(-3) : [];
  const signature = JSON.stringify(lines);

  if (signature === lastRenderedLiveSignature) {
    return;
  }
  lastRenderedLiveSignature = signature;

  syncLiveBody.innerHTML = "";

  if (!lines.length) {
    syncLiveBody.innerHTML = `
      <div class="sync-live-line muted">No live sync logs yet</div>
      <div class="sync-live-line muted">—</div>
      <div class="sync-live-line muted">—</div>
    `;
    return;
  }

  for (const line of lines) {
    const div = document.createElement("div");
    div.className = "sync-live-line mono";

    const lower = String(line).toLowerCase();
    if (
      lower.includes("failed") ||
      lower.includes("error") ||
      lower.includes("exception")
    ) {
      div.classList.add("sync-live-error");
    } else if (lower.includes("finished") || lower.includes("success")) {
      div.classList.add("sync-live-success");
    } else if (
      lower.includes("running") ||
      lower.includes("started") ||
      lower.includes("processed") ||
      lower.includes("fetch") ||
      lower.includes("loading")
    ) {
      div.classList.add("sync-live-running");
    }

    div.textContent = line;
    div.title = line;
    syncLiveBody.appendChild(div);
  }

  while (syncLiveBody.children.length < 3) {
    const filler = document.createElement("div");
    filler.className = "sync-live-line muted";
    filler.textContent = "—";
    syncLiveBody.appendChild(filler);
  }
}

function applyRealtimeSyncState(sync, live) {
  if (!sync) return;

  lastSyncStatus = sync;

  if (syncBtn) {
    syncBtn.disabled = !!sync.is_running;
    syncBtn.textContent = sync.is_running ? "Sync running…" : "Sync Jira";
  }

  renderSyncInfo(dashboardConfig, sync);

  if (syncLiveBody) {
    renderSyncLive(live);
  }
}

function startLiveRefresh(intervalMs) {
  const parsed = Number(intervalMs);
  const safeMs = Number.isFinite(parsed) && parsed > 250 ? parsed : 4000;

  if (liveRefreshTimer && liveRefreshIntervalMs === safeMs) {
    return;
  }

  liveRefreshIntervalMs = safeMs;

  if (liveRefreshTimer) {
    clearInterval(liveRefreshTimer);
  }

  liveRefreshTimer = setInterval(refreshSyncLiveOnly, safeMs);
}

async function refreshSyncLiveOnly() {
  try {
    const [live, sync] = await Promise.all([
      fetchSyncLive(),
      fetchSyncStatus(),
    ]);

    applyRealtimeSyncState(sync, live);

    if (live?.is_running || sync?.is_running) {
      startLiveRefresh(800);
    } else {
      startLiveRefresh(4000);
    }
  } catch (e) {
    console.error("refreshSyncLiveOnly failed", e);
  }
}

async function refreshDashboard() {
  try {
    clearError();

    const [
      overview,
      byAssignee,
      oldestCreated,
      timeByProject,
      statusFamilyDistribution,
      syncStatus,
    ] = await Promise.all([
      fetchJson("/stats/overview"),
      fetchJson("/stats/by_assignee?only_open=true"),
      fetchJson("/stats/top_oldest_open?limit=200&sort=created"),
      fetchJson("/stats/time_by_project"),
      fetchJson("/stats/status_family_distribution"),
      fetchSyncStatus(),
    ]);

    const config = dashboardConfig;

    if (
      config?.auto_refresh_seconds &&
      Number(config.auto_refresh_seconds) !== refreshIntervalSeconds
    ) {
      startAutoRefresh(config.auto_refresh_seconds);
    }

    kpiTotal.textContent = fmtInt(overview.total_tickets);
    kpiOpen.textContent = fmtInt(overview.open_tickets);
    kpiClosed.textContent = fmtInt(overview.closed_tickets);

    setKpiOldest(
      kpiOldestCreated,
      kpiOldestCreatedHint,
      overview.oldest_open_ticket,
    );
    setKpiOldest(
      kpiOldestUpdated,
      kpiOldestUpdatedHint,
      overview.oldest_open_ticket_by_updated,
    );

    assigneeBody.innerHTML = "";
    const assignees = Array.isArray(byAssignee) ? byAssignee.slice() : [];
    assignees.sort(
      (a, b) => Number(b.open_count || 0) - Number(a.open_count || 0),
    );

    const topAssignees = ASSIGNEE_TOP_N
      ? assignees.slice(0, ASSIGNEE_TOP_N)
      : assignees;
    assigneeMeta.textContent = `${topAssignees.length} assignees • Sorted by open`;

    if (!topAssignees.length) {
      assigneeBody.innerHTML = `<tr><td colspan="4" class="muted">No data</td></tr>`;
    } else {
      for (const row of topAssignees) {
        const tr = document.createElement("tr");

        const tdA = document.createElement("td");
        tdA.className = "truncate";
        if (isUnassigned(row.assignee)) {
          tdA.appendChild(makeUnassignedBadge());
        } else {
          tdA.title = row.assignee || "";
          tdA.textContent = row.assignee || "—";
        }

        const tdC = document.createElement("td");
        tdC.className = "right mono";
        tdC.textContent = fmtInt(row.open_count);

        const tdOC = document.createElement("td");
        tdOC.className = "right mono";
        tdOC.textContent = fmtHours0(row.oldest_open_created_hours);

        const tdOU = document.createElement("td");
        tdOU.className = "right mono";
        tdOU.textContent = fmtHours0(row.oldest_open_updated_hours);

        tr.appendChild(tdA);
        tr.appendChild(tdC);
        tr.appendChild(tdOC);
        tr.appendChild(tdOU);

        assigneeBody.appendChild(tr);
      }
    }

    oldestBody.innerHTML = "";
    const oldestList = Array.isArray(oldestCreated) ? oldestCreated : [];
    if (!oldestList.length) {
      oldestBody.innerHTML = `<tr><td colspan="5" class="muted">No open tickets</td></tr>`;
    } else {
      for (const row of oldestList) {
        const tr = document.createElement("tr");
        tr.className = ageRowClass(row.age_hours);

        const tdKey = document.createElement("td");
        tdKey.className = "mono nowrap";
        tdKey.textContent = row.key || "—";

        const tdStatus = document.createElement("td");
        tdStatus.className = "truncate";
        tdStatus.title = row.status || "";
        tdStatus.textContent = row.status || "—";

        const tdPrio = document.createElement("td");
        tdPrio.className = "nowrap";
        tdPrio.textContent = row.priority || "—";

        const tdAss = document.createElement("td");
        tdAss.className = "truncate";
        if (isUnassigned(row.assignee)) {
          tdAss.appendChild(makeUnassignedBadge());
        } else {
          tdAss.title = row.assignee || "";
          tdAss.textContent = row.assignee || "—";
        }

        const tdAge = document.createElement("td");
        tdAge.className = "right mono";
        tdAge.textContent = fmtHours0(row.age_hours);
        applyAgeClass(tdAge, row.age_hours);

        tr.appendChild(tdKey);
        tr.appendChild(tdStatus);
        tr.appendChild(tdPrio);
        tr.appendChild(tdAss);
        tr.appendChild(tdAge);

        oldestBody.appendChild(tr);
      }
    }

    timeByProjectBody.innerHTML = "";
    const projects = Array.isArray(timeByProject) ? timeByProject.slice() : [];
    projects.sort(
      (a, b) =>
        Number(b.time_spent_hours || 0) - Number(a.time_spent_hours || 0),
    );

    timeByProjectMeta.textContent = `${projects.length} projects • Sorted by hours`;

    if (!projects.length) {
      timeByProjectBody.innerHTML = `<tr><td colspan="7" class="muted">No data</td></tr>`;
    } else {
      for (const row of projects) {
        const totalHours = Number(row.time_spent_hours || 0);
        const totalIssues = Number(row.total_issues || 0);
        const avg = totalIssues > 0 ? totalHours / totalIssues : 0;
        const avgResolution = row.avg_resolution_hours;

        const tr = document.createElement("tr");
        tr.className = effortRowClass(totalHours);

        const tdProj = document.createElement("td");
        tdProj.className = "mono nowrap";
        tdProj.textContent = row.project_key || "UNKNOWN";

        const tdOpen = document.createElement("td");
        tdOpen.className = "right mono";
        tdOpen.textContent = fmtInt(row.open_issues);

        const tdClosed = document.createElement("td");
        tdClosed.className = "right mono";
        tdClosed.textContent = fmtInt(row.closed_issues);

        const tdHours = document.createElement("td");
        tdHours.className = "right mono";
        tdHours.textContent = fmtHours1(totalHours);
        const tClass = effortTextClass(totalHours);
        if (tClass) tdHours.classList.add(tClass);

        const tdAvg = document.createElement("td");
        tdAvg.className = "right mono";
        tdAvg.textContent = fmtHours1(avg);

        const tdAvgResolution = document.createElement("td");
        tdAvgResolution.className = "right mono";
        tdAvgResolution.textContent = fmtHours1(avgResolution);
        tdAvgResolution.title =
          row.resolved_issues_with_dates > 0
            ? `${fmtInt(row.resolved_issues_with_dates)} resolved tickets with valid dates`
            : "No resolved tickets with valid created/resolved dates";

        const tdTickets = document.createElement("td");
        tdTickets.className = "right mono";
        tdTickets.textContent = fmtInt(totalIssues);

        tr.appendChild(tdProj);
        tr.appendChild(tdOpen);
        tr.appendChild(tdClosed);
        tr.appendChild(tdHours);
        tr.appendChild(tdAvg);
        tr.appendChild(tdAvgResolution);
        tr.appendChild(tdTickets);

        timeByProjectBody.appendChild(tr);
      }
    }

    renderStatusFamilyChart(statusFamilyDistribution);

    requestAnimationFrame(() => updateScrollableCue(timeByProjectWrap));

    const t = new Date();
    if (lastRefresh) {
      lastRefresh.textContent = t.toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
    }

    renderSyncInfo(config, syncStatus);
  } catch (e) {
    console.error("refreshDashboard failed", e);
    showError(String(e));
    try {
      const syncStatus = await fetchSyncStatus();
      renderSyncInfo(dashboardConfig, syncStatus);
    } catch (innerError) {
      console.error("Fallback refresh failed", innerError);
    }
  }
}

syncBtn.addEventListener("click", async () => {
  syncBtn.disabled = true;
  syncBtn.textContent = "Starting…";
  try {
    await triggerSync();
  } finally {
    setTimeout(refreshSyncLiveOnly, 200);
    setTimeout(refreshDashboard, 700);
  }
});

window.addEventListener("resize", () => {
  updateScrollableCue(timeByProjectWrap);
  try {
    zingchart.exec("statusFamilyChart", "resize");
  } catch (e) {}
});

toggleClosed.addEventListener("change", () => {
  refreshDashboard();
});

async function bootstrapDashboard() {
  try {
    dashboardConfig = await fetchJson("/config");
    startAutoRefresh(dashboardConfig.auto_refresh_seconds);
  } catch (e) {
    console.error("Failed to load /config", e);
    dashboardConfig = null;
    startAutoRefresh(10);
  }

  startLiveRefresh(4000);
  refreshSyncLiveOnly();

  try {
    await refreshDashboard();
  } catch (e) {
    console.error("Initial refreshDashboard failed", e);
  }
}

bootstrapDashboard();
