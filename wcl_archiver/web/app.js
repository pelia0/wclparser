let currentFightSummary = null;
let currentFightUid = null;
let currentFightTab = "damage";
let replayPlayer = null;
let fightsOffset = 0;
const fightsPageSize = 100;
let fightsRequest = 0;
let downloadSettingsSupported = false;
let downloadSettingsApplying = false;
let eventsState = {
  eventType: "all",
  tStartSec: "",
  tEndSec: "",
  offset: 0,
  limit: 500,
};

const CLASS_COLORS = {
  Warrior: "#C79C6E",
  Paladin: "#F58CBA",
  Hunter: "#ABD473",
  Rogue: "#FFF569",
  Priest: "#FFFFFF",
  DeathKnight: "#C41F3B",
  Shaman: "#0070DE",
  Mage: "#40C7EB",
  Warlock: "#8787ED",
  Monk: "#00FF96",
  Druid: "#FF7D0A",
  DemonHunter: "#A330C9",
};

const QUALITY_COLORS = {
  1: "#ffffff",
  2: "#1eff00",
  3: "#0070dd",
  4: "#a335ee",
  5: "#ff8000",
  6: "#e6cc80",
};

const RAID_DIFFICULTY_NAMES = {
  5: "Mythic",
  4: "Heroic",
  3: "Normal",
  1: "LFR",
};

const GEAR_SLOTS = [
  "Head",
  "Neck",
  "Shoulder",
  "Shirt",
  "Chest",
  "Waist",
  "Legs",
  "Feet",
  "Wrist",
  "Hands",
  "Finger 1",
  "Finger 2",
  "Trinket 1",
  "Trinket 2",
  "Back",
  "Main Hand",
  "Off Hand",
];

async function initApp() {
  replayPlayer = new window.Replay2DPlayer("replay-canvas", "replay-scrubber", "replay-time-label");
  document.getElementById("replay-play").addEventListener("click", () => replayPlayer.togglePlay());
  document.getElementById("replay-speed").addEventListener("change", e => {
    replayPlayer.speed = Number(e.target.value);
  });

  document.querySelectorAll(".nav-btn[data-view]").forEach(btn => {
    btn.addEventListener("click", () => switchView(btn.dataset.view));
  });

  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      currentFightTab = btn.dataset.tab;
      renderFightTab(currentFightTab);
    });
  });

  const playerSel = document.getElementById("fight-filter-player");
  if (playerSel) {
    playerSel.addEventListener("change", () => {
      eventsState.offset = 0;
      if (replayPlayer) replayPlayer.setFocusActor(playerSel.value);
      renderFightTab(currentFightTab);
    });
  }

  const targetSel = document.getElementById("fight-filter-target");
  if (targetSel) {
    targetSel.addEventListener("change", () => {
      eventsState.offset = 0;
      renderFightTab(currentFightTab);
    });
  }

  const resetBtn = document.getElementById("fight-filter-reset");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      if (playerSel) playerSel.value = "";
      if (targetSel) targetSel.value = "";
      eventsState.offset = 0;
      if (replayPlayer) replayPlayer.setFocusActor(null);
      renderFightTab(currentFightTab);
    });
  }

  document.getElementById("btn-search").addEventListener("click", () => loadFightsList(0));
  document.getElementById("btn-fights-prev").addEventListener("click", () => loadFightsList(Math.max(0, fightsOffset - fightsPageSize)));
  document.getElementById("btn-fights-next").addEventListener("click", () => loadFightsList(fightsOffset + fightsPageSize));
  document.getElementById("btn-save-auth").addEventListener("click", submitAuth);
  document.getElementById("btn-harvest-mplus").addEventListener("click", () => startHarvest("mplus_s5"));
  document.getElementById("btn-harvest-raids").addEventListener("click", () => startHarvest("raids"));
  document.getElementById("btn-harvest-stop").addEventListener("click", stopHarvest);
  const applyDownloads = document.getElementById("btn-apply-download-threads");
  if (applyDownloads) applyDownloads.addEventListener("click", applyDownloadConcurrency);
  const downloadThreads = document.getElementById("harvest-download-threads");
  if (downloadThreads) downloadThreads.addEventListener("input", () => {
    downloadThreads.dataset.dirty = "1";
    downloadThreads.setCustomValidity("");
  });
  document.getElementById("btn-import-report").addEventListener("click", importSingleReport);
  const btnBrowserOpen = document.getElementById("btn-browser-open");
  if (btnBrowserOpen) btnBrowserOpen.addEventListener("click", openBrowserWindow);
  const btnBrowserClose = document.getElementById("btn-browser-close");
  if (btnBrowserClose) btnBrowserClose.addEventListener("click", closeBrowserWindow);

  await loadCatalog();
  await refreshAuthStatus();
  await refreshHarvesterStatus();
  await loadFightsList();
  setInterval(refreshHarvesterStatus, 1000);
}

function switchView(viewName) {
  document.querySelectorAll(".nav-btn").forEach(b => {
    b.classList.toggle("active", b.dataset.view === viewName);
  });
  document.querySelectorAll(".view").forEach(v => {
    v.classList.toggle("active", v.id === `view-${viewName}`);
  });
}

async function loadCatalog() {
  const res = await fetch("/api/meta/catalog");
  const cat = await res.json();
  const encSel = document.getElementById("filter-encounter");
  (cat.mplus_dungeons || []).forEach(d => {
    const opt = document.createElement("option");
    opt.value = d.id;
    opt.textContent = d.name;
    encSel.appendChild(opt);
  });
  Object.values(cat.raids || {}).forEach(raid => {
    const raidName = raid.name || raid.slug;
    Object.entries(raid.bosses || {}).forEach(([bossId, bossSlug]) => {
      const opt = document.createElement("option");
      opt.value = bossId;
      const bossName = bossSlug.replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase());
      opt.textContent = `${raidName} - ${bossName}`;
      encSel.appendChild(opt);
    });
  });
  const specSel = document.getElementById("filter-spec");
  (cat.specs || []).forEach(s => {
    const opt = document.createElement("option");
    opt.value = s.spec;
    opt.textContent = `${s.class} - ${s.spec}`;
    specSel.appendChild(opt);
  });
}

function renderAuthState(st) {
  const badge = document.getElementById("auth-badge");
  badge.textContent = st.authenticated ? `Authenticated (${st.email})` : "Not Authenticated";
  badge.style.color = st.authenticated ? "#4ade80" : "#f87171";

  const banner = document.getElementById("auth-feedback-banner");
  if (banner) {
    if (st.last_error) {
      banner.style.display = "block";
      banner.style.background = "#3b181e";
      banner.style.border = "1px solid #ef4444";
      banner.style.color = "#fca5a5";
      banner.textContent = st.last_error;
    } else if (st.authenticated && st.status_message) {
      banner.style.display = "block";
      banner.style.background = "#123524";
      banner.style.border = "1px solid #22c55e";
      banner.style.color = "#86efac";
      banner.textContent = st.status_message;
    } else {
      banner.style.display = "none";
    }
  }
  document.getElementById("auth-status-box").textContent = JSON.stringify(st, null, 2);
}

async function refreshAuthStatus() {
  const res = await fetch("/api/auth/status");
  const st = await res.json();
  renderAuthState(st);
}

async function submitAuth() {
  const btn = document.getElementById("btn-save-auth");
  const statusBox = document.getElementById("auth-status-box");
  btn.disabled = true;
  btn.textContent = "Signing in to WarcraftLogs...";
  statusBox.textContent = "Connecting to WarcraftLogs...";

  const xsrfEl = document.getElementById("auth-xsrf");
  const payload = {
    email: document.getElementById("auth-email").value.trim(),
    password: document.getElementById("auth-password").value,
    client_id: document.getElementById("auth-client-id").value.trim(),
    client_secret: document.getElementById("auth-client-secret").value.trim(),
    wcl_session_cookie: document.getElementById("auth-cookie").value.trim(),
    xsrf_token: xsrfEl ? xsrfEl.value.trim() : "",
  };
  try {
    const res = await fetch("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const st = await res.json();
    renderAuthState(st);
  } catch (err) {
    statusBox.textContent = "Error: " + String(err);
  } finally {
    btn.disabled = false;
    btn.textContent = "Sign In & Save Session";
  }
}

function renderHarvesterState(st) {
  const downloadThreads = document.getElementById("harvest-download-threads");
  const applyDownloads = document.getElementById("btn-apply-download-threads");
  const settingsFeedback = document.getElementById("download-settings-feedback");
  downloadSettingsSupported = Number.isInteger(st.download_concurrency);
  if (downloadThreads) {
    downloadThreads.disabled = !downloadSettingsSupported;
    if (downloadSettingsSupported && document.activeElement !== downloadThreads && !downloadThreads.dataset.dirty) {
      downloadThreads.value = String(st.download_concurrency);
    }
  }
  if (applyDownloads) applyDownloads.disabled = downloadSettingsApplying || !downloadSettingsSupported;
  if (!downloadSettingsSupported && settingsFeedback) {
    settingsFeedback.textContent = "Restart server to enable download controls.";
  }
  const workerSummary = document.getElementById("download-worker-summary");
  if (workerSummary && downloadSettingsSupported) {
    workerSummary.textContent = `${st.download_concurrency} downloads · ${st.browser_request_concurrency} request limit · ${st.pack_workers} pack workers`;
  }
  const staging = document.getElementById("stat-staging");
  if (staging) staging.textContent = `${st.staging_files || 0} files / ${(Number(st.staging_bytes || 0) / (1024 * 1024)).toFixed(2)} MB`;
  const packing = document.getElementById("stat-packing");
  if (packing) packing.textContent = `${st.packing_fights || 0} / ${st.pack_workers || 4} workers`;
  const downloading = document.getElementById("stat-downloading");
  if (downloading) downloading.textContent = Number.isInteger(st.downloading_fights)
    ? `${st.downloading_fights} / ${st.download_concurrency || 4} workers`
    : "Restart server for active count";
  const preparing = document.getElementById("stat-preparing");
  if (preparing) preparing.textContent = Number.isInteger(st.preparing_reports) ? String(st.preparing_reports) : "—";
  const errBanner = document.getElementById("harvester-error-banner");
  if (errBanner) {
    if (st.last_error) {
      errBanner.style.display = "block";
      errBanner.textContent = st.last_error;
    } else {
      errBanner.style.display = "none";
    }
  }

  const bState = st.browser_state || "not_started";
  const bLabel = document.getElementById("browser-state-label");
  if (bLabel) {
    bLabel.textContent = bState;
    if (bState === "ready") {
      bLabel.style.color = "#4ade80";
    } else if (bState === "waiting_challenge" || bState === "waiting_login") {
      bLabel.style.color = "#fbbf24";
    } else {
      bLabel.style.color = "#e2e8f0";
    }
  }

  const bBanner = document.getElementById("browser-action-banner");
  if (bBanner) {
    if (bState === "waiting_challenge" || bState === "waiting_login") {
      bBanner.style.display = "block";
      bBanner.textContent = st.browser_message || (
        bState === "waiting_challenge"
          ? "Solve the challenge in the browser window"
          : "Please log in in the browser window"
      );
    } else {
      bBanner.style.display = "none";
    }
  }

  const taskLabel = document.getElementById("harvest-task-label");
  if (taskLabel) taskLabel.textContent = st.current_task || "Idle";

  const pct = Number(st.progress_pct || 0);
  const pctLabel = document.getElementById("harvest-pct-label");
  if (pctLabel) {
    pctLabel.textContent = `${pct.toFixed(1)}% (${st.specs_done || 0} / ${st.specs_total || 0} specs)`;
  }

  const fill = document.getElementById("harvest-progress-fill");
  if (fill) {
    fill.style.width = `${Math.min(100, Math.max(0, pct))}%`;
  }

  const statState = document.getElementById("stat-state");
  if (statState) {
    if (st.running) {
      statState.textContent = "RUNNING";
      statState.style.color = "#4ade80";
    } else if (st.last_error) {
      statState.textContent = "ERROR";
      statState.style.color = "#f87171";
    } else {
      statState.textContent = "IDLE";
      statState.style.color = "#94a3b8";
    }
  }

  const statPages = document.getElementById("stat-pages");
  if (statPages) statPages.textContent = String(st.pages_scanned || 0);

  const statQueued = document.getElementById("stat-queued");
  if (statQueued) statQueued.textContent = String(st.queued_fights || 0);

  const statPacked = document.getElementById("stat-packed");
  if (statPacked) statPacked.textContent = String(st.packed_fights || 0);

  const statSize = document.getElementById("stat-size");
  if (statSize) statSize.textContent = `${st.compressed_mb || 0} MB`;
  const statFailures = document.getElementById("stat-failures");
  if (statFailures) statFailures.textContent = `${st.failed_reports || 0} reports / ${st.failed_fights || 0} fights`;
  const statPending = document.getElementById("stat-pending");
  if (statPending) statPending.textContent = `${st.pending_reports || 0} reports / ${(st.retryable_reports || 0) + (st.retryable_fights || 0)} retries`;

  const box = document.getElementById("harvester-status-box");
  if (box) {
    const logs = st.recent_logs || [];
    box.textContent = logs.length > 0 ? logs.slice().reverse().join("\n") : JSON.stringify(st, null, 2);
  }
}

async function refreshHarvesterStatus() {
  const res = await fetch("/api/harvester/status");
  const st = await res.json();
  renderHarvesterState(st);
}

function getSelectedSourceMode() {
  const el = document.getElementById("harvest-source-mode");
  return el ? el.value : "browser";
}

async function openBrowserWindow() {
  const btn = document.getElementById("btn-browser-open");
  if (btn) {
    btn.disabled = true;
    btn.textContent = "Opening Browser...";
  }
  try {
    const res = await fetch("/api/browser/open", { method: "POST" });
    const st = await res.json();
    if (!res.ok) {
      const errBanner = document.getElementById("harvester-error-banner");
      if (errBanner) {
        errBanner.style.display = "block";
        errBanner.textContent = st.detail || "Failed to open browser window";
      }
    } else {
      renderHarvesterState(st);
    }
  } catch (err) {
    const errBanner = document.getElementById("harvester-error-banner");
    if (errBanner) {
      errBanner.style.display = "block";
      errBanner.textContent = "Error: " + String(err);
    }
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = "Open Browser Window";
    }
  }
}

async function closeBrowserWindow() {
  const res = await fetch("/api/browser/close", { method: "POST" });
  const st = await res.json();
  renderHarvesterState(st);
}

async function startHarvest(mode) {
  const downloadConcurrency = downloadSettingsSupported ? readDownloadConcurrency() : undefined;
  if (downloadConcurrency === null) return;
  const rawVal = document.getElementById("harvest-pages").value.trim();
  const pages = rawVal === "" ? 2 : Math.max(0, parseInt(rawVal, 10));
  const rankings_source = getSelectedSourceMode();
  const raidDiffEl = document.getElementById("harvest-raid-difficulty");
  const raid_difficulty = raidDiffEl ? Number(raidDiffEl.value || 0) : 0;
  const res = await fetch("/api/harvester/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      mode,
      pages_per_spec: isNaN(pages) ? 2 : pages,
      rankings_source,
      raid_difficulty,
      download_concurrency: downloadConcurrency,
    }),
  });
  const st = await res.json();
  if (!res.ok) {
    const banner = document.getElementById("harvester-error-banner");
    banner.style.display = "block";
    banner.textContent = st.detail || "Unable to start harvest";
    return;
  }
  const threads = document.getElementById("harvest-download-threads");
  if (threads) threads.dataset.dirty = "";
  renderHarvesterState(st);
}

function readDownloadConcurrency() {
  const input = document.getElementById("harvest-download-threads");
  const raw = input ? input.value.trim() : "";
  const value = Number(raw);
  const valid = /^\d+$/.test(raw) && Number.isSafeInteger(value) && value >= 1;
  if (input) {
    input.setCustomValidity(valid ? "" : "Enter a positive whole number.");
    if (!valid) input.reportValidity();
  }
  return valid ? value : null;
}

async function applyDownloadConcurrency() {
  if (!downloadSettingsSupported || downloadSettingsApplying) return;
  const value = readDownloadConcurrency();
  if (value === null) return;
  const button = document.getElementById("btn-apply-download-threads");
  const feedback = document.getElementById("download-settings-feedback");
  downloadSettingsApplying = true;
  if (button) button.disabled = true;
  if (feedback) feedback.textContent = "Applying…";
  try {
    const response = await fetch("/api/harvester/settings", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ download_concurrency: value }),
    });
    const state = await response.json();
    if (!response.ok) throw new Error(typeof state.detail === "string" ? state.detail : "Unable to apply download workers");
    document.getElementById("harvest-download-threads").dataset.dirty = "";
    renderHarvesterState(state);
    if (feedback) feedback.textContent = "Applied. Free download slots use this worker count.";
  } catch (error) {
    if (feedback) feedback.textContent = String(error.message || error);
  } finally {
    downloadSettingsApplying = false;
    if (button) button.disabled = !downloadSettingsSupported;
  }
}

async function stopHarvest() {
  const res = await fetch("/api/harvester/stop", { method: "POST" });
  const st = await res.json();
  renderHarvesterState(st);
}

async function importSingleReport() {
  const report = document.getElementById("import-report-input").value.trim();
  if (!report) return;
  const btn = document.getElementById("btn-import-report");
  btn.disabled = true;
  btn.textContent = "Importing...";
  const rankings_source = getSelectedSourceMode();
  try {
    const response = await fetch("/api/harvester/import-report", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ report, content_type: "mplus", rankings_source }),
    });
    if (!response.ok) {
      const error = await response.json();
      const banner = document.getElementById("harvester-error-banner");
      banner.style.display = "block";
      banner.textContent = error.detail || "Import failed";
      return;
    }
    await refreshHarvesterStatus();
    await loadFightsList();
  } finally {
    btn.disabled = false;
    btn.textContent = "Import Single Report Now";
  }
}

function formatKeyOrDifficulty(keystoneLevel, difficulty) {
  if (keystoneLevel && Number(keystoneLevel) > 0) {
    return "+" + keystoneLevel;
  }
  const diffNum = Number(difficulty || 0);
  return RAID_DIFFICULTY_NAMES[diffNum] || (diffNum ? `Diff ${diffNum}` : "Normal/Raid");
}

async function loadFightsList(offset = 0) {
  const request = ++fightsRequest;
  const requestedOffset = Math.max(0, Number(offset) || 0);
  const contentType = document.getElementById("filter-content-type").value;
  const encounterId = document.getElementById("filter-encounter").value;
  const spec = document.getElementById("filter-spec").value;
  const minKey = document.getElementById("filter-min-key").value;
  const difficulty = document.getElementById("filter-difficulty")?.value || "";

  const params = new URLSearchParams();
  if (contentType) params.set("content_type", contentType);
  if (encounterId) params.set("encounter_id", encounterId);
  if (spec) params.set("spec", spec);
  if (minKey) params.set("min_key_level", minKey);
  if (difficulty) params.set("difficulty", difficulty);
  params.set("status", document.getElementById("filter-status")?.value || "PACKED");
  params.set("offset", requestedOffset);
  params.set("limit", fightsPageSize);

  const res = await fetch(`/api/fights?${params.toString()}`);
  const fights = await res.json();
  if (request !== fightsRequest || !res.ok) return;
  fightsOffset = requestedOffset;
  const tbody = document.getElementById("fights-tbody");
  tbody.innerHTML = "";

  fights.forEach(f => {
    const tr = document.createElement("tr");
    const durS = Math.round((f.duration_ms || 0) / 1000);
    const parts = (f.participants || []).map(p => `${p.player_name} (${p.spec_name})`).join(", ");
    const sizeKb = f.compressed_bytes ? `${(f.compressed_bytes / 1024).toFixed(1)} KB` : "-";
    const keyDiff = formatKeyOrDifficulty(f.keystone_level, f.difficulty);
    [f.encounter_slug, keyDiff, `${Math.floor(durS / 60)}m ${durS % 60}s`, parts, f.status, sizeKb].forEach(value => {
      const td = document.createElement("td");
      td.textContent = String(value ?? "");
      tr.appendChild(td);
    });
    const actions = document.createElement("td");
    if (f.status === "PACKED") {
      const open = document.createElement("button");
      open.className = "btn-primary";
      open.textContent = "Open Fight";
      open.addEventListener("click", () => openSingleFight(f.fight_uid));
      actions.appendChild(open);
    }
    tr.appendChild(actions);
    tbody.appendChild(tr);
  });
  document.getElementById("btn-fights-prev").disabled = fightsOffset === 0;
  document.getElementById("btn-fights-next").disabled = fights.length < fightsPageSize;
  document.getElementById("fights-page-label").textContent = fights.length
    ? `Fights ${fightsOffset + 1}-${fightsOffset + fights.length}` : "No fights on this page";
}

async function openSingleFight(fightUid) {
  currentFightUid = fightUid;
  const res = await fetch(`/api/fights/${fightUid}/summary`);
  if (!res.ok) {
    alert("Fight file is not packed on disk yet.");
    return;
  }
  currentFightSummary = await res.json();
  const sourceLinks = document.getElementById('fight-source-links');
  if (sourceLinks) {
    sourceLinks.classList.toggle('hidden', !currentFightSummary.source_available);
    document.getElementById('fight-source-manifest').href = `/api/fights/${encodeURIComponent(fightUid)}/sources`;
    document.getElementById('fight-source-events').href = `/api/fights/${encodeURIComponent(fightUid)}/full-events`;
  }
  document.getElementById("nav-fight-btn").disabled = false;
  const keyDiff = formatKeyOrDifficulty(currentFightSummary.keystone_level, currentFightSummary.difficulty);
  document.getElementById("fight-header-info").textContent =
    `${currentFightSummary.name} (${keyDiff}) — Report ${currentFightSummary.report_code} #${currentFightSummary.fight_id}`;

  // Populate global Player and Target filter dropdowns
  await populateFightGlobalFilters(fightUid, currentFightSummary);

  eventsState.offset = 0;
  switchView("fight");
  currentFightTab = "damage";
  document.querySelectorAll(".tab-btn").forEach(b => {
    b.classList.toggle("active", b.dataset.tab === "damage");
  });
  renderFightTab("damage");
}

async function populateFightGlobalFilters(fightUid, summary) {
  const playerSel = document.getElementById("fight-filter-player");
  const targetSel = document.getElementById("fight-filter-target");
  if (!playerSel || !targetSel) return;

  const actors = summary.actors || [];
  const players = actors.filter(a => Boolean(CLASS_COLORS[a.type] || CLASS_COLORS[a.subType]));
  const npcs = actors.filter(a => !CLASS_COLORS[a.type] && a.type !== "Pet");

  playerSel.innerHTML = `<option value="">All Players (${players.length})</option>`;
  players.forEach(p => {
    const opt = document.createElement("option");
    opt.value = String(p.id);
    opt.textContent = `${p.name} (${p.type})`;
    playerSel.appendChild(opt);
  });
  if (npcs.length > 0) {
    const grp = document.createElement("optgroup");
    grp.label = "Bosses & NPCs";
    npcs.slice(0, 80).forEach(n => {
      const opt = document.createElement("option");
      opt.value = String(n.id);
      opt.textContent = `${n.name} (${n.type})`;
      grp.appendChild(opt);
    });
    playerSel.appendChild(grp);
  }

  targetSel.innerHTML = `<option value="">All Targets</option>`;
  try {
    const bRes = await fetch(`/api/fights/${fightUid}/breakdown?mode=damage`);
    if (bRes.ok) {
      const bData = await bRes.json();
      const activeTargets = bData.active_targets || [];
      activeTargets.forEach(t => {
        const opt = document.createElement("option");
        opt.value = String(t.id);
        const dmgM = t.dmg_taken ? `Dmg: ${(t.dmg_taken / 1e6).toFixed(1)}M` : "";
        const healM = t.heal_taken ? `Heal: ${(t.heal_taken / 1e6).toFixed(1)}M` : "";
        const meta = [dmgM, healM].filter(Boolean).join(", ");
        opt.textContent = `${t.name} [${t.type}]${meta ? " — " + meta : ""}`;
        targetSel.appendChild(opt);
      });
    }
  } catch (_) {}
}

function setFightPlayerFilter(actorId) {
  const playerSel = document.getElementById("fight-filter-player");
  if (!playerSel) return;
  playerSel.value = actorId ? String(actorId) : "";
  eventsState.offset = 0;
  if (replayPlayer) replayPlayer.setFocusActor(playerSel.value);
  renderFightTab(currentFightTab);
}

function setFightTargetFilter(targetId) {
  const targetSel = document.getElementById("fight-filter-target");
  if (!targetSel) return;
  targetSel.value = targetId ? String(targetId) : "";
  eventsState.offset = 0;
  renderFightTab(currentFightTab);
}

function refreshWowheadTooltips() {
  if (window.$WowheadPower && typeof window.$WowheadPower.refreshLinks === "function") {
    setTimeout(() => window.$WowheadPower.refreshLinks(), 50);
  }
}

function formatTimeMMSS(ms) {
  const sec = Math.max(0, (ms || 0) / 1000);
  const m = Math.floor(sec / 60);
  const s = (sec % 60).toFixed(2).padStart(5, "0");
  return `${String(m).padStart(2, "0")}:${s}`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, char => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[char]));
}

async function renderFightTab(tab) {
  currentFightTab = tab;
  const area = document.getElementById("tab-content-area");
  const replayBox = document.getElementById("replay-container");
  if (!currentFightSummary) return;

  const selectedPlayerId = document.getElementById("fight-filter-player")?.value || "";
  const selectedTargetId = document.getElementById("fight-filter-target")?.value || "";

  if (tab === "replay2d") {
    area.innerHTML = "";
    replayBox.classList.remove("hidden");
    const res = await fetch(`/api/fights/${currentFightUid}/replay`);
    const replayData = await res.json();
    replayPlayer.loadReplay(replayData, currentFightSummary);
    replayPlayer.setFocusActor(selectedPlayerId || null);
    return;
  }

  replayBox.classList.add("hidden");
  const actorsObjById = {};
  (currentFightSummary.actors || []).forEach(a => {
    actorsObjById[a.id] = a;
  });
  const actorBadge = id => {
    const a = actorsObjById[id];
    if (!a) return id ? `Actor #${escapeHtml(id)}` : "Environment";
    const col = CLASS_COLORS[a.type] || CLASS_COLORS[a.subType] || (a.type === "Boss" ? "#f87171" : "#cbd5e1");
    return `<span style="color:${col}; font-weight:600;">${escapeHtml(a.name)}</span>`;
  };

  const abilities = currentFightSummary.abilities || {};
  const spellNamePlain = id => (abilities[String(id)] ? abilities[String(id)].name : `Spell #${id}`);
  const spellHtml = id => {
    const numId = Number(id);
    if (!numId || numId <= 0) return `<span style="color:#94a3b8;">Melee / Auto</span>`;
    if (numId === 1) return `<span style="color:#e2e8f0;">Melee</span>`;
    const ab = abilities[String(numId)];
    const name = escapeHtml(ab && ab.name ? ab.name : `Spell #${numId}`);
    const iconName = ab && ab.icon ? ab.icon.replace(/\.jpg$/i, "").toLowerCase() : "";
    const iconImg = iconName
      ? `<img src="https://wow.zamimg.com/images/wow/icons/small/${encodeURIComponent(iconName)}.jpg" style="width:16px;height:16px;vertical-align:middle;margin-right:5px;border-radius:3px;" onerror="this.style.display='none'" />`
      : "";
    return `${iconImg}<a href="https://www.wowhead.com/spell=${numId}" target="_blank" style="color:#60a5fa;text-decoration:none;">${name}</a>`;
  };

  if (tab === "damage" || tab === "healing") {
    const mode = tab === "healing" ? "healing" : "damage";
    const rateLabel = mode === "healing" ? "HPS" : "DPS";
    const titleLabel = mode === "healing" ? "Healing Done" : "Damage Done";

    const q = new URLSearchParams({ mode });
    if (selectedPlayerId) q.set("source_id", selectedPlayerId);
    if (selectedTargetId) q.set("target_id", selectedTargetId);

    area.innerHTML = `<div class="card">Loading detailed breakdown...</div>`;
    const res = await fetch(`/api/fights/${currentFightUid}/breakdown?${q.toString()}`);
    const data = await res.json();

    const totalBySource = Math.max(1, (data.by_source || []).reduce((s, r) => s + r.total, 0));
    const totalByAbility = Math.max(1, (data.by_ability || []).reduce((s, r) => s + r.total, 0));
    const totalByTarget = Math.max(1, (data.by_target || []).reduce((s, r) => s + r.total, 0));

    const sourceRows = (data.by_source || []).map(r => {
      const pct = ((r.total / totalBySource) * 100).toFixed(1);
      const isSel = String(r.actor_id) === String(selectedPlayerId);
      const aObj = actorsObjById[r.actor_id] || {};
      const barCol = CLASS_COLORS[aObj.type] || "#3b82f6";
      return `
        <tr style="cursor:pointer; ${isSel ? "background:#1e293b;" : ""}" onclick="setFightPlayerFilter('${isSel ? "" : r.actor_id}')" title="Click to filter by this player">
          <td>${actorBadge(r.actor_id)} <small style="color:#64748b;">(${escapeHtml(aObj.type || "Unit")})</small></td>
          <td>${r.total.toLocaleString()}</td>
          <td><strong>${r.per_sec.toLocaleString()}</strong></td>
          <td>${r.hits} (${r.hits ? Math.round((r.crits / r.hits) * 100) : 0}% crit)</td>
          <td style="min-width:140px;">
            <div style="display:flex;align-items:center;gap:6px;">
              <div style="flex:1;height:8px;background:#0f172a;border-radius:4px;overflow:hidden;">
                <div style="width:${pct}%;height:100%;background:${barCol};"></div>
              </div>
              <span style="font-size:12px;">${pct}%</span>
            </div>
          </td>
        </tr>
      `;
    }).join("");

    const abilityRows = (data.by_ability || []).map(r => {
      const pct = ((r.total / totalByAbility) * 100).toFixed(1);
      const critPct = r.hits ? ((r.crits / r.hits) * 100).toFixed(1) : "0.0";
      return `
        <tr>
          <td>${spellHtml(r.ability_id)}</td>
          <td>${r.total.toLocaleString()}</td>
          <td><strong>${r.per_sec.toLocaleString()}</strong></td>
          <td>${r.hits}</td>
          <td>${r.crits} (${critPct}%)</td>
          <td style="min-width:120px;">
            <div style="display:flex;align-items:center;gap:6px;">
              <div style="flex:1;height:8px;background:#0f172a;border-radius:4px;overflow:hidden;">
                <div style="width:${pct}%;height:100%;background:#3b82f6;"></div>
              </div>
              <span style="font-size:12px;">${pct}%</span>
            </div>
          </td>
        </tr>
      `;
    }).join("");

    const targetRows = (data.by_target || []).map(r => {
      const pct = ((r.total / totalByTarget) * 100).toFixed(1);
      const isSel = String(r.target_id) === String(selectedTargetId);
      const tObj = actorsObjById[r.target_id] || {};
      return `
        <tr style="cursor:pointer; ${isSel ? "background:#1e293b;" : ""}" onclick="setFightTargetFilter('${isSel ? "" : r.target_id}')" title="Click to filter by this target">
          <td>${actorBadge(r.target_id)} <small style="color:#64748b;">[${escapeHtml(tObj.type || "Unit")}]</small></td>
          <td>${r.total.toLocaleString()}</td>
          <td><strong>${r.per_sec.toLocaleString()}</strong></td>
          <td>${r.hits}</td>
          <td style="min-width:120px;">
            <div style="display:flex;align-items:center;gap:6px;">
              <div style="flex:1;height:8px;background:#0f172a;border-radius:4px;overflow:hidden;">
                <div style="width:${pct}%;height:100%;background:#22c55e;"></div>
              </div>
              <span style="font-size:12px;">${pct}%</span>
            </div>
          </td>
        </tr>
      `;
    }).join("");

    area.innerHTML = `
      <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px;">
        <div class="card">
          <h3 style="margin-top:0; font-size:15px; color:#60a5fa;">1. By Player (${titleLabel} — click a player row to filter)</h3>
          <table class="data-table">
            <thead><tr><th>Player</th><th>Total</th><th>${rateLabel}</th><th>Hits (Crit%)</th><th>Share</th></tr></thead>
            <tbody>${sourceRows || '<tr><td colspan="5">No data for the selected filter</td></tr>'}</tbody>
          </table>
        </div>
        <div class="card">
          <h3 style="margin-top:0; font-size:15px; color:#4ade80;">2. By Target (Targets Damaged / Healed — click a target row to filter)</h3>
          <div style="max-height: 360px; overflow-y: auto;">
            <table class="data-table">
              <thead><tr><th>Target</th><th>Total</th><th>${rateLabel}</th><th>Hits</th><th>Share</th></tr></thead>
              <tbody>${targetRows || '<tr><td colspan="5">No data for the selected filter</td></tr>'}</tbody>
            </table>
          </div>
        </div>
      </div>
      <div class="card">
        <h3 style="margin-top:0; font-size:15px; color:#facc15;">
          3. By Ability Breakdown (${selectedPlayerId ? "Player: " + (actorsObjById[selectedPlayerId]?.name || selectedPlayerId) : "All Players"} &rarr; ${selectedTargetId ? "Target: " + (actorsObjById[selectedTargetId]?.name || selectedTargetId) : "All Targets"})
        </h3>
        <table class="data-table">
          <thead><tr><th>Ability (Wowhead)</th><th>Total</th><th>${rateLabel}</th><th>Hits</th><th>Crits</th><th>Share</th></tr></thead>
          <tbody>${abilityRows || '<tr><td colspan="6">No data for the selected filter</td></tr>'}</tbody>
        </table>
      </div>
    `;
    refreshWowheadTooltips();
  } else if (tab === "events") {
    await renderEventsLogTab(area, actorsObjById, actorBadge, spellHtml);
    refreshWowheadTooltips();
  } else if (tab === "casts") {
    const allCasts = [];
    Object.entries(currentFightSummary.cast_timeline || {}).forEach(([aid, list]) => {
      if (selectedPlayerId && String(aid) !== String(selectedPlayerId)) return;
      list.forEach(c => {
        if (selectedTargetId && String(c.target_id || "") !== String(selectedTargetId)) return;
        allCasts.push({ actorId: Number(aid), ...c });
      });
    });
    allCasts.sort((a, b) => a.t_ms - b.t_ms);
    const rows = allCasts.slice(0, 1000).map(c => `
      <tr>
        <td>${formatTimeMMSS(c.t_ms)} <small style="color:#64748b;">(${(c.t_ms / 1000).toFixed(2)}s)</small></td>
        <td>${actorBadge(c.actorId)}</td>
        <td>${spellHtml(c.ability_id)}</td>
        <td>${c.target_id ? actorBadge(c.target_id) : '<span style="color:#64748b;">—</span>'}</td>
        <td>${(c.active_buffs || []).slice(0, 12).map(spellHtml).join(" &bull; ")}</td>
      </tr>
    `).join("");
    area.innerHTML = `
      <div class="card">
        <div style="margin-bottom:8px; color:#94a3b8; font-size:13px;">Showing ${Math.min(1000, allCasts.length)} of ${allCasts.length} casts</div>
        <table class="data-table">
          <thead><tr><th>Time</th><th>Player</th><th>Ability Cast</th><th>Target</th><th>Active Buffs at Cast</th></tr></thead>
          <tbody>${rows || '<tr><td colspan="5">No casts found for the selected filters</td></tr>'}</tbody>
        </table>
      </div>
    `;
    refreshWowheadTooltips();
  } else if (tab === "buffs") {
    const rows = [];
    Object.entries(currentFightSummary.buff_uptimes || {}).forEach(([aid, list]) => {
      if (selectedPlayerId && String(aid) !== String(selectedPlayerId)) return;
      if (selectedTargetId && String(aid) !== String(selectedTargetId)) return;
      list.forEach(b => {
        rows.push(`
          <tr>
            <td>${actorBadge(Number(aid))}</td>
            <td>${spellHtml(b.ability_id)}</td>
            <td style="min-width:180px;">
              <div style="display:flex;align-items:center;gap:8px;">
                <div style="flex:1;height:8px;background:#0f172a;border-radius:4px;overflow:hidden;">
                  <div style="width:${Math.min(100, b.uptime_pct)}%;height:100%;background:#a855f7;"></div>
                </div>
                <strong>${b.uptime_pct}%</strong>
              </div>
            </td>
          </tr>
        `);
      });
    });
    area.innerHTML = `
      <div class="card">
        <table class="data-table">
          <thead><tr><th>Player / Target</th><th>Buff / Aura (Wowhead)</th><th>Uptime %</th></tr></thead>
          <tbody>${rows.join("") || '<tr><td colspan="3">No buffs found for the selected filter</td></tr>'}</tbody>
        </table>
      </div>
    `;
    refreshWowheadTooltips();
  } else if (tab === "builds") {
    renderBuildsTab(area, selectedPlayerId, actorsObjById, actorBadge, spellHtml);
    refreshWowheadTooltips();
  } else if (tab === "deaths") {
    renderDeathsAndInterruptsTab(area, selectedPlayerId, selectedTargetId, actorBadge, spellHtml);
    refreshWowheadTooltips();
  }
}

async function renderEventsLogTab(area, actorsObjById, actorBadge, spellHtml) {
  const selectedPlayerId = document.getElementById("fight-filter-player")?.value || "";
  const selectedTargetId = document.getElementById("fight-filter-target")?.value || "";

  const params = new URLSearchParams();
  params.set("offset", String(eventsState.offset));
  params.set("limit", String(eventsState.limit));
  if (eventsState.eventType && eventsState.eventType !== "all") {
    params.set("event_type", eventsState.eventType);
  }
  if (eventsState.tStartSec !== "" && !isNaN(Number(eventsState.tStartSec))) {
    params.set("t_start", String(Math.floor(Number(eventsState.tStartSec) * 1000)));
  }
  if (eventsState.tEndSec !== "" && !isNaN(Number(eventsState.tEndSec))) {
    params.set("t_end", String(Math.floor(Number(eventsState.tEndSec) * 1000)));
  }
  if (selectedPlayerId) params.set("source_id", selectedPlayerId);
  if (selectedTargetId) params.set("target_id", selectedTargetId);

  const res = await fetch(`/api/fights/${currentFightUid}/events?${params.toString()}`);
  const data = await res.json();
  const totalCount = data.count || 0;
  const events = data.events || [];

  const describeEvent = ev => {
    const src = actorBadge(ev.sourceID);
    const tgt = actorBadge(ev.targetID);
    const sp = spellHtml(ev.abilityGameID);
    const amt = Number(ev.amount || 0).toLocaleString();
    const ovk = Number(ev.overkill || 0);
    const abs = Number(ev.absorbed || 0);
    const isCrit = ev.hitType === 2;
    const critTag = isCrit ? ` <strong style="color:#facc15;">(Crit!)</strong>` : "";
    const extraParts = [];
    if (ovk > 0) extraParts.push(`Overkill/Overheal: ${ovk.toLocaleString()}`);
    if (abs > 0) extraParts.push(`Absorbed: ${abs.toLocaleString()}`);
    const extraStr = extraParts.length ? ` <small style="color:#94a3b8;">[${extraParts.join(", ")}]</small>` : "";

    if (ev.type === "damage") {
      return `${src} dealt <strong style="color:#f87171;">${amt}</strong> damage${critTag} to ${tgt} with ${sp}${extraStr}`;
    }
    if (ev.type === "heal" || ev.type === "absorbed") {
      return `${src} healed ${tgt} for <strong style="color:#4ade80;">${amt}</strong>${critTag} with ${sp}${extraStr}`;
    }
    if (ev.type === "cast" || ev.type === "begincast") {
      return `${src} cast ${sp}${ev.targetID ? ` on ${tgt}` : ""}`;
    }
    if (ev.type === "applybuff" || ev.type === "refreshbuff" || ev.type === "applybuffstack") {
      return `${tgt} gained buff ${sp} (from ${src})`;
    }
    if (ev.type === "removebuff") {
      return `Buff ${sp} faded from ${tgt}`;
    }
    if (ev.type === "applydebuff" || ev.type === "refreshdebuff" || ev.type === "applydebuffstack") {
      return `${src} applied debuff ${sp} to ${tgt}`;
    }
    if (ev.type === "removedebuff") {
      return `Debuff ${sp} faded from ${tgt}`;
    }
    if (ev.type === "interrupt") {
      return `${src} <strong style="color:#38bdf8;">interrupted</strong> ${tgt} with ${sp}`;
    }
    if (ev.type === "death" || ev.type === "instakill") {
      return `<strong style="color:#ef4444;">💀 ${tgt} died!</strong> ${ev.abilityGameID ? `(Killing blow: ${sp})` : ""}`;
    }
    if (ev.type === "summon") {
      return `${src} summoned ${tgt} (${sp})`;
    }
    return `${src} &rarr; ${tgt}: ${sp} (${ev.type})`;
  };

  const badgeColor = t => {
    if (t === "damage") return "#ef4444";
    if (t === "heal" || t === "absorbed") return "#22c55e";
    if (t === "cast" || t === "begincast") return "#3b82f6";
    if (t.includes("buff")) return "#a855f7";
    if (t === "death" || t === "interrupt") return "#f59e0b";
    return "#64748b";
  };

  const rowsHtml = events.map(ev => {
    const hpPct = ev.maxHitPoints > 0 ? `${Math.round((ev.hitPoints / ev.maxHitPoints) * 100)}%` : "—";
    return `
      <tr>
        <td style="white-space:nowrap;">${formatTimeMMSS(ev.timestamp)} <small style="color:#64748b;">(${(ev.timestamp / 1000).toFixed(2)}s)</small></td>
        <td><span style="background:${badgeColor(ev.type)}22; color:${badgeColor(ev.type)}; border:1px solid ${badgeColor(ev.type)}55; padding:2px 6px; border-radius:4px; font-size:11px; font-weight:700;">${ev.type.toUpperCase()}</span></td>
        <td>${actorBadge(ev.sourceID)}</td>
        <td>${spellHtml(ev.abilityGameID)}</td>
        <td>${ev.targetID ? actorBadge(ev.targetID) : "—"}</td>
        <td>${describeEvent(ev)}</td>
        <td style="text-align:right; color:#94a3b8;">${hpPct}</td>
      </tr>
    `;
  }).join("");

  const startIdx = totalCount === 0 ? 0 : eventsState.offset + 1;
  const endIdx = Math.min(totalCount, eventsState.offset + events.length);

  area.innerHTML = `
    <div class="card">
      <div class="filter-bar" style="align-items:center; margin-bottom:12px;">
        <label style="font-size:13px;">Event Type:
          <select id="ev-type-select" style="margin-left:6px;">
            <option value="all" ${eventsState.eventType === "all" ? "selected" : ""}>All Events</option>
            <option value="damage" ${eventsState.eventType === "damage" ? "selected" : ""}>Damage</option>
            <option value="heal" ${eventsState.eventType === "heal" ? "selected" : ""}>Healing</option>
            <option value="cast" ${eventsState.eventType === "cast" ? "selected" : ""}>Casts</option>
            <option value="buff" ${eventsState.eventType === "buff" ? "selected" : ""}>Buffs &amp; Debuffs (Auras)</option>
            <option value="death" ${eventsState.eventType === "death" ? "selected" : ""}>Interrupts &amp; Deaths</option>
          </select>
        </label>
        <label style="font-size:13px;">From (sec):
          <input type="number" id="ev-t-start" value="${eventsState.tStartSec}" placeholder="0" min="0" style="width:80px; margin-left:4px;" />
        </label>
        <label style="font-size:13px;">To (sec):
          <input type="number" id="ev-t-end" value="${eventsState.tEndSec}" placeholder="End" min="0" style="width:80px; margin-left:4px;" />
        </label>
        <button id="ev-apply-btn" class="btn-primary">Filter</button>
        <div style="margin-left:auto; display:flex; align-items:center; gap:8px;">
          <span style="font-size:13px; color:#94a3b8;">Events <strong>${startIdx.toLocaleString()}–${endIdx.toLocaleString()}</strong> of <strong>${totalCount.toLocaleString()}</strong></span>
          <button id="ev-prev-btn" class="nav-btn" ${eventsState.offset <= 0 ? "disabled" : ""}>&larr; Prev 500</button>
          <button id="ev-next-btn" class="nav-btn" ${eventsState.offset + eventsState.limit >= totalCount ? "disabled" : ""}>Next 500 &rarr;</button>
        </div>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Type</th>
            <th>Source</th>
            <th>Ability</th>
            <th>Target</th>
            <th>Event Details (Source &rarr; Amount &rarr; Target &rarr; Ability)</th>
            <th>Target HP</th>
          </tr>
        </thead>
        <tbody>${rowsHtml || '<tr><td colspan="7">No events found for the selected filters</td></tr>'}</tbody>
      </table>
    </div>
  `;

  document.getElementById("ev-apply-btn")?.addEventListener("click", () => {
    eventsState.eventType = document.getElementById("ev-type-select").value;
    eventsState.tStartSec = document.getElementById("ev-t-start").value.trim();
    eventsState.tEndSec = document.getElementById("ev-t-end").value.trim();
    eventsState.offset = 0;
    renderFightTab("events");
  });
  document.getElementById("ev-type-select")?.addEventListener("change", e => {
    eventsState.eventType = e.target.value;
    eventsState.offset = 0;
    renderFightTab("events");
  });
  document.getElementById("ev-prev-btn")?.addEventListener("click", () => {
    eventsState.offset = Math.max(0, eventsState.offset - eventsState.limit);
    renderFightTab("events");
  });
  document.getElementById("ev-next-btn")?.addEventListener("click", () => {
    eventsState.offset += eventsState.limit;
    renderFightTab("events");
  });
}

function renderBuildsTab(area, selectedPlayerId, actorsObjById, actorBadge, spellHtml) {
  const builds = currentFightSummary.combatant_builds || {};
  const entries = Object.entries(builds).filter(([aid]) => !selectedPlayerId || String(aid) === String(selectedPlayerId));

  if (entries.length === 0) {
    area.innerHTML = `<div class="card">No build or equipment data (combatantinfo) available for the selected player.</div>`;
    return;
  }

  const cardsHtml = entries.map(([aid, b]) => {
    const actor = actorsObjById[Number(aid)] || { name: `Actor #${aid}`, type: "Player" };
    const talentsHtml = (b.talents || []).map(t => {
      const tid = t.id || t.guid || 0;
      if (!tid) return "";
      const iconName = (t.icon || "").replace(/\.jpg$/i, "").toLowerCase();
      const iconImg = iconName
        ? `<img src="https://wow.zamimg.com/images/wow/icons/medium/${iconName}.jpg" style="width:22px;height:22px;vertical-align:middle;margin-right:6px;border-radius:4px;" onerror="this.style.display='none'" />`
        : "";
      return `<div style="background:#18202c; padding:6px 10px; border-radius:6px; border:1px solid #2a364b;">
        ${iconImg}<a href="https://www.wowhead.com/spell=${tid}" target="_blank" style="color:#60a5fa; font-weight:600; text-decoration:none;">Talent #${tid}</a>
      </div>`;
    }).join("");

    const gearRows = (b.gear || []).map((item, idx) => {
      const itemId = Number(item.id || 0);
      if (!itemId) return "";
      const slotName = GEAR_SLOTS[idx] || `Slot ${idx + 1}`;
      const qColor = QUALITY_COLORS[item.quality] || "#a335ee";
      const iconName = (item.icon || "").replace(/\.jpg$/i, "").toLowerCase();
      const iconImg = iconName
        ? `<img src="https://wow.zamimg.com/images/wow/icons/small/${iconName}.jpg" style="width:18px;height:18px;vertical-align:middle;margin-right:6px;border-radius:3px;" onerror="this.style.display='none'" />`
        : "";

      const whParams = [];
      if (item.bonusIDs && item.bonusIDs.length) whParams.push(`bonus=${item.bonusIDs.join(":")}`);
      if (item.permanentEnchant) whParams.push(`ench=${item.permanentEnchant}`);
      if (item.gems && item.gems.length) whParams.push(`gems=${item.gems.map(g => g.id).join(":")}`);
      const whUrl = `https://www.wowhead.com/item=${itemId}${whParams.length ? "&" + whParams.join("&") : ""}`;

      const gemsHtml = (item.gems || []).map(g =>
        `<a href="https://www.wowhead.com/item=${g.id}" target="_blank" style="color:#38bdf8;font-size:12px;margin-right:6px;">[Gem #${g.id}]</a>`
      ).join("");

      return `
        <tr>
          <td style="color:#94a3b8; font-size:12px;">${slotName}</td>
          <td>
            ${iconImg}<a href="${whUrl}" target="_blank" style="color:${qColor}; font-weight:600; text-decoration:none;">Item #${itemId}</a>
          </td>
          <td><strong>${item.itemLevel || "—"}</strong></td>
          <td>${gemsHtml || "—"}</td>
          <td style="color:#94a3b8; font-size:12px;">${item.permanentEnchant ? `Enchant #${item.permanentEnchant}` : "—"}</td>
        </tr>
      `;
    }).filter(Boolean).join("");

    const artifactHtml = (b.artifact || []).map(tr => {
      const spellId = tr.spellID || tr.traitID || 0;
      if (!spellId) return "";
      const iconName = (tr.icon || "").replace(/\.jpg$/i, "").toLowerCase();
      const iconImg = iconName
        ? `<img src="https://wow.zamimg.com/images/wow/icons/small/${iconName}.jpg" style="width:16px;height:16px;vertical-align:middle;margin-right:4px;border-radius:3px;" onerror="this.style.display='none'" />`
        : "";
      return `<span style="background:#18202c; padding:4px 8px; border-radius:4px; border:1px solid #2a364b; font-size:12px;">
        ${iconImg}<a href="https://www.wowhead.com/spell=${spellId}" target="_blank" style="color:#e6cc80; text-decoration:none;">Trait #${spellId}</a> (Rank ${tr.rank || 1})
      </span>`;
    }).filter(Boolean).join("");

    return `
      <div class="card" style="margin-bottom:16px;">
        <h3 style="margin-top:0; border-bottom:1px solid #2a364b; padding-bottom:8px;">
          ${actorBadge(Number(aid))} <span style="color:#94a3b8; font-size:13px; font-weight:normal;">(${actor.type} — Spec ID: ${b.spec_id || "—"})</span>
        </h3>
        <div style="margin-bottom:12px;">
          <h4 style="margin:0 0 8px 0; font-size:13px; color:#94a3b8;">Talents (Wowhead):</h4>
          <div style="display:flex; flex-wrap:wrap; gap:8px;">${talentsHtml || "No data"}</div>
        </div>
        <div style="margin-bottom:12px;">
          <h4 style="margin:0 0 8px 0; font-size:13px; color:#94a3b8;">Equipped Gear (Wowhead Links):</h4>
          <table class="data-table">
            <thead><tr><th>Slot</th><th>Item (Wowhead)</th><th>ilvl</th><th>Gems</th><th>Enchant</th></tr></thead>
            <tbody>${gearRows || '<tr><td colspan="5">No items</td></tr>'}</tbody>
          </table>
        </div>
        ${artifactHtml ? `<div><h4 style="margin:0 0 8px 0; font-size:13px; color:#94a3b8;">Artifact Traits:</h4><div style="display:flex; flex-wrap:wrap; gap:6px;">${artifactHtml}</div></div>` : ""}
      </div>
    `;
  }).join("");

  area.innerHTML = cardsHtml;
}

function renderDeathsAndInterruptsTab(area, selectedPlayerId, selectedTargetId, actorBadge, spellHtml) {
  const interrupts = (currentFightSummary.interrupts || []).filter(i => {
    if (selectedPlayerId && String(i.source_id) !== String(selectedPlayerId)) return false;
    if (selectedTargetId && String(i.target_id) !== String(selectedTargetId)) return false;
    return true;
  });

  const deaths = (currentFightSummary.deaths || []).filter(d => {
    if (selectedPlayerId && String(d.target_id) !== String(selectedPlayerId)) return false;
    if (selectedTargetId && String(d.target_id) !== String(selectedTargetId)) return false;
    return true;
  });

  const intRows = interrupts.map(i => `
    <tr>
      <td>${formatTimeMMSS(i.t_ms)} <small style="color:#64748b;">(${(i.t_ms / 1000).toFixed(2)}s)</small></td>
      <td>${actorBadge(i.source_id)}</td>
      <td>${spellHtml(i.ability_id)}</td>
      <td>${actorBadge(i.target_id)}</td>
      <td>${spellHtml(i.interrupted_ability_id)}</td>
    </tr>
  `).join("");

  const deathCards = deaths.map(d => {
    const recRows = (d.recent_events || []).slice(-15).map(re => {
      const isDmg = re.type === "damage";
      const amtCol = isDmg ? "#f87171" : "#4ade80";
      const sign = isDmg ? "-" : "+";
      return `
        <tr>
          <td>${formatTimeMMSS(re.t_ms)}</td>
          <td>${actorBadge(re.source_id)}</td>
          <td>${spellHtml(re.ability_id)}</td>
          <td style="color:${amtCol}; font-weight:600;">${sign}${Number(re.amount || 0).toLocaleString()}</td>
        </tr>
      `;
    }).join("");

    return `
      <div style="background:#141b26; border:1px solid #2a364b; border-radius:6px; padding:10px; margin-bottom:10px;">
        <div style="margin-bottom:8px;">
          <strong style="color:#ef4444;">💀 ${formatTimeMMSS(d.t_ms)}</strong> — Died: ${actorBadge(d.target_id)}
          (Killing Blow: ${d.killing_ability_id ? spellHtml(d.killing_ability_id) : "Unknown / Melee"})
        </div>
        <details>
          <summary style="cursor:pointer; color:#60a5fa; font-size:13px;">Show last 15 seconds before death (${(d.recent_events || []).length} events)</summary>
          <table class="data-table" style="margin-top:8px;">
            <thead><tr><th>Time</th><th>Source</th><th>Ability</th><th>Damage / Heal</th></tr></thead>
            <tbody>${recRows || '<tr><td colspan="4">No records</td></tr>'}</tbody>
          </table>
        </details>
      </div>
    `;
  }).join("");

  area.innerHTML = `
    <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 16px;">
      <div class="card">
        <h3 style="margin-top:0; color:#38bdf8;">Interrupts (${interrupts.length})</h3>
        <table class="data-table">
          <thead><tr><th>Time</th><th>Interrupted By</th><th>Interrupt Ability</th><th>Target</th><th>Interrupted Spell</th></tr></thead>
          <tbody>${intRows || '<tr><td colspan="5">No interrupts</td></tr>'}</tbody>
        </table>
      </div>
      <div class="card">
        <h3 style="margin-top:0; color:#ef4444;">Deaths (${deaths.length})</h3>
        ${deathCards || '<div style="color:#94a3b8;">No deaths for the selected filters</div>'}
      </div>
    </div>
  `;
}

window.openSingleFight = openSingleFight;
window.setFightPlayerFilter = setFightPlayerFilter;
window.setFightTargetFilter = setFightTargetFilter;
window.addEventListener("DOMContentLoaded", initApp);
