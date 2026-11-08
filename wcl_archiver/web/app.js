let currentFightSummary = null;
let currentFightUid = null;
let replayPlayer = null;

async function initApp() {
  replayPlayer = new window.Replay2DPlayer("replay-canvas", "replay-scrubber", "replay-time-label");
  document.getElementById("replay-play").addEventListener("click", () => replayPlayer.togglePlay());
  document.getElementById("replay-speed").addEventListener("change", e => {
    replayPlayer.speed = Number(e.target.value);
  });

  document.querySelectorAll(".nav-btn").forEach(btn => {
    btn.addEventListener("click", () => switchView(btn.dataset.view));
  });

  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      renderFightTab(btn.dataset.tab);
    });
  });

  document.getElementById("btn-search").addEventListener("click", loadFightsList);
  document.getElementById("btn-save-auth").addEventListener("click", submitAuth);
  document.getElementById("btn-harvest-mplus").addEventListener("click", () => startHarvest("mplus_s5"));
  document.getElementById("btn-harvest-raids").addEventListener("click", () => startHarvest("raids"));
  document.getElementById("btn-harvest-stop").addEventListener("click", stopHarvest);
  document.getElementById("btn-import-report").addEventListener("click", importSingleReport);

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
  const errBanner = document.getElementById("harvester-error-banner");
  if (errBanner) {
    if (st.last_error) {
      errBanner.style.display = "block";
      errBanner.textContent = st.last_error;
    } else {
      errBanner.style.display = "none";
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

async function startHarvest(mode) {
  const rawVal = document.getElementById("harvest-pages").value.trim();
  const pages = rawVal === "" ? 0 : Math.max(0, parseInt(rawVal, 10));
  const res = await fetch("/api/harvester/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode, pages_per_spec: isNaN(pages) ? 0 : pages }),
  });
  const st = await res.json();
  renderHarvesterState(st);
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
  try {
    await fetch("/api/harvester/import-report", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ report, content_type: "mplus" }),
    });
    await refreshHarvesterStatus();
    await loadFightsList();
  } finally {
    btn.disabled = false;
    btn.textContent = "Import Single Report Now";
  }
}

async function loadFightsList() {
  const contentType = document.getElementById("filter-content-type").value;
  const encounterId = document.getElementById("filter-encounter").value;
  const spec = document.getElementById("filter-spec").value;
  const minKey = document.getElementById("filter-min-key").value;

  const params = new URLSearchParams();
  if (contentType) params.set("content_type", contentType);
  if (encounterId) params.set("encounter_id", encounterId);
  if (spec) params.set("spec", spec);
  if (minKey) params.set("min_key_level", minKey);

  const res = await fetch(`/api/fights?${params.toString()}`);
  const fights = await res.json();
  const tbody = document.getElementById("fights-tbody");
  tbody.innerHTML = "";

  fights.forEach(f => {
    const tr = document.createElement("tr");
    const durS = Math.round((f.duration_ms || 0) / 1000);
    const parts = (f.participants || []).map(p => `${p.player_name} (${p.spec_name})`).join(", ");
    const sizeKb = f.compressed_bytes ? `${(f.compressed_bytes / 1024).toFixed(1)} KB` : "Queued";
    const keyDiff = f.keystone_level ? '+' + f.keystone_level : 'Diff ' + f.difficulty;
    tr.innerHTML = `
      <td>${f.encounter_slug}</td>
      <td>${keyDiff}</td>
      <td>${Math.floor(durS / 60)}m ${durS % 60}s</td>
      <td>${parts}</td>
      <td>${sizeKb}</td>
      <td><button class="btn-primary" onclick="openSingleFight('${f.fight_uid}')">Open Fight</button></td>
    `;
    tbody.appendChild(tr);
  });
}

async function openSingleFight(fightUid) {
  currentFightUid = fightUid;
  const res = await fetch(`/api/fights/${fightUid}/summary`);
  if (!res.ok) {
    alert("Fight file is not packed on disk yet.");
    return;
  }
  currentFightSummary = await res.json();
  document.getElementById("nav-fight-btn").disabled = false;
  const keyDiff = currentFightSummary.keystone_level
    ? '+' + currentFightSummary.keystone_level
    : 'Diff ' + (currentFightSummary.difficulty || 0);
  document.getElementById("fight-header-info").textContent =
    `${currentFightSummary.name} (${keyDiff}) — Report ${currentFightSummary.report_code} #${currentFightSummary.fight_id}`;
  switchView("fight");
  renderFightTab("damage");
}

async function renderFightTab(tab) {
  const area = document.getElementById("tab-content-area");
  const replayBox = document.getElementById("replay-container");
  if (!currentFightSummary) return;

  if (tab === "replay2d") {
    area.innerHTML = "";
    replayBox.classList.remove("hidden");
    const res = await fetch(`/api/fights/${currentFightUid}/replay`);
    const replayData = await res.json();
    replayPlayer.loadReplay(replayData, currentFightSummary);
    return;
  }

  replayBox.classList.add("hidden");
  const actorsById = {};
  (currentFightSummary.actors || []).forEach(a => { actorsById[a.id] = a.name; });
  const abilities = currentFightSummary.abilities || {};
  const spellName = id => (abilities[String(id)] ? abilities[String(id)].name : `Spell #${id}`);

  if (tab === "damage") {
    const rows = (currentFightSummary.damage_done || []).map(r => `
      <tr>
        <td>${actorsById[r.actor_id] || r.actor_id}</td>
        <td>${r.total.toLocaleString()}</td>
        <td>${r.dps.toLocaleString()}</td>
      </tr>
    `).join("");
    area.innerHTML = `<table class="data-table"><thead><tr><th>Actor</th><th>Total Damage</th><th>DPS</th></tr></thead><tbody>${rows}</tbody></table>`;
  } else if (tab === "healing") {
    const rows = (currentFightSummary.healing_done || []).map(r => `
      <tr>
        <td>${actorsById[r.actor_id] || r.actor_id}</td>
        <td>${r.total.toLocaleString()}</td>
        <td>${r.hps.toLocaleString()}</td>
      </tr>
    `).join("");
    area.innerHTML = `<table class="data-table"><thead><tr><th>Actor</th><th>Total Healing</th><th>HPS</th></tr></thead><tbody>${rows}</tbody></table>`;
  } else if (tab === "casts") {
    const allCasts = [];
    Object.entries(currentFightSummary.cast_timeline || {}).forEach(([aid, list]) => {
      list.forEach(c => allCasts.push({ actor: actorsById[aid] || aid, ...c }));
    });
    allCasts.sort((a, b) => a.t_ms - b.t_ms);
    const rows = allCasts.slice(0, 500).map(c => `
      <tr>
        <td>${(c.t_ms / 1000).toFixed(2)}s</td>
        <td>${c.actor}</td>
        <td>${spellName(c.ability_id)}</td>
        <td>${(c.active_buffs || []).map(spellName).join(", ")}</td>
      </tr>
    `).join("");
    area.innerHTML = `<table class="data-table"><thead><tr><th>Time</th><th>Player</th><th>Ability Cast</th><th>Active Buffs at Cast</th></tr></thead><tbody>${rows}</tbody></table>`;
  } else if (tab === "buffs") {
    const rows = [];
    Object.entries(currentFightSummary.buff_uptimes || {}).forEach(([aid, list]) => {
      list.forEach(b => rows.push(`<tr><td>${actorsById[aid] || aid}</td><td>${spellName(b.ability_id)}</td><td>${b.uptime_pct}%</td></tr>`));
    });
    area.innerHTML = `<table class="data-table"><thead><tr><th>Player</th><th>Buff / Aura</th><th>Uptime %</th></tr></thead><tbody>${rows.join("")}</tbody></table>`;
  } else if (tab === "builds") {
    area.innerHTML = `<pre class="card">${JSON.stringify(currentFightSummary.combatant_builds || {}, null, 2)}</pre>`;
  } else if (tab === "deaths") {
    area.innerHTML = `<pre class="card">${JSON.stringify({ interrupts: currentFightSummary.interrupts, deaths: currentFightSummary.deaths }, null, 2)}</pre>`;
  }
}

window.openSingleFight = openSingleFight;
window.addEventListener("DOMContentLoaded", initApp);
