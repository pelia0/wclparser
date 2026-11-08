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

  await loadCatalog();
  await refreshAuthStatus();
  await loadFightsList();
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
  cat.mplus_dungeons.forEach(d => {
    const opt = document.createElement("option");
    opt.value = d.id;
    opt.textContent = d.name;
    encSel.appendChild(opt);
  });
  const specSel = document.getElementById("filter-spec");
  cat.specs.forEach(s => {
    const opt = document.createElement("option");
    opt.value = s.spec;
    opt.textContent = `${s.class} - ${s.spec}`;
    specSel.appendChild(opt);
  });
}

async function refreshAuthStatus() {
  const res = await fetch("/api/auth/status");
  const st = await res.json();
  const badge = document.getElementById("auth-badge");
  badge.textContent = st.authenticated ? `Authenticated (${st.email})` : "Not Authenticated";
  document.getElementById("auth-status-box").textContent = JSON.stringify(st, null, 2);
}

async function submitAuth() {
  const payload = {
    email: document.getElementById("auth-email").value,
    password: document.getElementById("auth-password").value,
    client_id: document.getElementById("auth-client-id").value,
    client_secret: document.getElementById("auth-client-secret").value,
    wcl_session_cookie: document.getElementById("auth-cookie").value,
  };
  await fetch("/api/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  await refreshAuthStatus();
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
    tr.innerHTML = `
      <td>${f.encounter_slug}</td>
      <td>+${f.keystone_level || f.difficulty}</td>
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
  document.getElementById("fight-header-info").textContent =
    `${currentFightSummary.name} (+${currentFightSummary.keystone_level}) — Report ${currentFightSummary.report_code} #${currentFightSummary.fight_id}`;
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
