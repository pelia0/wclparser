const WOW_CLASS_COLORS = {
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
  Player: "#38bdf8",
};

function isPlayerActor(actor) {
  if (!actor) return false;
  return Boolean(WOW_CLASS_COLORS[actor.type] || WOW_CLASS_COLORS[actor.subType]);
}

class Replay2DPlayer {
  constructor(canvasId, scrubberId, timeLabelId) {
    this.canvas = document.getElementById(canvasId);
    this.ctx = this.canvas.getContext("2d");
    this.scrubber = document.getElementById(scrubberId);
    this.timeLabel = document.getElementById(timeLabelId);
    this.frames = [];
    this.framesByActor = {};
    this.playerTrail = [];
    this.actors = {};
    this.durationMs = 1;
    this.currentMs = 0;
    this.playing = false;
    this.speed = 1.0;
    this.cameraMode = "follow"; // "follow" | "full"
    this.zoom = 1.0;
    this.panX = 0;
    this.panY = 0;
    this.focusActorId = null;
    this.bounds = { minX: -1000, maxX: 1000, minY: -1000, maxY: 1000 };
    this._dragging = false;
    this._dragStart = { x: 0, y: 0 };
    this._bindEvents();
  }

  _bindEvents() {
    this.scrubber.addEventListener("input", () => {
      this.currentMs = (Number(this.scrubber.value) / 1000) * this.durationMs;
      this.renderFrame();
    });

    const camSel = document.getElementById("replay-camera-mode");
    if (camSel) {
      camSel.addEventListener("change", e => {
        this.cameraMode = e.target.value;
        this.panX = 0;
        this.panY = 0;
        this.zoom = 1.0;
        this.renderFrame();
      });
    }

    this.canvas.addEventListener("wheel", e => {
      e.preventDefault();
      const factor = e.deltaY < 0 ? 1.2 : 1 / 1.2;
      this.zoom = Math.max(0.2, Math.min(25.0, this.zoom * factor));
      this.renderFrame();
    }, { passive: false });

    this.canvas.addEventListener("mousedown", e => {
      this._dragging = true;
      this._dragStart = { x: e.clientX, y: e.clientY };
      this.canvas.style.cursor = "grabbing";
    });

    window.addEventListener("mousemove", e => {
      if (!this._dragging) return;
      const dx = e.clientX - this._dragStart.x;
      const dy = e.clientY - this._dragStart.y;
      this._dragStart = { x: e.clientX, y: e.clientY };
      this.panX += dx;
      this.panY += dy;
      this.renderFrame();
    });

    window.addEventListener("mouseup", () => {
      if (this._dragging) {
        this._dragging = false;
        this.canvas.style.cursor = "grab";
      }
    });
  }

  setFocusActor(actorId) {
    this.focusActorId = actorId ? Number(actorId) : null;
    this.renderFrame();
  }

  loadReplay(replayData, summaryData) {
    this.playing = false;
    this.frames = replayData.frames || [];
    this.durationMs = Math.max(1, summaryData.duration_ms || 1);
    this.actors = {};
    (summaryData.actors || []).forEach(a => { this.actors[a.id] = a; });

    this.framesByActor = {};
    this.playerTrail = [];
    let lastTrailT = -5000;

    if (this.frames.length > 0) {
      let minX = this.frames[0].x;
      let maxX = this.frames[0].x;
      let minY = this.frames[0].y;
      let maxY = this.frames[0].y;

      for (let i = 0; i < this.frames.length; i++) {
        const f = this.frames[i];
        if (!f.x && !f.y) continue;
        if (f.x < minX) minX = f.x;
        if (f.x > maxX) maxX = f.x;
        if (f.y < minY) minY = f.y;
        if (f.y > maxY) maxY = f.y;

        if (!this.framesByActor[f.actor]) {
          this.framesByActor[f.actor] = [];
        }
        this.framesByActor[f.actor].push(f);

        const actor = this.actors[f.actor];
        if (isPlayerActor(actor) && f.t - lastTrailT >= 1500) {
          this.playerTrail.push({ t: f.t, x: f.x, y: f.y });
          lastTrailT = f.t;
        }
      }
      this.bounds = {
        minX: minX - 500,
        maxX: maxX + 500,
        minY: minY - 500,
        maxY: maxY + 500,
      };
      this.currentMs = this.frames[0].t || 0;
    } else {
      this.currentMs = 0;
    }

    this.zoom = 1.0;
    this.panX = 0;
    this.panY = 0;
    this.scrubber.value = String(Math.floor((this.currentMs / this.durationMs) * 1000));
    this.renderFrame();
  }

  togglePlay() {
    this.playing = !this.playing;
    if (this.playing) {
      if (this.currentMs >= this.durationMs) {
        this.currentMs = this.frames.length > 0 ? this.frames[0].t : 0;
      }
      this._lastTick = performance.now();
      requestAnimationFrame(ts => this._loop(ts));
    }
  }

  _loop(ts) {
    if (!this.playing) return;
    const dt = (ts - this._lastTick) * this.speed;
    this._lastTick = ts;
    this.currentMs = Math.min(this.durationMs, this.currentMs + dt);
    this.scrubber.value = String(Math.floor((this.currentMs / this.durationMs) * 1000));
    this.renderFrame();
    if (this.currentMs < this.durationMs) {
      requestAnimationFrame(nextTs => this._loop(nextTs));
    } else {
      this.playing = false;
    }
  }

  _sampleActorAt(actorId, tMs, isPlayer) {
    const list = this.framesByActor[actorId];
    if (!list || list.length === 0) return null;

    if (tMs <= list[0].t) {
      if (isPlayer) {
        return { x: list[0].x, y: list[0].y, hp_pct: list[0].hp_pct, t: list[0].t };
      }
      return list[0].t - tMs <= 5000 ? list[0] : null;
    }

    let lo = 0;
    let hi = list.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (list[mid].t <= tMs) lo = mid;
      else hi = mid - 1;
    }

    const prev = list[lo];
    const next = lo + 1 < list.length ? list[lo + 1] : null;

    if (!isPlayer) {
      const age = tMs - prev.t;
      if (age > 15000 || prev.hp_pct <= 0) return null;
    }

    if (next && next.t > prev.t && next.t - prev.t <= 8000) {
      const alpha = Math.max(0, Math.min(1, (tMs - prev.t) / (next.t - prev.t)));
      return {
        x: prev.x + (next.x - prev.x) * alpha,
        y: prev.y + (next.y - prev.y) * alpha,
        hp_pct: prev.hp_pct,
        t: tMs,
      };
    }
    return { x: prev.x, y: prev.y, hp_pct: prev.hp_pct, t: prev.t };
  }

  _formatTime(ms) {
    const totalSec = Math.max(0, ms / 1000);
    const m = Math.floor(totalSec / 60);
    const s = (totalSec % 60).toFixed(1).padStart(4, "0");
    return `${String(m).padStart(2, "0")}:${s}`;
  }

  renderFrame() {
    const ctx = this.ctx;
    const w = this.canvas.width;
    const h = this.canvas.height;
    ctx.clearRect(0, 0, w, h);

    this.timeLabel.textContent = `${this._formatTime(this.currentMs)} / ${this._formatTime(this.durationMs)}`;

    if (this.frames.length === 0) {
      ctx.fillStyle = "#94a3b8";
      ctx.font = "15px sans-serif";
      ctx.fillText("No (x, y) coordinates found in this log for 2D replay.", 40, 60);
      return;
    }

    const playerUnits = [];
    const enemyUnits = [];

    for (const aidStr of Object.keys(this.framesByActor)) {
      const aid = Number(aidStr);
      const actor = this.actors[aid] || { id: aid, name: `#${aid}`, type: "NPC" };
      if (actor.type === "Pet") continue;
      const isPlayer = isPlayerActor(actor);
      const sample = this._sampleActorAt(aid, this.currentMs, isPlayer);
      if (!sample) continue;
      const unit = { id: aid, actor, ...sample, isPlayer };
      if (isPlayer) playerUnits.push(unit);
      else enemyUnits.push(unit);
    }

    let viewMinX = this.bounds.minX;
    let viewMaxX = this.bounds.maxX;
    let viewMinY = this.bounds.minY;
    let viewMaxY = this.bounds.maxY;

    if (this.cameraMode === "follow" && (playerUnits.length > 0 || enemyUnits.length > 0)) {
      const refUnits = this.focusActorId
        ? playerUnits.filter(u => u.id === this.focusActorId)
        : playerUnits;
      const activeList = refUnits.length > 0 ? refUnits : (playerUnits.length > 0 ? playerUnits : enemyUnits);

      let cx = 0;
      let cy = 0;
      activeList.forEach(u => {
        cx += u.x;
        cy += u.y;
      });
      cx /= activeList.length;
      cy /= activeList.length;

      let halfSpan = 4200 / this.zoom;
      activeList.forEach(u => {
        halfSpan = Math.max(halfSpan, Math.abs(u.x - cx) * 1.35, Math.abs(u.y - cy) * 1.35);
      });
      const aspect = w / Math.max(1, h);
      viewMinX = cx - halfSpan * aspect;
      viewMaxX = cx + halfSpan * aspect;
      viewMinY = cy - halfSpan;
      viewMaxY = cy + halfSpan;
    } else {
      const cx = (this.bounds.minX + this.bounds.maxX) / 2;
      const cy = (this.bounds.minY + this.bounds.maxY) / 2;
      const spanX = Math.max(1000, this.bounds.maxX - this.bounds.minX) / this.zoom;
      const spanY = Math.max(1000, this.bounds.maxY - this.bounds.minY) / this.zoom;
      viewMinX = cx - spanX / 2;
      viewMaxX = cx + spanX / 2;
      viewMinY = cy - spanY / 2;
      viewMaxY = cy + spanY / 2;
    }

    const spanX = Math.max(1, viewMaxX - viewMinX);
    const spanY = Math.max(1, viewMaxY - viewMinY);
    const toScreen = (wx, wy) => ({
      px: 40 + ((wx - viewMinX) / spanX) * (w - 80) + this.panX,
      py: h - (40 + ((wy - viewMinY) / spanY) * (h - 80)) + this.panY,
    });

    // Draw subtle background grid
    ctx.strokeStyle = "rgba(42, 54, 75, 0.45)";
    ctx.lineWidth = 1;
    for (let gx = 0; gx <= w; gx += 100) {
      ctx.beginPath();
      ctx.moveTo(gx, 0);
      ctx.lineTo(gx, h);
      ctx.stroke();
    }
    for (let gy = 0; gy <= h; gy += 100) {
      ctx.beginPath();
      ctx.moveTo(0, gy);
      ctx.lineTo(w, gy);
      ctx.stroke();
    }

    // Draw dungeon path trail up to currentMs
    if (this.playerTrail.length > 1) {
      ctx.beginPath();
      ctx.strokeStyle = "rgba(56, 189, 248, 0.25)";
      ctx.lineWidth = 2;
      let started = false;
      for (let i = 0; i < this.playerTrail.length; i++) {
        const pt = this.playerTrail[i];
        if (pt.t > this.currentMs) break;
        const { px, py } = toScreen(pt.x, pt.y);
        if (!started) {
          ctx.moveTo(px, py);
          started = true;
        } else {
          ctx.lineTo(px, py);
        }
      }
      ctx.stroke();
    }

    // Draw enemies first, then players on top
    const drawUnit = u => {
      const { px, py } = toScreen(u.x, u.y);
      if (px < -50 || px > w + 50 || py < -50 || py > h + 50) return;

      const isBoss = u.actor.type === "Boss" || u.actor.subType === "Boss";
      const radius = u.isPlayer ? 9 : (isBoss ? 12 : 6);
      const color = u.isPlayer
        ? (WOW_CLASS_COLORS[u.actor.type] || WOW_CLASS_COLORS[u.actor.subType] || "#38bdf8")
        : (isBoss ? "#ef4444" : "#f97316");

      ctx.beginPath();
      ctx.arc(px, py, radius, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
      ctx.lineWidth = u.isPlayer || isBoss ? 2 : 1;
      ctx.strokeStyle = this.focusActorId === u.id ? "#facc15" : "#0f172a";
      ctx.stroke();

      // HP bar above unit
      const hpRatio = Math.max(0, Math.min(1, (u.hp_pct || 10000) / 10000));
      const barW = u.isPlayer ? 36 : 26;
      const barH = 4;
      const bx = px - barW / 2;
      const by = py - radius - 8;
      ctx.fillStyle = "rgba(15, 23, 42, 0.85)";
      ctx.fillRect(bx, by, barW, barH);
      ctx.fillStyle = u.isPlayer ? "#22c55e" : "#ef4444";
      ctx.fillRect(bx, by, barW * hpRatio, barH);

      if (u.isPlayer || isBoss || enemyUnits.length <= 12) {
        ctx.fillStyle = u.isPlayer ? "#f8fafc" : "#cbd5e1";
        ctx.font = u.isPlayer ? "bold 12px sans-serif" : "11px sans-serif";
        const pctText = `${Math.round(hpRatio * 100)}%`;
        ctx.fillText(`${u.actor.name} (${pctText})`, px + radius + 5, py + 4);
      }
    };

    enemyUnits.forEach(drawUnit);
    playerUnits.forEach(drawUnit);

    // Draw legend overlay
    ctx.fillStyle = "rgba(15, 23, 42, 0.82)";
    ctx.fillRect(12, 12, 540, 28);
    ctx.fillStyle = "#e2e8f0";
    ctx.font = "12px sans-serif";
    ctx.fillText(
      `Players: ${playerUnits.length} | Active Enemies Nearby: ${enemyUnits.length} | Mouse Wheel: Zoom (${this.zoom.toFixed(1)}x) | Drag: Pan`,
      20,
      30
    );
  }
}

window.Replay2DPlayer = Replay2DPlayer;
