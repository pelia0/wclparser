class Replay2DPlayer {
  constructor(canvasId, scrubberId, timeLabelId) {
    this.canvas = document.getElementById(canvasId);
    this.ctx = this.canvas.getContext("2d");
    this.scrubber = document.getElementById(scrubberId);
    this.timeLabel = document.getElementById(timeLabelId);
    this.frames = [];
    this.actors = {};
    this.durationMs = 1;
    this.currentMs = 0;
    this.playing = false;
    this.speed = 1.0;
    this.bounds = { minX: -1000, maxX: 1000, minY: -1000, maxY: 1000 };
    this._bindEvents();
  }

  _bindEvents() {
    this.scrubber.addEventListener("input", () => {
      this.currentMs = (Number(this.scrubber.value) / 1000) * this.durationMs;
      this.renderFrame();
    });
  }

  loadReplay(replayData, summaryData) {
    this.frames = replayData.frames || [];
    this.durationMs = Math.max(1, summaryData.duration_ms || 1);
    this.actors = {};
    (summaryData.actors || []).forEach(a => { this.actors[a.id] = a; });
    if (this.frames.length > 0) {
      const xs = this.frames.map(f => f.x);
      const ys = this.frames.map(f => f.y);
      this.bounds = {
        minX: Math.min(...xs) - 500,
        maxX: Math.max(...xs) + 500,
        minY: Math.min(...ys) - 500,
        maxY: Math.max(...ys) + 500,
      };
    }
    this.currentMs = 0;
    this.renderFrame();
  }

  togglePlay() {
    this.playing = !this.playing;
    if (this.playing) {
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

  renderFrame() {
    const ctx = this.ctx;
    const w = this.canvas.width;
    const h = this.canvas.height;
    ctx.clearRect(0, 0, w, h);

    const sec = (this.currentMs / 1000).toFixed(1);
    this.timeLabel.textContent = `${sec}s / ${(this.durationMs / 1000).toFixed(1)}s`;

    const latestByActor = {};
    for (let i = 0; i < this.frames.length; i++) {
      const f = this.frames[i];
      if (f.t > this.currentMs) break;
      latestByActor[f.actor] = f;
    }

    const spanX = Math.max(1, this.bounds.maxX - this.bounds.minX);
    const spanY = Math.max(1, this.bounds.maxY - this.bounds.minY);

    Object.values(latestByActor).forEach(pos => {
      const px = 40 + ((pos.x - this.bounds.minX) / spanX) * (w - 80);
      const py = 40 + ((pos.y - this.bounds.minY) / spanY) * (h - 80);
      const actor = this.actors[pos.actor] || { name: `#${pos.actor}`, type: "Unit" };

      ctx.beginPath();
      ctx.arc(px, py, actor.type === "Boss" ? 12 : 8, 0, Math.PI * 2);
      ctx.fillStyle = actor.type === "Boss" ? "#ef4444" : "#22c55e";
      ctx.fill();

      ctx.fillStyle = "#e2e8f0";
      ctx.font = "12px sans-serif";
      ctx.fillText(`${actor.name} (${(pos.hp_pct / 100).toFixed(0)}%)`, px + 12, py + 4);
    });
  }
}
window.Replay2DPlayer = Replay2DPlayer;
