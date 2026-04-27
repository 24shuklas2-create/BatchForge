const http = require("http");
const fs = require("fs");
const path = require("path");
const { EventEmitter } = require("events");

const PORT = process.env.PORT || 8080;
const bus = new EventEmitter();

// In-memory state
const state = {
  jobs: {},       // jobId -> job object
  clients: {},    // ip -> { ip, submittedAt, jobIds[] }
  log: [],
  throughputBytes: 0,
  throughputWindow: [],
};

// Helpers
function genId() {
  return Math.random().toString(36).slice(2, 9);
}

function addLog(msg, type = "info") {
  const entry = {
    ts: new Date().toISOString(),
    msg,
    type,
  };
  state.log.unshift(entry);
  if (state.log.length > 100) state.log.pop();
  bus.emit("update");
}

function calcThroughput() {
  const now = Date.now();
  state.throughputWindow = state.throughputWindow.filter(
    (w) => now - w.ts < 3000
  );
  return state.throughputWindow.reduce((s, w) => s + w.bytes, 0) / 3;
}

// Simulate worker
function processJob(jobId) {
  const job = state.jobs[jobId];
  if (!job) return;
  job.status = "active";
  job.startedAt = Date.now();
  bus.emit("update");

  const duration = 800 + Math.random() * 2500;
  const fail = Math.random() < 0.12; // 12% failure rate

  setTimeout(() => {
    if (fail) {
      job.status = "error";
      job.error = "Worker timeout or corrupt payload";
      addLog(`✗ Job #${jobId} error — ${job.error}`, "error");
    } else {
      job.status = "done";
      job.finishedAt = Date.now();
      job.duration = job.finishedAt - job.startedAt;
      state.throughputWindow.push({ ts: Date.now(), bytes: job.sizeKB * 1024 });
      addLog(
        `✓ Job #${jobId} done in ${job.duration}ms`,
        "success"
      );
    }
    bus.emit("update");
  }, duration);
}

// CORS / JSON helpers
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

function json(res, data, status = 200) {
  cors(res);
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

function readBody(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      try {
        resolve(JSON.parse(body));
      } catch {
        resolve({});
      }
    });
  });
}

// SSE clients for live push
const sseClients = new Set();

function pushUpdate() {
  const payload = JSON.stringify(getStats());
  for (const res of sseClients) {
    res.write(`data: ${payload}\n\n`);
  }
}

bus.on("update", pushUpdate);

// Stats aggregation
function getStats() {
  const jobs = Object.values(state.jobs);
  return {
    totalJobs: jobs.length,
    activeJobs: jobs.filter((j) => j.status === "active").length,
    completedJobs: jobs.filter((j) => j.status === "done").length,
    errorJobs: jobs.filter((j) => j.status === "error").length,
    throughputKBs: Math.round(calcThroughput() / 1024),
    clients: Object.values(state.clients).map((c) => ({
      ip: c.ip,
      total: c.jobIds.length,
      done: c.jobIds.filter(
        (id) => state.jobs[id] && state.jobs[id].status === "done"
      ).length,
      error: c.jobIds.filter(
        (id) => state.jobs[id] && state.jobs[id].status === "error"
      ).length,
    })),
    log: state.log.slice(0, 20),
    jobs: Object.values(state.jobs)
      .sort((a, b) => b.createdAt - a.createdAt)
      .slice(0, 50),
  };
}

// Routes
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost`);
  const clientIp =
    req.headers["x-forwarded-for"] || req.socket.remoteAddress || "unknown";

  // Preflight
  if (req.method === "OPTIONS") {
    cors(res);
    res.writeHead(204);
    res.end();
    return;
  }

  // Serve frontend
  if (req.method === "GET" && url.pathname === "/") {
    const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
    cors(res);
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(html);
    return;
  }

  // SSE stream
  if (req.method === "GET" && url.pathname === "/events") {
    cors(res);
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
    res.write(`data: ${JSON.stringify(getStats())}\n\n`);
    sseClients.add(res);
    req.on("close", () => sseClients.delete(res));
    return;
  }

  // GET stats
  if (req.method === "GET" && url.pathname === "/stats") {
    return json(res, getStats());
  }

  // GET my jobs
  if (req.method === "GET" && url.pathname === "/my-jobs") {
    const client = state.clients[clientIp];
    if (!client) return json(res, { jobs: [] });
    const jobs = client.jobIds
      .map((id) => state.jobs[id])
      .filter(Boolean)
      .sort((a, b) => b.createdAt - a.createdAt);
    return json(res, { jobs });
  }

  // POST batch
  if (req.method === "POST" && url.pathname === "/batch") {
    const body = await readBody(req);
    const { type = "image_resize", sizeKB = 128, count = 3, priority = "normal" } = body;
    const clamped = Math.min(Math.max(parseInt(count) || 1, 1), 20);

    if (!state.clients[clientIp]) {
      state.clients[clientIp] = { ip: clientIp, submittedAt: Date.now(), jobIds: [] };
    }

    const ids = [];
    for (let i = 0; i < clamped; i++) {
      const id = genId();
      state.jobs[id] = {
        id,
        type,
        sizeKB: parseInt(sizeKB) || 128,
        priority,
        status: "queued",
        clientIp,
        createdAt: Date.now(),
      };
      state.clients[clientIp].jobIds.push(id);
      ids.push(id);
    }

    addLog(
      `→ POST /batch (${clamped}× ${type}, ${sizeKB}KB) from ${clientIp}`,
      "info"
    );
    addLog(`  202 Accepted — ${clamped} jobs queued`, "success");

    // Stagger processing
    ids.forEach((id, i) => setTimeout(() => processJob(id), i * 180));

    return json(res, { accepted: ids.length, ids }, 202);
  }

  // POST reset
  if (req.method === "POST" && url.pathname === "/reset") {
    const client = state.clients[clientIp];
    if (client) {
      client.jobIds.forEach((id) => delete state.jobs[id]);
      delete state.clients[clientIp];
    }
    addLog(`↺ Client ${clientIp} reset`, "info");
    return json(res, { ok: true });
  }

  cors(res);
  res.writeHead(404);
  res.end("Not found");
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`\n  ⚡ Batch Processing Server running`);
  console.log(`  → http://localhost:${PORT}\n`);
  addLog("Server started", "success");
});
