# BatchForge — Distributed Batch Processing Demo

A sleek, dark-themed client-server batch processing dashboard with real-time updates via Server-Sent Events (SSE).

## Features

- **Real-time updates** via SSE (no polling needed)
- **Multi-client support** — any device on the same Wi-Fi can connect
- **Job types**: Image Resize, ML Inference, Video Transcode, PDF Export, Data ETL
- **Priorities**: Low / Normal / High / Critical
- **Live throughput meter** and server log
- **Simulated failures** (~12% error rate) to demonstrate error handling

## Setup

### Prerequisites
- Node.js 16+ (no npm packages needed — pure stdlib)

### Run

```bash
node server.js
```

Then open: `http://localhost:8080`

To connect from other devices on the same network:
`http://<your-ip>:8080`   (e.g. `http://192.168.1.11:8080`)

## Architecture

```
server.js          — HTTP + SSE server (Node stdlib only)
index.html         — Served by the server at GET /

API Endpoints:
  GET  /           → serves index.html
  GET  /events     → SSE stream (real-time push)
  GET  /stats      → current server stats (JSON)
  GET  /my-jobs    → jobs submitted by this client IP
  POST /batch      → submit a batch of jobs
                     body: { type, sizeKB, count, priority }
  POST /reset      → clear this client's jobs
```

## Customization

- Change `PORT` at top of `server.js` (default: 8080)
- Adjust failure rate: change `0.12` in `processJob()` 
- Add real job logic by replacing the `setTimeout` in `processJob()`