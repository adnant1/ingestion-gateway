# Multi-Storage Ingestion Gateway

A lightweight, async ingestion service that accepts JSON records over HTTP, buffers and batches them, and reliably delivers them to a configurable storage backend.

The core ingestion pipeline is **storage-agnostic** — changing where data is written requires swapping a sink implementation, not rewriting the pipeline.

---

## What This Is

The ingestion gateway sits in front of storage systems and provides a dependable way to ingest high-volume data without coupling the pipeline to a specific backend.

**Clients** send JSON records
**The gateway** queues, batches, retries, and flushes
**Sinks** handle persistence (file, terminal, S3, etc.)

This mirrors real ingestion layers used in log shippers, telemetry collectors, and storage frontends.

---

## Key Features

- Async HTTP ingestion (`FastAPI`, `asyncio`)
- Bounded in-memory queue with backpressure
- Size- and time-based batching
- Pluggable sink interface
- Retry with exponential backoff
- Dead-Letter Queue (DLQ) for unrecoverable failures
- Graceful startup and shutdown
- Storage backends configurable at runtime

---

## Architecture Overview

```
Client
  |
  v
POST /ingest
  |
  v
Ingestion Queue (bounded)
  |
  v
Batch Worker
  |
  +--> Retry Policy
  |
  +--> Primary Sink
  |
  +--> DLQ Sink (on failure)
```

**Design principle:**

> The ingestion pipeline never changes — only the sink does.

---

## Supported Sinks

### Primary Sinks

- **TerminalSink** – logs records to stdout (development)
- **FileSink** – appends records as NDJSON to a file
- **S3Sink** – writes each batch as an object to Amazon S3

### Dead-Letter Queue

- File-based DLQ (NDJSON)

---

## Failure Semantics

- Transient sink failures are retried with exponential backoff
- Permanent failures or exhausted retries are routed to the DLQ
- DLQ writes are durable and append-only
- If DLQ write fails, the service crashes loudly (no silent data loss)

---

## Configuration

Configuration is provided via environment variables.

### Example `.env`

```bash
# Primary sink
SINK_TYPE=file
FILE_SINK_PATH=/absolute/path/output.ndjson

# DLQ sink
DLQ_PATH=/absolute/path/dlq.ndjson

# S3 (if using S3Sink)
AWS_REGION=us-east-1
S3_BUCKET=my-ingestion-bucket
S3_PREFIX=ingestion/
```

AWS credentials are resolved via standard AWS mechanisms (environment variables, IAM role, etc.).

---

## Running Locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Send data:

```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"payload":[{"msg":"hello"},{"msg":"world"}]}'
```

---

## Project Goals

- async concurrency and background workers
- backpressure and buffering
- clean separation of pipeline vs storage
- failure-mode and reliability thinking
- lifecycle-aware service design

---

## 👨‍💻 Author

**Adnan T.** — [@adnant1](https://github.com/adnant1)
