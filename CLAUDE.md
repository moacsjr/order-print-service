# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**SQS Local Printer Worker** — A Go service for Windows that bridges AWS SQS and a local Windows printer. It polls a queue, prints sale orders as raw/text receipts, routes failures to a DLQ, and exposes a local HTML dashboard for diagnostics.

## Architecture

The application is a single-file Go program (`main.go`) with three operational modules:

1. **SQS Worker** — Long-polls the main queue, prints messages via the Win32 spooler (`alexbrainman/printer`), deletes on success, routes to DLQ on failure.
2. **DLQ Drain** — Consumes the Dead Letter Queue on startup (and on demand via dashboard). Aborts immediately on print failure to avoid blocking startup.
3. **HTTP Dashboard** — Native `net/http` server on `:8080` serving `dashboard.html` + three JSON API endpoints.

### Key design decisions
- Single file `main.go` — the app is small enough that splitting into packages adds ceremony without benefit.
- DLQ drain runs **before** the main worker loop on startup. If a DLQ message fails to print, the drain aborts (message stays in DLQ) and the main loop starts so the system isn't blocked.
- The `/api/dlq` peek endpoint uses `VisibilityTimeout: 2` then immediately resets visibility to `0` so messages remain available for workers.
- All AWS and printer calls use a shared `context.Context` wired to OS signal handling for graceful shutdown.

## File Structure

```
main.go          # Entire application: config, SQS worker, printer, HTTP dashboard
config.yaml      # AWS credentials, printer name, worker params, logging
dashboard.html   # Dark-mode HTML dashboard with vanilla JS
go.mod           # Go module definition
```

## Commands

### Build (cross-compile for Windows)

```bash
GOOS=windows GOARCH=amd64 go build -o order-print-service.exe .
```

### Build for current OS

```bash
go build .
```

### Run locally

```bash
go run .
```

### Download dependencies

```bash
go mod tidy
go mod download
```

## Configuration (`config.yaml`)

- `aws.profile` can be left empty for default credential chain (env vars, ~/.aws/credentials, EC2 IAM role).
- `printer.name` left empty (`""`) uses the Windows default printer.
- `printer.mode` is `"RAW"` (ESC/POS receipts) or `"TEXT"` (formatted text via GDI).
- `worker.max_number_of_messages` max 10, `worker.wait_time_seconds` max 20 (hard limits enforced at load).

## API Endpoints (Dashboard)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Dashboard UI |
| POST | `/api/test-print` | Send a mock receipt to the printer |
| GET | `/api/dlq` | Peek at DLQ messages (ID + body preview, non-destructive) |
| POST | `/api/dlq/reprocess` | Trigger DLQ drain on demand |

## Target Platform

**Windows** — the `alexbrainman/printer` package uses CGO/Win32 API calls and will not compile or run on Linux/macOS. Use `GOOS=windows` for cross-compilation.
