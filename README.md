# SQS Local Printer Worker

A Go service for Windows that bridges AWS SQS and a local printer. It polls a queue, prints sale orders as raw/text receipts, routes failures to a Dead Letter Queue (DLQ), and exposes a local HTML dashboard for diagnostics.

## Architecture

The application consists of three modules:

1. **SQS Worker** — Long-polls the main queue, prints messages via the Win32 spooler (`alexbrainman/printer`), deletes on success, routes to DLQ on failure.
2. **DLQ Drain** — Consumes the Dead Letter Queue on startup and on demand via the dashboard. Aborts immediately on print failure to avoid blocking startup.
3. **HTTP Dashboard** — Native `net/http` server on `:8080` serving a dark-mode HTML dashboard with vanilla JS and three JSON API endpoints.

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

## Quick Start

### Prerequisites

- Go 1.21+
- Windows (required for the printer backend)
- AWS credentials configured (environment variables, `~/.aws/credentials`, or IAM role)

### Build

Cross-compile for Windows:

```bash
GOOS=windows GOARCH=amd64 go build -o order-print-service.exe .
```

Build for the current OS:

```bash
go build .
```

### Run

```bash
go run .
```

The dashboard will be available at `http://localhost:8080`.

## Configuration

Configuration lives in `config.yaml`:

```yaml
aws:
  region: us-east-1
  profile: ""              # empty = default credential chain
  queue_url: ""
  dlq_url: ""

printer:
  name: ""                 # empty = Windows default printer
  mode: "RAW"              # "RAW" (ESC/POS) or "TEXT" (GDI)

worker:
  max_number_of_messages: 5
  wait_time_seconds: 10
```

> **Note:** `worker.max_number_of_messages` has a max of 10 and `worker.wait_time_seconds` a max of 20. These limits are enforced at load time.

## API Endpoints

| Method | Path               | Description                        |
|--------|--------------------|------------------------------------|
| GET    | `/`                | Dashboard UI                       |
| POST   | `/api/test-print`  | Send a mock receipt to the printer |
| GET    | `/api/dlq`         | Peek at DLQ messages (non-destructive) |
| POST   | `/api/dlq/reprocess` | Trigger DLQ drain on demand     |

## Target Platform

**Windows** — the `alexbrainman/printer` package uses CGO/Win32 API calls and will not compile or run on Linux/macOS. Use `GOOS=windows` for cross-compilation.

## License

MIT
