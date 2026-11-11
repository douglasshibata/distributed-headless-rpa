# 🚀 Distributed Headless RPA Fleet Engine

[![Go Version](https://img.shields.io/badge/go-1.24%2B-blue.svg)](https://golang.org)
[![CDP Protocol](https://img.shields.io/badge/protocol-Chrome%20DevTools%20Protocol-green.svg)](https://chromedevtools.github.io/devtools-protocol/)
[![Engine](https://img.shields.io/badge/engine-go--rod-orange.svg)](https://github.com/go-rod/rod)
[![License](https://img.shields.io/badge/license-MIT-brightgreen.svg)](#)

A high-performance, containerized **Distributed Headless RPA Fleet** in Go that decouples declarative task definitions from browser execution environments. Instead of running monolithic, long-lived browser sessions on local desktop environments or heavy virtual machines, it operates as an event-driven, containerized runner fleet that provisions isolated, ephemeral Chromium instances on demand.

---

## 🏛️ High-Level Architecture

```text
                    [ Client / Scheduler / Webhook ]
                                   │
                                   ▼
                    ┌───────────────────────────────┐
                    │  Control Plane & Job Ingestion│
                    │  (REST / gRPC Gateway)        │
                    └──────────────┬────────────────┘
                                   │
                                   ▼
                    ┌───────────────────────────────┐
                    │   Message Broker & State      │
                    │   (NATS JetStream / Redis)    │
                    └──────────────┬────────────────┘
                                   │
            ┌──────────────────────┴──────────────────────┐
            ▼                                             ▼
┌─────────────────────────┐                   ┌─────────────────────────┐
│ Worker Node A (Docker)  │                   │ Worker Node B (Docker)  │
│ ┌─────────────────────┐ │                   │ ┌─────────────────────┐ │
│ │ Go Runner (Daemon)  │ │                   │ │ Go Runner (Daemon)  │ │
│ └──────────┬──────────┘ │                   │ └──────────┬──────────┘ │
│            ▼ (CDP)      │                   │            ▼ (CDP)      │
│ ┌─────────────────────┐ │                   │ ┌─────────────────────┐ │
│ │ Chromium (Headless) │ │                   │ │ Chromium (Headless) │ │
│ └─────────────────────┘ │                   │ └─────────────────────┘ │
└────────────┬────────────┘                   └────────────┬────────────┘
             │                                             │
             └──────────────────────┬──────────────────────┘
                                    │
                  ┌─────────────────┴─────────────────┐
                  ▼                                   ▼
      ┌───────────────────────┐           ┌───────────────────────┐
      │ Object Storage (S3)   │           │ Webhook / Callbacks   │
      │ (Artifacts, HAR, PNG) │           │ (Caller Notification) │
      └───────────────────────┘           └───────────────────────┘
```

---

## 🧩 Core Subsystems

### 1. Ingestion & Control Plane
- **Task Routing:** Receives automation requests containing target URLs, credentials references, interaction schemas, and timeout policies.
- **Lease Management:** Uses distributed locks (`LeaseManager` via Redis or NATS KV) so tasks are not picked up by multiple nodes during network partitions.
- **Rate-Limiting per Target Host:** Tracks concurrent requests to the same target domain (`DomainRateLimiter`) to avoid getting IP-blocked or overwhelming legacy servers.

### 2. Worker Runner Engine (Go + `go-rod`)
- **CDP Direct Wire:** Interacts with the browser over WebSockets directly via Chrome DevTools Protocol (`go-rod`) rather than wrapping WebDriver/Selenium, cutting latency and protocol translation layers.
- **Memory Isolation:** Each job spawns an isolated browser context. Memory ceiling per container is strictly enforced (e.g., 512MB RAM limit).
- **Process Garbage Collection:** When a task ends (success or error), the Go runner hard-kills the entire OS process group (`syscall.Kill(-pgid, syscall.SIGKILL)`) associated with that Chromium PID. This prevents orphaned browser zombies from exhausting RAM.
- **Stealth Evasion:** Masking `navigator.webdriver`, spoofing user-agent, languages, and plugins.
- **Media Blocking:** Drops images, fonts, and media over CDP request hijacking to optimize network and memory footprint.

### 3. Session & Credential Vault Connector
- **Zero-Trust Memory Buffers:** Workers never accept plain-text passwords over the wire. The payload provides a `credential_ref` (e.g., `vault://production/bank_portal_login`).
- **Memory Zeroization:** The worker dynamically pulls the credential from an encrypted vault into a `SensitiveBuffer` and wipes the memory buffer (`Wipe()`) immediately after form submission.
- **Encrypted Session Caching:** Reusable sessions (cookies/local storage tokens) are cached in an AES-256-GCM encrypted layer so the bot skips MFA/login flows on recurrent runs whenever sessions remain valid.

---

## 📄 Declarative Job Contract

Clients submit JSON payloads defining execution parameters:

```json
{
  "task_id": "job_94821a3f-8c11-4a3a-92e1",
  "target_domain": "supplier-portal.internal",
  "entrypoint": "https://supplier-portal.internal/invoices/export",
  "auth_ref": "vault://creds/supplier_portal_prod",
  "timeout_seconds": 90,
  "execution_policy": {
    "headless": true,
    "block_media": true,
    "proxy_profile": "datacenter_latam_pool",
    "retries": 2
  },
  "pipeline_steps": [
    { "action": "navigate", "url": "https://supplier-portal.internal/invoices/export" },
    { "action": "wait_selector", "selector": "#invoice-table", "fallback_selectors": [".invoice-grid", "table[data-testid='invoices']"], "timeout_ms": 10000 },
    { "action": "input", "selector": "#date-range", "value": "LAST_30_DAYS" },
    { "action": "click", "selector": "#btn-csv-download" },
    { "action": "intercept_download", "destination_bucket": "s3://rpa-exports/invoices/", "output_file": "invoices.csv" }
  ],
  "callback_webhook": "https://api.internal/v1/jobs/callback"
}
```

---

## 🛡️ Resilience, Failure Modes & Edge Cases

| Failure Mode | Root Cause | System Defense |
|---|---|---|
| **Zombified Process** | Page freezes on heavy JavaScript execution or broken WebSocket. | Go execution wrapper uses a `context.WithTimeout`. On expiration, it fires a process-tree kill signal (`SIGKILL` to `-pgid`) to Chromium and releases the worker. |
| **DOM Drift / Mutation** | Target site changes layout or CSS class names. | Fallback selector arrays (`fallback_selectors`). If all fail, capture a full-page screenshot, DOM dump, and error stack to S3/storage for instant triage. |
| **Bot Detection / Captcha** | Target site detects headless browser signatures or repeated IP hits. | Injection of stealth evasion patches (masking `navigator.webdriver`, spoofing languages) and dynamic routing through proxy rotation pools. |
| **Worker Host OOM** | Concurrently executing multiple heavy single-page apps (SPAs). | Browser flag tuning: `--disable-gpu`, `--no-sandbox`, `--disable-dev-shm-usage`, and blocking images/fonts (`*.png`, `*.woff2`) unless explicitly required. |

---

## 📈 Scaling Strategy (KEDA on Kubernetes)

The fleet scales automatically based on queue pressure:
- **Queue Length Trigger:** Kubernetes Event-driven Autoscaling (KEDA) monitors the depth of pending jobs in the NATS/RabbitMQ/Redis queue.
- **Horizontal Pod Autoscaling (HPA):** If pending jobs > 20, KEDA scales the worker deployment from a base pool (e.g., 3 pods) up to a burst ceiling (e.g., 50 pods).
- **Warm Pool Provisioning:** To avoid Chromium cold-start latency (~800ms–1.5s), workers maintain a pool of pre-warmed idle browser contexts ready to bind to incoming tasks instantly.

---

## 🧪 Testing

Run all unit and integration tests:

```bash
cd distributed-headless-rpa
go test -v ./...
```
