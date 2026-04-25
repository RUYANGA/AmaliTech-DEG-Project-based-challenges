# Idempotency Gateway — The "Pay-Once" Protocol

This repository implements an Idempotency Gateway that prevents duplicate charges when client systems retry payment requests. The gateway enforces an `Idempotency-Key` contract: the first request with a key is processed and its response persisted; subsequent requests with the same key return the original response immediately.

Overview
--------
I built this service to demonstrate a robust, production-minded approach to request idempotency for payment workflows. This README contains the architecture, API, setup, design decisions, and one production-focused enhancement I implemented.

Key Goals
- Process each logical payment exactly once for a given `Idempotency-Key`.
- Return the same response and HTTP status for retries.
- Reject reuse of an idempotency key with a different payload.
- Handle concurrent (in-flight) identical requests without double processing.

Tech stack
- Python 3.10+
- FastAPI (example implementation)
- PostgreSQL for durable idempotency records

Architecture
------------
Sequence and flow diagrams are embedded below (Mermaid). They describe the canonical flows: first request, cache hit, and in-flight waiting.

Sequence diagram (Mermaid):

```mermaid
sequenceDiagram
    participant Client
    participant Gateway
    participant DB as PostgreSQL
    participant Processor as PaymentService

    Client->>Gateway: POST /process-payment (Idempotency-Key + payload)
    Gateway->>DB: SELECT idempotency_key
    alt Key not present
        Gateway->>DB: INSERT record (status: processing)
        Gateway->>Processor: simulate processing (2s)
        Processor-->>Gateway: processing result
        Gateway->>DB: UPDATE record (status: completed, store response)
        Gateway-->>Client: 200/201 + response
    else Key present and completed
        DB-->>Gateway: stored response
        Gateway-->>Client: cached response (X-Cache-Hit: true)
    else Key present and processing
        Gateway-->>DB: wait for completion (block)
        DB-->>Gateway: completed response
        Gateway-->>Client: cached response (X-Cache-Hit: true)
    end
```

Flowchart (high-level):

```mermaid
flowchart TD
  A[Incoming POST /process-payment]
  B{Has Idempotency-Key header?}
  C[Return 400 Bad Request]
  D{Key exists in DB?}
  E[Compare payload hash]
  F[Return 409/422 - Key reused with different payload]
  G[If status=processing → wait]
  H[Return stored response (X-Cache-Hit: true)]
  I[Process payment (simulate 2s) and store response]
  A-->B
  B-->|no|C
  B-->|yes|D
  D-->|no|I
  D-->|yes|E
  E-->|different|F
  E-->|same & completed|H
  E-->|same & processing|G
  G-->H
```

Getting started
---------------
Prerequisites
- Python 3.10+
- PostgreSQL 12+

Environment (example):
- `DATABASE_URL=postgresql://user:password@localhost:5432/idempotency_db`
- `PORT=8000`

Install and run (example with FastAPI):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# create table (example)
psql $DATABASE_URL -c "CREATE TABLE IF NOT EXISTS idempotency_records (
  id SERIAL PRIMARY KEY,
  idempotency_key TEXT UNIQUE NOT NULL,
  payload_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  response_body JSONB,
  response_status INT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  expires_at TIMESTAMPTZ NULL
);"

uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --reload
```

API
---
POST /process-payment

Headers:
- `Content-Type: application/json`
- `Idempotency-Key: <unique-string>` (required)

Body example:
```json
{ "amount": 100, "currency": "GHS", "metadata": { "order_id": "abc-123" } }
```

Successful first request
- 200 OK or 201 Created
- Body: `{ "status": "Charged 100 GHS", "id": "<transaction-id>" }`

Duplicate (same key + same payload)
- Returns the same status and body as the first response
- Response header: `X-Cache-Hit: true`

Duplicate (same key + different payload)
- 409 Conflict
- `{ "error": "Idempotency key already used for a different request body." }`

Errors
- 400 Bad Request — missing `Idempotency-Key` or invalid JSON
- 500 Internal Server Error — unexpected failures

Behavioral notes
----------------
- I use a canonicalized JSON + SHA256 payload hash to detect payload equality.
- First request: insert a record with `status=processing`, simulate processing (2s), then update to `completed` with stored response.
- If a second request arrives while the first is `processing`, it waits for completion and then returns the stored response (no duplicate processing).

Schema (simplified)
```
idempotency_records(
  id SERIAL PRIMARY KEY,
  idempotency_key TEXT UNIQUE NOT NULL,
  payload_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('processing','completed','failed')),
  response_body JSONB,
  response_status INT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  expires_at TIMESTAMPTZ NULL
)
```

Developer's choice (implemented)
--------------------------------
Feature: Idempotency key TTL + automatic cleanup + payload hashing.

Rationale: Idempotency records should expire to limit storage growth and privacy exposure. I store a `payload_hash` to ensure structural payload equality even if clients reorder JSON fields.

Implementation notes: when creating a record I set `expires_at = now() + TTL` (configurable). A periodic cleanup job (cron or background worker) removes expired records.

Security
--------
- Do not store raw sensitive payment data in logs or responses.
- Use HTTPS in production.

Design decisions (short)
- PostgreSQL for durability and atomic constraints.
- Payload equality via canonicalized JSON + SHA256.
- Concurrency via atomic insert + status polling to avoid races.

Pre-submission checklist
- [ ] Repository public
- [ ] Remove sensitive files (node_modules, .env with secrets, etc.)
- [ ] Server starts with documented commands

Next steps
- I can add a minimal FastAPI example, migration script, and a small test harness on request.

---
*End of README*
