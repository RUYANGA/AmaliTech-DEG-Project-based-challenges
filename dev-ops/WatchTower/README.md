# WatchTower

This challenge is designed to test your understanding of observability — one of the most critical and often overlooked areas of DevOps engineering.

---

## 1. Business Context

**Client:** Reyla Logistics
**Industry:** Last-mile delivery operations

### The Problem

Reyla runs three backend services: an order service, a tracking service, and a notification service. Over the past month, each of them has gone down at least once. In every case, Reyla found out from an angry customer, not from their own team.

They have no dashboards, no alerting, and their logs are scattered across three separate terminal windows that someone has to be watching to catch errors.

### Your Role

You are joining as their DevOps engineer. The services are already written. Your job is to **wire up a full observability stack** — metrics collection, dashboards, and alerting — so the team can see what is happening inside their system at any time, and get notified before customers do.

---

## 2. The System

Three small services are provided in the [`app/`](./app/) directory:

| Service                | Port | What it does                        |
| ---------------------- | ---- | ----------------------------------- |
| `order-service`        | 3001 | Creates and lists orders            |
| `tracking-service`     | 3002 | Updates and returns delivery status |
| `notification-service` | 3003 | Logs notification events            |

Each service has a `/health` endpoint and a `/metrics` endpoint that exposes [Prometheus-compatible metrics](https://prometheus.io/docs/instrumenting/exposition_formats/).

Run all three locally:

```bash
docker compose up --build
```

Do not change the business logic of any service. You may add environment variable support or adjust how metrics are exposed if needed.

---

## 3. The Assignment

### Part 1 — Unified Local Environment

**Deliverable:** A `docker-compose.yml` that runs the entire stack together.

Requirements:

- All three app services must start with `docker compose up`.
- Each service must have its port mapped to the host for local testing.
- Services must be on a shared Docker network so they can communicate by service name.
- All configuration (ports, service names) must be passed via environment variables from an `.env` file. Include a `.env.example` with placeholder values.

---

### Part 2 — Metrics Collection

**Deliverable:** Prometheus added to the `docker-compose.yml` and configured to scrape all three services.

Requirements:

- Add a **Prometheus** container to your Compose file.
- Write a `prometheus.yml` configuration file that scrapes the `/metrics` endpoint of each service every 15 seconds.
- Prometheus UI must be accessible at `http://localhost:9090`.
- Verify it works: the Prometheus "Targets" page (`/targets`) must show all three services as **UP**.

---

### Part 3 — Dashboards

**Deliverable:** Grafana added to the Compose stack with a pre-built dashboard.

Requirements:

- Add a **Grafana** container to your Compose file. It must use Prometheus as its data source.
- Grafana must be accessible at `http://localhost:3000`.
- Create a dashboard (exported as JSON in `grafana/dashboards/`) that displays **at minimum**:
  - HTTP request rate for each service
  - Error rate (5xx responses) for each service
  - Current health status of each service
- The dashboard must load automatically when Grafana starts — no manual import steps. Use [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) to achieve this.

---

### Part 4 — Alerting

**Deliverable:** Alert rules defined in a `prometheus/alerts.yml` file.

Write alerting rules for the following conditions:

| Alert Name           | Condition                                                                  | Severity |
| -------------------- | -------------------------------------------------------------------------- | -------- |
| `ServiceDown`        | Any service's `/health` returns non-200 for more than 1 minute             | critical |
| `HighErrorRate`      | More than 5% of requests result in 5xx errors over a 5-minute window       | warning  |
| `ServiceNotScraping` | Prometheus has not received metrics from a service for more than 2 minutes | warning  |

Requirements:

- Rules must be loaded into Prometheus via the `prometheus.yml` config.
- Each rule must include a human-readable `summary` and `description` annotation.
- Document in your README how you tested that each alert fires correctly.

---

### Part 5 — Structured Logging

**Deliverable:** Documentation in your README.

Each service already logs to stdout. Your task:

- Configure the Docker Compose log driver to write logs in JSON format.
- Show a command a developer can run to:
  1. View live logs from all services at once.
  2. Filter logs to show only errors from a specific service.
- Document both commands in your README with example output.

---

## 4. Bonus (Optional)

Pick **one** if you want to go further:

- **Alertmanager:** Route `critical` alerts to a webhook or email using [Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/).
- **Loki:** Add [Grafana Loki](https://grafana.com/oss/loki/) to the stack for log aggregation and build a Logs panel in your dashboard.
- **Uptime graph:** Add a panel to your Grafana dashboard that visualises service uptime percentage over the past 24 hours.

Describe what you added and why in your README.

---

## 5. Documentation Requirements

Your final `README.md` must replace these instructions and cover:

1. **Architecture diagram** — show the full observability stack (services → Prometheus → Grafana → alerts).
2. **Setup instructions** — how to start the stack and verify everything is working.
3. **Dashboard walkthrough** — a screenshot and short description of each panel.
4. **Alert testing** — how you confirmed each alert fires correctly.
5. **Log commands** — the two log commands from Part 5 with example output.

---

## 6. Submission Instructions

1. **Fork** this repository.
2. Complete all five parts in your fork.
3. Replace this README with your own documentation as outlined above.
4. Submit your repo link via the [online form](https://forms.cloud.microsoft/e/f3FF83LVz3).

---

## ⚠️ Pre-Submission Checklist

### Stack

- [ ] `docker compose up --build` starts all services, Prometheus, and Grafana with no errors.
- [ ] A `.env.example` file is committed; the real `.env` is not.
- [ ] Prometheus `/targets` shows all three services as **UP**.
- [ ] Grafana dashboard loads automatically without manual import.

### Alerts

- [ ] All three alert rules are present in `prometheus/alerts.yml`.
- [ ] Each rule has a `summary` and `description` annotation.
- [ ] README explains how you tested each alert.

### Documentation

- [ ] Architecture diagram is included.
- [ ] Both log commands are documented with example output.
- [ ] This README has been replaced with your own documentation.
- [ ] Commit history shows progress over time (not a single upload commit).
- [ ] GitHub repository is set to **Public**.
