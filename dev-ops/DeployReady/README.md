# DeployReady

This challenge is designed to test your understanding of core DevOps practices: containerisation, automated pipelines, and cloud deployment.

---

## 1. Business Context

**Client:** Kora Analytics
**Industry:** SaaS — Data dashboards for logistics companies

### The Problem

Every time the Kora team wants to deploy a new version of their app, a developer manually SSHs into the server, pulls the code, and restarts the process by hand. There are no automated tests before a release and no way to tell if a deploy broke something until a customer complains.

### Your Role

You are joining as their first DevOps engineer. The application code already works — your job is to **containerise it, automate the delivery pipeline, and get it running on AWS**.

---

## 2. The Application

A simple Node.js API is provided in the [`app/`](./app/) directory. It has three endpoints:

| Method | Route      | Description                            |
| ------ | ---------- | -------------------------------------- |
| GET    | `/health`  | Returns `{ "status": "ok" }`           |
| GET    | `/metrics` | Returns uptime and memory usage        |
| POST   | `/data`    | Accepts a JSON body and echoes it back |

Run it locally:

```bash
cd app
npm install
npm start
```

Do not change the application logic. Your work is everything around it.

---

## 3. The Assignment

### Part 1 — Containerise the App

**Deliverables:** A `Dockerfile` and a `docker-compose.yml` in the root of your repository.

**Dockerfile requirements:**

- The app must run inside a Docker container.
- The container must accept a `PORT` environment variable.
- The container must **not** run as the `root` user.

**Docker Compose requirements:**

- Define the app as a service in `docker-compose.yml`.
- Map port `3000` on the host to the container.
- Pass the `PORT` variable via an `.env` file (include a `.env.example` with placeholder values).
- Running the following must start a working API:
  ```bash
  docker compose up --build
  ```

---

### Part 2 — Automate the Pipeline

**Deliverable:** A `.github/workflows/deploy.yml` GitHub Actions workflow.

The pipeline must run these steps **in order** on every push to `main`:

1. **Test** — Run `npm test`. If tests fail, the pipeline stops. Nothing gets deployed.
2. **Build** — Build the Docker image and tag it with the Git commit SHA.
3. **Push** — Push the image to a container registry (GitHub Container Registry or AWS ECR).
4. **Deploy** — Pull the new image on the EC2 server and restart the container.

Additional requirements:

- Secrets (SSH key, registry token) must be stored as **GitHub repository secrets** — never in the code.
- Add a short comment above each step in the YAML explaining what it does.

---

### Part 3 — Deploy to AWS

**Deliverable:** A running service on AWS and a short `DEPLOYMENT.md` explaining your setup.

Provision the following manually (via the AWS Console is fine):

- An **EC2 instance** (`t2.micro`, Amazon Linux 2023) with Docker installed.
- A **Security Group** that allows:
  - HTTP on port 80 from anywhere
  - SSH on port 22 **from your IP only** — not `0.0.0.0/0`
- An **IAM user or role** for the pipeline with only the permissions it needs.

At submission time, `GET http://<your-ec2-ip>/health` must return `{ "status": "ok" }`.

Document in `DEPLOYMENT.md`:

- How you set up the EC2 instance
- How you installed Docker and pulled your image
- How to check if the container is running
- How to view the application logs

---

## 4. Bonus (Optional)

Pick **one** of the following if you want to go further:

- **Use Terraform** to provision the EC2 instance and Security Group instead of the console.
- **Add a CloudWatch alarm** that triggers if `/health` stops responding.
- **Implement a rollback step** in the pipeline that re-deploys the previous image if the health check fails after deploy.

Describe what you added and why in your `DEPLOYMENT.md`.

---

## 5. Submission Instructions

1. **Fork** this repository.
2. Complete all three parts in your fork.
3. **Replace this README** with your own documentation (architecture overview, setup steps, decisions made).
4. Submit your repo link via the [online form](https://forms.cloud.microsoft/e/f3FF83LVz3).

---

## ⚠️ Pre-Submission Checklist

- [ ] `docker compose up --build` starts the app locally
- [ ] A `.env.example` file is committed (the real `.env` is not)
- [ ] At least one successful pipeline run is visible in the GitHub Actions tab
- [ ] `GET /health` on your EC2 public IP returns 200
- [ ] No secrets or `.pem` files committed to the repository
- [ ] SSH port 22 is **not** open to `0.0.0.0/0`
- [ ] `DEPLOYMENT.md` is present and covers the four points in Part 3
- [ ] This README has been replaced with your own documentation
- [ ] Commit history shows progress over time (not a single upload commit)
