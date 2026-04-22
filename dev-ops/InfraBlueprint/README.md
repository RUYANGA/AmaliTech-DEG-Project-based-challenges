# InfraBlueprint

This challenge is designed to test your ability to define, provision, and manage cloud infrastructure as code — a foundational DevOps skill.

---

## 1. Business Context

**Client:** Vela Payments
**Industry:** Fintech — Payment processing for small businesses

### The Problem

Vela's infrastructure was built by clicking through the AWS Console over two years. No one documented what was created or why. Last month, their lead engineer left the company. Nobody knows what is running, what it costs, or how to rebuild it if something goes wrong.

The CTO has one goal: **if the entire AWS account was deleted tomorrow, a new engineer should be able to rebuild the infrastructure by running a single command**.

### Your Role

You are joining as their infrastructure engineer. You will not be running application code. Your job is to write **Terraform configuration** that provisions Vela's infrastructure from scratch — reproducibly, securely, and cleanly.

---

## 2. What You Are Building

You will provision the infrastructure for a simple two-tier web application: a public-facing server and a managed database.

The diagram below shows the target architecture:

```
Internet
    │
    ▼
[Security Group: web]
    │
    ▼
[EC2 Instance — t2.micro]  ──────►  [Security Group: db]
    │                                       │
    │                                       ▼
    │                               [RDS — db.t3.micro]
    │
    ▼
[S3 Bucket — static assets]
```

All resources must live inside a **custom VPC** — not the default AWS VPC.

---

## 3. Getting Started

1. **Fork** this repository.
2. Install [Terraform](https://developer.hashicorp.com/terraform/install) (≥ 1.5).
3. Configure your AWS credentials via environment variables or `~/.aws/credentials`. **Do not hardcode credentials anywhere.**
4. All Terraform code goes in the `infra/` directory.

---

## 4. The Assignment

### Part 1 — Networking

**Deliverable:** Terraform resources for the VPC and subnets.

Requirements:

- [ ] A **VPC** with a CIDR block of `10.0.0.0/16`.
- [ ] Two **public subnets** in different Availability Zones (for the EC2 instance).
- [ ] Two **private subnets** in different Availability Zones (for the RDS instance).
- [ ] An **Internet Gateway** attached to the VPC.
- [ ] A **Route Table** for the public subnets that routes `0.0.0.0/0` through the Internet Gateway.
- [ ] All resources tagged with `Project = "vela-payments"`.

---

### Part 2 — Compute

**Deliverable:** Terraform resources for the EC2 instance and its security group.

Requirements:

- [ ] A **Security Group** (`web-sg`) that allows:
  - Inbound HTTP on port 80 from `0.0.0.0/0`
  - Inbound HTTPS on port 443 from `0.0.0.0/0`
  - Inbound SSH on port 22 from **your IP only** (use a variable, not a hardcoded IP)
  - All outbound traffic
- [ ] An **EC2 instance** (`t2.micro`, Amazon Linux 2023) in one of the public subnets, attached to `web-sg`.
- [ ] An **IAM Instance Profile** attached to the EC2 instance. The role must only allow `s3:GetObject` and `s3:PutObject` on the S3 bucket you create in Part 4 — nothing else.

---

### Part 3 — Database

**Deliverable:** Terraform resources for an RDS instance and its security group.

Requirements:

- [ ] A **Security Group** (`db-sg`) that allows inbound traffic on port 5432 (PostgreSQL) **only from `web-sg`** — not from the internet.
- [ ] An **RDS instance** (`db.t3.micro`, PostgreSQL 15) in the private subnets.
- [ ] The database username and password must be passed in as **Terraform variables**. They must never appear as default values in the code.
- [ ] The RDS instance must **not** be publicly accessible.

> **Cost note:** RDS `db.t3.micro` is included in the AWS free tier (750 hours/month for 12 months). Destroy it when you are done with `terraform destroy`.

---

### Part 4 — Storage

**Deliverable:** Terraform resources for an S3 bucket.

Requirements:

- [ ] An **S3 bucket** for static assets.
- [ ] **Block all public access** on the bucket — it should only be reachable from the EC2 instance's IAM role.
- [ ] **Versioning** enabled on the bucket.
- [ ] The bucket name must come from a Terraform variable (bucket names are globally unique — hardcoding one will cause apply failures for other reviewers).

---

### Part 5 — Variables, Outputs & State

**Deliverable:** `variables.tf`, `outputs.tf`, and a `backend` configuration.

Requirements:

- [ ] Define all configurable values (region, CIDR blocks, your IP, DB credentials, bucket name) in `variables.tf` with descriptions. Provide an `example.tfvars` file with placeholder values.
- [ ] Define the following **outputs** in `outputs.tf`:
  - EC2 instance public IP
  - RDS endpoint
  - S3 bucket name
- [ ] Configure a [Terraform S3 backend](https://developer.hashicorp.com/terraform/language/settings/backends/s3) to store state remotely. Document how to set up the backend bucket in your README (it must be created manually before `terraform init`).

---

## 5. Verification

A reviewer will test your submission by running:

```bash
terraform init
terraform plan -var-file="example.tfvars"
```

`terraform plan` must complete with no errors. They will not run `terraform apply`, so you do not need a live environment at submission time — but your code must be correct enough to plan cleanly.

---

## 6. Bonus (Optional)

Pick **one** if you want to go further:

- **Modules:** Refactor your code to use reusable modules (e.g., a `networking` module and a `compute` module).
- **Multi-environment:** Use Terraform workspaces or separate `.tfvars` files to support a `staging` and `production` environment from the same codebase.
- **RDS snapshot:** Add a Terraform resource that takes an automated RDS snapshot before any `terraform destroy` is run.

Describe what you added and why in your README.

---

## 7. Documentation Requirements

Your final `README.md` must replace these instructions and cover:

1. **Architecture diagram** — a visual showing the VPC, subnets, EC2, RDS, and S3 with their security relationships.
2. **Setup instructions** — how to configure credentials, create the backend bucket, and run `terraform init && terraform plan`.
3. **Variable reference** — a table listing every variable, its type, and what it controls.
4. **Design decisions** — at least two choices you made and why (e.g., why private subnets for RDS, why IAM instead of access keys).

---

## 8. Submission Instructions

1. **Fork** this repository.
2. Complete all five parts in your fork.
3. Replace this README with your own documentation as outlined above.
4. Submit your repo link via the [online form](https://forms.cloud.microsoft/e/f3FF83LVz3).

---

## ⚠️ Pre-Submission Checklist

### Code

- [ ] `terraform init && terraform plan -var-file="example.tfvars"` completes with no errors.
- [ ] No AWS credentials, passwords, or real IP addresses are committed to the repository.
- [ ] `example.tfvars` is committed; any real `.tfvars` files with actual values are in `.gitignore`.
- [ ] RDS is in **private** subnets and `publicly_accessible = false`.
- [ ] SSH access is locked to a variable (not `0.0.0.0/0`).

### Documentation

- [ ] Architecture diagram is included.
- [ ] Variable reference table is present.
- [ ] Backend setup instructions are clear enough for a reviewer to follow.
- [ ] This README has been replaced with your own documentation.
- [ ] Commit history shows progress over time (not a single upload commit).
- [ ] GitHub repository is set to **Public**.
