# QueueCare-QA

# 🏥 QueueCare — Technical Assessment

> **QA Engineering Challenge: Build It. Then Break It.**

---

## What Is This?

This is a combined development and test automation assessment. You will build a small but real working application — a Clinic Appointment System called **QueueCare** — and then write a full automated test suite against your own code.

This is intentional. Real QA engineers understand systems well enough to break them. That is what we are testing here.

---

## The Two-Part Challenge

### Part 1 — Build the Application

Build a simplified clinic appointment system with the following features:

#### 🔐 Authentication

- Register a new user (name, email, password, role)
- Login and receive a token (JWT or session-based)
- Protect all endpoints — unauthenticated requests must be rejected

#### 📅 Appointment Management (CRUD)

- Create an appointment
- View all appointments
- View a single appointment by ID
- Update an appointment (date, reason, doctor)
- Cancel / delete an appointment

#### ⏱️ Queue Logic

- Automatically assign a queue number when a booking is created
- View today's queue ordered by queue number
- Allow staff to mark a patient as served

#### 🔒 Role-Based Authorization

- **Patient** — can only manage their own appointments
- **Staff / Admin** — can view and manage all appointments, and mark patients as served

> **Keep it simple.** A clean REST API with basic HTML pages is perfectly fine. You do not need a polished UI. Spend your energy on correctness and test quality.

---

### Part 2 — Test Automation

Write automated tests against the application you just built. You are testing your own code — we expect you to know where the weak spots are.

#### 🔌 API Automation

You must cover all three categories:

**Happy Path**

- Register → Login → receive valid token
- Create appointment → verify queue number is assigned
- Fetch all appointments → verify role-based filtering works
- Fetch single appointment by ID
- Staff marks patient as served → verify status updates

**Negative Cases**

- Login with wrong password → expect `401`
- Login with non-existent email → expect `401`
- Create appointment with missing fields → expect `400`
- Access protected endpoint with no token → expect `401`
- Access protected endpoint with invalid token → expect `401`
- Patient accesses another patient's appointment → expect `403`
- Patient tries to mark a patient as served → expect `403`
- Fetch appointment with non-existent ID → expect `404`

**Edge Cases**

- Book an appointment in the past → should be rejected
- Duplicate booking on the same day for the same patient → should be rejected
- Invalid date format submitted → should return a clear error
- Reschedule to a past date → should be rejected
- Cancel an already-cancelled appointment → should be handled gracefully
- Mark an already-served patient as served again → should be handled gracefully
- Re-book on the same day after a cancellation → should be allowed

#### 🖥️ UI Automation

Automate the following flows through the browser:

- **Login flow** — valid credentials, invalid credentials, empty form submission
- **Create appointment** — fill and submit the form, verify the booking appears
- **Form validation** — empty fields, invalid input formats
- **Update or cancel** — make a change and verify it is reflected in the UI

> UI tests must be stable. Use meaningful selectors — data attributes, labels, or accessible roles. Avoid relying on CSS classes or element positions. Flaky tests are a red flag.

---

### Part 3 — Test Report

Write a short, honest report. This is not a formality — we read it carefully.

| Section                    | What to Cover                                                       |
| -------------------------- | ------------------------------------------------------------------- |
| **What You Built**         | Your stack, architecture, and key decisions                         |
| **What You Tested**        | Which parts you covered and which you skipped, and why              |
| **What You Automated**     | Which scenarios are automated vs manual, and why you drew that line |
| **Bugs Found**             | Any bugs or unexpected behaviour you found in your own system       |
| **What You Would Improve** | Given more time — what would you test, fix, or refactor             |

> ⚠️ **Strong candidates find bugs in their own systems.** If your report says "no bugs found", that is almost always a sign of incomplete testing — not perfect code.

---

## Technology — Your Choice

Use whatever you know best. The following are suggestions only.

### Backend

| Option             | Notes                                   |
| ------------------ | --------------------------------------- |
| Node.js + Express  | Fast setup, large ecosystem             |
| Python + FastAPI   | Built-in validation, auto docs          |
| Python + Flask     | Lightweight, minimal boilerplate        |
| Java + Spring Boot | Strongly typed, good if you know Java   |
| Go + Gin / Fiber   | Fast, good to demonstrate Go experience |
| Ruby on Rails      | Rapid development                       |
| PHP + Laravel      | Mature web API ecosystem                |

### Database

| Option                | Notes                                                 |
| --------------------- | ----------------------------------------------------- |
| SQLite                | Zero setup, file-based, perfect for assessments       |
| PostgreSQL            | Production-grade, widely supported                    |
| MongoDB               | Flexible schema, easy to start                        |
| In-memory (array/map) | Simplest option — perfectly valid for this assessment |

### API Testing

| Tool                      | Language                   |
| ------------------------- | -------------------------- |
| Jest + Supertest          | JavaScript                 |
| Pytest + Requests / HTTPX | Python                     |
| RestAssured               | Java                       |
| Postman + Newman          | Any — GUI + CLI runner     |
| Playwright (API mode)     | JavaScript / Python / Java |
| Karate DSL                | Any — Gherkin-style        |

### UI Automation

| Tool               | Language                                 |
| ------------------ | ---------------------------------------- |
| Playwright         | JavaScript / Python / Java — recommended |
| Cypress            | JavaScript                               |
| Selenium WebDriver | Java / Python                            |
| Puppeteer          | JavaScript                               |
| Robot Framework    | Python                                   |

### Frontend

Plain HTML + JavaScript is completely fine. Other options:

- React.js or Vue.js if you prefer a structured frontend
- Server-side templates (Jinja, EJS, Thymeleaf, Blade)
- No UI at all — a documented Postman collection is acceptable

---

## Deliverables

- [ ] `README.md` — instructions to install, run, and test everything from scratch
- [ ] `TEST_REPORT.md` — your written test report
- Submission link: https://forms.cloud.microsoft/e/vU0px4a5du

### Your README Must Include

- Prerequisites (runtime versions, browser requirements, etc.)
- How to install dependencies
- How to start the application
- How to run API tests
- How to run UI tests
- Any environment variables needed
- Default test credentials (if applicable)

> If a reviewer cannot run your project because of missing setup instructions, it will affect your score. Treat the README as part of the deliverable.

---

## How You Are Evaluated

We are not just checking whether the tests pass.

| Dimension                | What We Look For                                                                 |
| ------------------------ | -------------------------------------------------------------------------------- |
| **Engineering Quality**  | Clean, readable code. Sensible structure. Simple is better than clever.          |
| **QA Thinking**          | Test coverage depth. Identification of edge cases and real failure modes.        |
| **Automation Skills**    | Stable, well-structured tests. Good assertions. Reusable helpers.                |
| **Honesty & Reflection** | Accurate test report. Self-found bugs documented. Realistic coverage assessment. |

---

## Final Notes

- A small, well-tested system beats a large, poorly-tested one every time
- Negative and edge case tests matter more than happy path tests — anyone can write a happy path test
- Acknowledging a known bug in your report is far better than pretending it does not exist
- If something does not work, document it and explain why

---

_Good luck. Build something you are proud to explain._
