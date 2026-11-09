# ETBeast — Operations Automation & Data Integrity (Portfolio Edition)

**Python · discord.py · SQLite · Linux · role-based authorization · audit trails · regression testing**

ETBeast is an employee-operations Discord bot I developed to replace manual administrative tasks with structured workflows for timekeeping, scheduling, commissions, expenses, and payroll reviews. This **portfolio edition** demonstrates selected production-inspired patterns using **anonymized branding and fictional compensation settings**.

> **Privacy note:** This repository does **not** contain employee records, live credentials, production databases, private receipts, payment exports, or internal payroll recovery tools. The portfolio copy is not intended for production payroll use, and you should not treat its example pay settings as real business rules.

## Why I built it

I worked on internal systems for a livestream commerce team and needed reliable employee time records, manager approvals, and auditable adjustments. Development required balancing employee usability with data accuracy: correcting missed hours without fabricating timestamps, preventing duplicate approvals, and making payroll errors traceable.

## Technical highlights

| Area | Implementation / engineering focus |
| --- | --- |
| Role-based authorization | Manager-only review, approval, correction, and export workflows; member-facing private responses |
| Data integrity | SQLite-backed ledgers, idempotency/duplicate guards, void-instead-of-delete audit trails |
| Payroll controls | Weekly snapshots, employee-level breakdowns, explicit review of unfinalized entries |
| Timekeeping | UTC elapsed time and US Eastern payroll-week boundaries, including daylight saving time |
| File validation | Receipt and supporting-photo MIME/content checks; reject mismatched or unsupported formats |
| Operations | Scheduled shifts, reminders, commission approvals, expenses, and exports |
| Reliability | Database backups and offline regression tests for accounting edge cases |

## Architecture

```text
Employee / Manager in Discord
         |
  discord.py commands, menus
         |
  role checks & input validation
         |
  workflow logic (shifts, reviews, expenses, payroll)
         |
     SQLite database
         |
  backups / sanitized exports / audit history
```

A single-file implementation (`bot.py`) reflects the project's iterative development. Breaking workflows into packages, improving migrations, and expanding the test suite are future maintenance goals.

## Security-related design decisions

- **Least privilege in application UX:** sensitive manager actions check permissions; employee responses use private/ephemeral messages where supported.
- **Data minimization:** credentials are read from environment variables; the sample repo excludes customer/employee data and attachments.
- **File handling:** uploaded receipt content is validated against advertised media type and size constraints before storage.
- **Auditability:** manual hour entries, voids, reviews, and payroll snapshots maintain accountability for changes.
- **Time accuracy:** timestamps use UTC for elapsed-time math and Eastern time for business reporting and payroll boundaries.

These controls are examples of practical secure coding, **not** a claim that this bot is formally security-audited or payroll-compliance certified.

## Running locally (optional, with a disposable test Discord server)

1. Install Python 3.11+ and create a virtual environment.
2. Install dependencies: `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env`; use a **new test-bot token** and your test server's IDs. Never use production credentials.
4. Run `python bot.py` from a private development environment.

**Do not deploy this portfolio edition over a live bot or point it at a production database.** The example configuration deliberately differs from real operations.

## Tests

Run `python -m unittest discover -s tests -v` for source-level privacy and structure smoke tests. These are not a substitute for full integration tests.

## What I learned

The most significant engineering lesson was the importance of immutable/traceable payroll approval states. When a legacy commission assignment affected historical statements, I used database-level reconciliation, preserved prior records, and added constraints to separate approved snapshots from later changes.

## Privacy and ownership

This public copy removes identifiable infrastructure and sensitive operational content. Production deployments and private business data are **not** published. Before reuse, verify you have the relevant owner's permission and review dependencies, permissions, and local data retention requirements.
