# Design notes — sanitized overview

## Permission boundaries

Employee-facing workflows provide access to personal time and submissions, while manager-only workflows review commissions, manage scheduling, correct records, and export administrative data. Permission checks must be enforced server-side, not just in UI controls.

## Source of truth

SQLite persists workflow data, including time records, manager review decisions and payroll snapshots. In this portfolio edition, data and business rules are illustrative. Production data is not included.

## Error handling and recovery

Important mutations require explicit review/confirmation, audit records, and backup support. Historical payroll corrections should be reconciled against external payment evidence instead of automatically inventing payments.

## Known limitations

This is a monolithic application, not a security-audited reference architecture. Tests cover selected patterns and do not establish regulatory compliance. Overtime and employment classification require independent employer review.
