# Security notes

This repository is a **sanitized portfolio demonstration**, not a security assessment or production-ready payroll system.

- Never commit `.env`, bot tokens, Discord server IDs, real employee records, receipts, database files, or log exports.
- Use a separate test Discord application and test guild; never reuse a production token.
- Validate roles and access controls before inviting the bot to a server.
- If credentials are accidentally published, **revoke/rotate them immediately**. Removing them from the latest commit does not erase Git history.
- Run a local secrets scanner such as [Gitleaks](https://github.com/gitleaks/gitleaks) before publishing and after later commits.
- Do not use this demonstration to calculate real compensation or payroll liabilities.
- Security issues can be reported privately through the GitHub repository's security advisory feature (if enabled).
