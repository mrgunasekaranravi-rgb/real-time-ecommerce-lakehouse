# Security Policy

## Supported Versions

This repository is a portfolio project demonstrating a Real-Time E-Commerce Lakehouse using Databricks, PySpark, Delta Lake, and streaming data engineering patterns.

Security-related improvements are applied to the latest version of the project.

## Reporting a Vulnerability

If you discover a security issue, avoid creating a public issue containing sensitive information.

Instead, report the issue privately to the repository owner.

Please include:

- A clear description of the issue
- Steps to reproduce it
- Potential impact
- Suggested remediation, if available

## Secrets and Credentials

Never commit:

- API keys
- Databricks access tokens
- Cloud credentials
- Database passwords
- `.env` files
- Private connection strings
- Production secrets

Sensitive configuration should be managed using environment variables or an appropriate secret-management service.

## Data Security

When adapting this project for real environments:

- Apply least-privilege access controls
- Protect customer and transaction data
- Use secure credential management
- Restrict access to production datasets
- Apply appropriate data governance policies
- Encrypt sensitive data where required

## Streaming Security

For production streaming workloads:

- Secure source and destination connections
- Protect checkpoint and state-store locations
- Restrict access to streaming infrastructure
- Monitor unauthorized data access
- Avoid exposing sensitive event payloads in logs

## Dependency Security

Project dependencies should be reviewed periodically for known vulnerabilities.

GitHub security features and dependency alerts may be used where applicable.

## Scope

This repository demonstrates real-time data engineering and Lakehouse architecture patterns for portfolio purposes.

It is not intended to represent a fully production-hardened security implementation.
