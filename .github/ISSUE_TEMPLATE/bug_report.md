---
name: Bug Report
about: Report a bug, security edge case, or delivery reliability issue
title: "[BUG] "
labels: bug
assignees: ''
---

## Describe the Bug
A clear and concise description of what happened.

## Steps to Reproduce
1. Deployment mode (`SQLite single-node` or `PostgreSQL + Redis Docker Compose`)
2. Webhook source (`github`, `gitlab`, `stripe`, `custom`) and destination provider (`discord`, `slack`, `http`)
3. Exact request or configuration

## Expected Behavior
What you expected HookRelay to do.

## Logs / Delivery Attempt Trace
Paste sanitized `/api/deliveries/{id}` attempt history or worker logs here.
