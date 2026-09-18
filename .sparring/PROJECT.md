# Sporely Python project context

Desktop Python application using PySide6. Work in the selected Git worktree on
the run's expected branch.

Read `AGENTS.md` once per session for repository-wide implementation rules,
Python/test commands, subsystem reading routes, verification and Git policy.
Claude imports the same contract through `CLAUDE.md`. Do not duplicate those
rules here or reread this injected context unless it changes.

The run's stage brief defines the current scope. Follow `AGENTS.md` for managed
plan immutability, stage notes and role-specific reports. Push authorization
must be explicit in the current run; this configuration does not grant it.

`.sparring/project.toml` and this file are version-controlled project
configuration. `.sparring/stages/` and `.sparring/plans/` are ignored local
runtime state owned by Agent Sparring.
