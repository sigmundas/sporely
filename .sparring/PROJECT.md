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

## Human-gated verification under plan runs

- A stage whose human check (live Supabase write, cross-client sync, live
  canary, interactive behavior, …) is held by an engine gate of the approved
  plan (`gates_before` / `completion_gates`) may be committed, pushed and
  accepted on its automated evidence. The gate holds the next stage or plan
  completion until the user records a pass; a failed check is fixed in a new
  stage or reverted. This is the exception in `AGENTS.md` (Working agreements).
- Agents never perform the human check themselves, including any live
  Supabase write. A human check not held by such a gate keeps the
  `AGENTS.md` rule: leave the work uncommitted until the user confirms.
