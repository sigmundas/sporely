# Optional agent routing

Read only when selecting a role or delegation is useful. Repository scope, Git, and workflow rules
remain in `AGENTS.md`. Use only roles available in the current runtime; role
names are routing guidance, not a requirement to launch another agent.

The write-capable roles below describe a sole implementation owner, not
authorization to delegate writes. Subagents in this workspace are limited to
bounded read-heavy work and specialist review.

## Codex roles

- `act` — **Luna, low effort**. Use for small, direct, low-risk work that is already clear: targeted symbol lookup, documentation, narrow mechanical edits, focused test repairs, or one-file/tightly bounded changes. If the task reveals a contract or subsystem question, stop and hand off rather than expanding.
- `explore` — **Luna, medium effort, read-only**. Use only as the cheap scout: locate symbols and map a call/data path. Return a compact symbol map; do not solve or review the task.
- `planner` — **Terra, medium effort, read-only**. Use before ambiguous, cross-repository, persistence/schema, sync, or architecture-changing work. Produce small independently verifiable stages. Do not use for an already-clear local patch.
- `implementer` — **Terra, medium effort**. Use for an approved bounded plan stage that needs nontrivial edits and focused tests. Stop at the stage boundary.
- `reviewer` — **Terra, high effort, read-only**. Use once at a meaningful stage/landing boundary or when explicitly requested. It is not the independent top-level sparring reviewer.
- `security_reviewer` — **Sol, high effort, read-only**. Use only when a change materially touches auth/session handling, RLS/authorization, SECURITY DEFINER/public RPCs, storage access, secrets/service-role use, account binding, privacy/visibility, moderation/blocking, deletion, or another authoritative security boundary.
- **Sol is escalation-only**, not the default. Use it only when the user explicitly requests it, Terra reports unresolved high-risk ambiguity, or a release/production gate has unusual architectural or security risk.

If Claude role agents are installed, apply the same task boundaries. This
worktree does not contain `.claude/agents/`; do not assume those agents exist.
Shared Claude routing is advisory where it agrees with this repository; its
Git/push and plan-update defaults do not override `AGENTS.md`.

