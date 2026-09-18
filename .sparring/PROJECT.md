# Sporely Python project context

## Repository

Desktop Python application using PySide6.

Repository root is the current Git worktree. Never search sibling worktrees or
the parent `sporely/` directory for implementation truth.

## Python environment

Use the canonical Sporely virtual environment:

`/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python`

Tests:

`/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest`

Do not install or upgrade dependencies without human approval.

## Agent instructions

`AGENTS.md` is the repository-wide coding-agent contract.

Claude also follows `CLAUDE.md` and applicable `.claude/rules/`.

Those repository instructions are authoritative for implementation conventions,
testing, Git behavior, and subsystem-specific invariants.

## Agent Sparring managed plans

When Agent Sparring is executing a managed plan:

- the active plan document is input and must not be edited by stage agents;
- stage progress, implementation notes, review findings and handoff information
  belong in Agent Sparring stage artifacts;
- only the currently assigned stage is in scope;
- a later stage must not be started early.

## Git/worktrees

Work only in the currently selected worktree and expected branch.

Do not switch branches, rewrite history or push unless explicitly authorized.

Agent Sparring runtime state under:

- `.sparring/stages/`
- `.sparring/plans/`

is local workflow bookkeeping and is ignored by Git.

`.sparring/project.toml` and `.sparring/PROJECT.md` are tracked project
configuration.