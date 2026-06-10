## Working agreements

- Do not run `npm test` after modifying JavaScript files.
- Ask for confirmation before adding new production dependencies.
- Do not run heavy build steps, including Capacitor syncs, PyInstaller, Docker builds, full app builds, packaging commands, or dependency installation, unless explicitly requested.
- Keep patches narrow. If a task touches multiple workflows or large UI files, propose staged patches and stop after the current stage.
- Do not rewrite or refactor unrelated code while fixing a bug. Preserve existing behavior unless the prompt explicitly asks for a behavior change.
- For sporely-py, always use the project virtual environment:
/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python
/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest
Ask for confirmation before installing or upgrading packages in .venv.
For syntax checks, use ./.venv/bin/python -m py_compile <touched files>