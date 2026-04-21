# AI Agent Instructions (Sporely Desktop)

This file contains guidelines for AI coding assistants (Gemini, Claude, Cursor, etc.) working on the `sporely-py` desktop project.

## Code Review & Auditing Mindset
When proposing changes, reviewing code, or debugging, always apply a strict refactor/audit mindset. We maintain a very high standard for code quality. 

Do not praise. Look for concrete problems only, categorizing them by:
1. Duplicate logic
2. Conflicting source of truth
3. Database consistency
4. State flow problems
5. UI consistency problems
6. Dead code / stale code
7. Overgrown files / bad boundaries
8. Naming problems
9. Error handling / edge cases

## Testing & Quality Assurance
We are transitioning from manual checklist auditing to an automated safety net. As an agent, you should:
- **Promote Static Analysis:** Ensure Python code adheres to modern standards. Propose and use `Ruff` for linting/formatting and `mypy` for type-checking. Ensure your code does not introduce dead code or unused imports.
- **Write Testable Code:** Isolate pure functions, especially in complex modules like `utils/cloud_sync.py`, `utils/artsobs_uploaders.py`, and local SQLite handlers in `database/models.py`.
- **Suggest Pytest Tests:** When fixing bugs or adding features (such as local media signature generation, conflict resolution, or EXIF metadata injection), provide `pytest` unit tests to cover those edge cases.

## Localization Workflow
When asked to translate or fix missing strings, do not edit Qt Linguist `.ts` XML files directly. Instead, use the safe JSON workflow:
1. Run `python3 tools/agent_translate.py extract` to generate `missing_translations.json`.
2. Edit `missing_translations.json` to fill in the blank translations.
3. Run `python3 tools/agent_translate.py apply` to safely inject the translations back into the XML files.
4. Run `./tools/update_translations.sh` to compile the binary `.qm` files.

## Stack Constraints
- **Python 3.10+**
- **PySide6** (Qt for Python) for the UI. Use the established design system ("Slate Lab / Clinical Nocturne") defined in `ui/styles.py`.
- **SQLite3** for the local database source-of-truth.
- **Matplotlib** for analysis and plots.
- **Requests** for networking (Supabase REST APIs, iNaturalist, Cloudflare R2).
