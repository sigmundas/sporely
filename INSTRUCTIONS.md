# AI Coding Instructions: Sporely Desktop (sporely-py)

## Audit Mindset (STRICT)
- **No praise.** No conversational filler. Look for concrete problems only.
- **Categorize problems:** 1. Duplicate logic, 2. Conflicting source of truth, 3. DB consistency, 4. State flow, 5. UI consistency, 6. Dead code, 7. Bad boundaries, 8. Naming, 9. Error handling, 10. Refactor opportunities.
- **Prioritize:** Distinguish "must fix" bugs from style opinions or "cleanup".

## Tech Stack & Constraints
- **Core:** Python 3.10+, PySide6 (STRICT: Do not use PyQt5, PyQt6, or PySide2).
- **Database:** Local SQLite3 (`sqlite3`), wrapping custom DB models.
- **Analysis/Images:** Matplotlib, Pillow (PIL), OpenCV. `QImageReader` for previews.
- **Networking:** `requests` for REST APIs.

## Critical GUI Patterns
- **Layouts:** NEVER use absolute positioning (`.setGeometry()`, `.move()`, fixed pixels). Always use Qt Layouts (`QVBoxLayout`, `QHBoxLayout`, etc.).
- **Sizing:** Avoid `setFixedWidth()` and `setFixedHeight()`. Prefer `QSizePolicy.Expanding` and use layout stretch factors. 
- **Splitters:** Use `QSplitter` instead of fixed sidebars. Ensure child widgets don't have large minimum widths. Persist size manually via `QSettings`.
- **State & Recursion:** Prevent UI signal recursion during programmatic updates using blocking flags (e.g., `self._loading = True`) or `widget.blockSignals(True)`.
- **Threading:** Offload heavy tasks (network, AI) to `QThread`. Clean up threads gracefully on dialog close. Use `QTimer.singleShot` to defer UI initialization.
- **Image Exif:** Always respect EXIF orientation (use `QImageReader.setAutoTransform(True)`).
- **Localization:** Wrap ALL user-facing text in `self.tr("Text")`.

## Code Standards
- **Typing:** Use strict native type hints for all signatures and class variables (`list[str]`, `dict`, `int | None`).
- **Naming:** Prefix private internal UI builders and event slots with a single underscore (e.g., `_build_left_panel`, `_on_button_clicked`).
- **Structures:** Prefer `dataclasses` for data passing between UI and internal logic.

## Sync & Cloud Invariants
- **Account Binding:** Desktop binds to a cloud account via `linked_cloud_user_id` inside `app_settings.json`.
- **Conflict Rules:** Auto-merge non-colliding image metadata in favor of desktop. User is prompted for true overlap collisions.
- **Privacy Control:** Desktop tracks `is_draft` (workflow state), `sharing_scope` (`private`, `friends`, `public`), and `location_precision`.