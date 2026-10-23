"""Release decisions for candidate-only taxonomy artifacts.

A release compiled from supersession records whose approval is
``candidate_build_only`` with ``release_approval: pending`` (taxonomy R1)
carries ``taxonomy_meta.release_approval = pending:candidate_build_only``.
Such an artifact can be built, validated and exported for local validation,
but promotion into the desktop bundle and a cloud export for import require an
explicit release decision bound to that exact artifact:

    {"format": "sporely-taxonomy-release-decision-v1",
     "decision": "release_approved",
     "content_release_id": "<id>", "sqlite_sha256": "<sha256>",
     "approved_by": "<owner>", "approved_at": "<date>", "decision_reference": "<ref>"}
"""
from __future__ import annotations

import json
from pathlib import Path

META_KEY = "release_approval"
PENDING_CANDIDATE_ONLY = "pending:candidate_build_only"
DECISION_FORMAT = "sporely-taxonomy-release-decision-v1"
_REQUIRED = ("approved_by", "approved_at", "decision_reference")


class ReleaseDecisionError(RuntimeError):
    """A candidate-only artifact lacks a valid release decision."""


def pending_supersession_count(ledger: dict) -> int:
    """Approved records whose release approval is still pending."""
    return sum(1 for record in ledger.get("supersessions") or []
               if record.get("review_status") == "approved"
               and record.get("release_approval") == "pending")


def require_release_decision(*, meta_value: str | None, content_release_id: str,
                             sqlite_sha256: str, decision_path: Path | None) -> dict | None:
    """Return the decision for a pending artifact, or ``None`` when none is needed.

    Raises :class:`ReleaseDecisionError` when the artifact is candidate-only and
    the decision is missing or names a different artifact.
    """
    if not meta_value:
        return None
    if meta_value != PENDING_CANDIDATE_ONLY:
        raise ReleaseDecisionError(f"unknown release_approval {meta_value!r}")
    if decision_path is None:
        raise ReleaseDecisionError(
            f"{content_release_id} is a candidate_build_only artifact (release approval "
            "pending); promotion or a cloud export for import needs a release decision "
            "bound to this content_release_id and sqlite_sha256")
    try:
        decision = json.loads(Path(decision_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ReleaseDecisionError(f"cannot read release decision {decision_path}: {exc}") from exc
    if decision.get("format") != DECISION_FORMAT or decision.get("decision") != "release_approved":
        raise ReleaseDecisionError("release decision is not a release_approved decision")
    if decision.get("content_release_id") != content_release_id \
            or decision.get("sqlite_sha256") != sqlite_sha256:
        raise ReleaseDecisionError(
            "release decision names a different artifact "
            f"({decision.get('content_release_id')}, {decision.get('sqlite_sha256')})")
    absent = [f for f in _REQUIRED if not str(decision.get(f) or "").strip()]
    if absent:
        raise ReleaseDecisionError(f"release decision carries no {', '.join(absent)}")
    return decision
