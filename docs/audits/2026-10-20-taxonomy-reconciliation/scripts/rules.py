"""Shared eligibility predicates (revision 4). Used by classify.py (new
candidates) and existing_bindings.py (legacy bindings) so that both are
evaluated by the SAME rules (Codex review-3 finding 4)."""
from __future__ import annotations

import authorship as au

#: NorTaxa taxon.txt nomenclaturalStatus values that block automatic approval
#: (seen in the 1.284 archive). Allowed: '' , 'conserved', 'sanctioned'.
NT_BLOCKED_NOM = frozenset({"illegitimate", "dubious", "notvalidlypublished",
                            "orthographic", "misapplied"})
#: COL NameUsage col:nameStatus values that block. Allowed: '', 'acceptable'.
COL_BLOCKED_NAME = frozenset({"unacceptable", "doubtful", "manuscript", "not established"})


def status_blockers(nt_nom_status: str | None, col_name_status: str | None) -> list[str]:
    out = []
    if (nt_nom_status or "") in NT_BLOCKED_NOM:
        out.append(f"nomenclatural_status_nortaxa:{nt_nom_status}")
    if (col_name_status or "") in COL_BLOCKED_NAME:
        out.append(f"nomenclatural_status_col:{col_name_status}")
    return out


def authorship_blockers(auth: str, agree: list[str]) -> list[str]:
    """R4. Missing authorship needs >= 2 agreeing higher ranks, at EVERY rank
    (rev 4: no exemption for class and above)."""
    if auth in au.COMPATIBLE:
        return []
    if auth in ("missing_one", "missing_both"):
        return [] if len(agree) >= 2 else ["authorship_missing_without_classification_support"]
    return ["authorship_" + auth]
