"""Literature discovery by concept ID or published name, never identity repair."""
from __future__ import annotations

import re
from typing import Any


def matches_library_taxon(candidate: Any, *, taxon_id: str | None, taxon_name: str) -> bool:
    """Match a known ID or the complete target name at the start of a treatment.

    Authorities may follow a binomial. Infraspecific names require their own
    complete match; a species search can include them as relevant literature.
    Name relevance does not assert concept equivalence or alter either ID.
    """
    target_id = str(taxon_id or "").strip()
    if target_id and str(getattr(candidate, "taxon_id", "") or "").strip() == target_id:
        return True
    target = " ".join(taxon_name.split()).casefold()
    published = " ".join(str(getattr(candidate, "name_as_published", "") or "").split()).casefold()
    return bool(target and re.match(re.escape(target) + r"(?:$|[\s(])", published))
