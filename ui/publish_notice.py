"""Publish notice (Stage 2c, desktop).

A Publish/Cancel confirmation shown every time a desktop change makes an
observation public and not a draft. Plan: sporely-web
``docs/plans/active/2026-10-01-reference-sharing-roles-and-publish-notice.md``,
"Publish notice (web and desktop)".

Verified public exposure of a published (visibility='public', is_draft=false)
observation, from the server read surfaces in sporely-web
``supabase/migrations`` (the same table as the web candidate's handoff and
security review). Drafts and private/friends observations are on no public
surface.

* Always, to anyone including signed-out visitors
  (``observations_community_view``, anon grant 20260803120000, latest
  definition 20260930181742): species, date, captured_at (time of day),
  created_at, author name, habitat, notes, uncertain flag, red-list status,
  and the AI selection fields (service, taxon, name, probability, time).
* ``location_precision='exact'``: location text and exact coordinates (same
  view; ``get_public_observation`` 20260721120000).
  ``'fuzzed'``: region or country name instead of the location text, and
  coordinates rounded to 2 decimals (~1 km) (same sources).
* Photos: thumbnails and full-size media for every non-deleted image, served
  as stored bytes (``search_public_observation_images``, 20260809120000);
  ``storage_exif_safe`` gates only the legacy direct full URL. Desktop cannot
  see the per-image cloud flag, so with an approximate location and any photo
  the notice adds the file-data caveat.
* Microscope photos (with scale bars) and preparation details: always public.
* ``spore_data_visibility='public'``: spore measurements, statistics, points
  and mosaic (view; ``get_public_observation``, ``get_public_observation_spore_summaries``
  20260713120000). Otherwise withheld.
* Comments: readable and writable by signed-in users only
  (``phase7_comments_read`` TO authenticated, 20260812120000).
* References: a use appears (``search_public_observation_references``,
  20260930224506) only when the owner's contribution for that set and the
  observation's species is shared with consent and the use qualifies (which
  needs spore data public). Unshared attached references are never public.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtWidgets import QMessageBox



def _scope(state: dict | None) -> str:
    state = state or {}
    return str(state.get("sharing_scope") or state.get("visibility") or "").strip().lower()


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "t"}
    return bool(value)


def is_public(state: dict | None) -> bool:
    """Public and not a draft. A missing ``is_draft`` counts as a draft."""
    if not state:
        return False
    return _scope(state) == "public" and not _truthy(state.get("is_draft", True))


# How much location a public observation shows; the server views treat a
# missing value as 'exact' (coalesce(location_precision, 'exact')).
_PRECISION_RANK = {"hidden": 0, "region": 1, "fuzzed": 2, "exact": 3}


def precision_rank(state: dict | None) -> int:
    raw = str((state or {}).get("location_precision") or "").strip().lower()
    return _PRECISION_RANK.get(raw, _PRECISION_RANK["exact"])


def needs_publish_notice(previous: dict | None, new: dict | None) -> bool:
    """True for a change that makes the observation public and not a draft,
    or that makes a public, non-draft observation's location more precise."""
    if not is_public(new):
        return False
    if not is_public(previous):
        return True
    return precision_rank(new) > precision_rank(previous)


@dataclass(frozen=True)
class PublishFacts:
    """What the notice needs to know. ``None`` means unknown."""

    attached_uses: tuple[tuple[str, str], ...] | None = None  # (set id, role)
    taxon_id: int | None = None  # species after this save
    taxon_known: bool = False
    spore_data_visibility: str | None = None
    has_photos: bool | None = None
    contributions: list[dict] | None = None  # owner list on status ok, else None


def resolve_already_shared(facts: PublishFacts) -> tuple[str, list[tuple[dict, str]]]:
    """``("yes", matches)``, ``("no", [])`` or ``("unknown", [])``."""
    if facts.attached_uses is not None and not facts.attached_uses:
        return "no", []
    if facts.spore_data_visibility is not None and facts.spore_data_visibility != "public":
        return "no", []  # no use qualifies without public spore data
    if facts.attached_uses is None or not facts.taxon_known:
        return "unknown", []
    if facts.taxon_id is None:
        return "no", []
    if facts.contributions is None:
        return "unknown", []
    shared = [c for c in facts.contributions if isinstance(c, dict) and c.get("status") == "shared"]
    if any("source_measurement_set_id" not in c for c in shared):
        return "unknown", []
    matches = []
    for contribution in shared:
        if str(contribution.get("sporely_taxon_id")) != str(facts.taxon_id):
            continue
        for set_id, role in facts.attached_uses:
            if str(contribution.get("source_measurement_set_id")) == str(set_id):
                matches.append((contribution, role))
    if facts.spore_data_visibility is None and matches:
        return "unknown", []
    return ("yes", matches) if matches else ("no", [])


def _role_text(role: str) -> str:
    return {
        "compared": QCoreApplication.translate("PublishNotice", "compared"),
        "supports_identification": QCoreApplication.translate("PublishNotice", "supports the identification"),
        "contradicts": QCoreApplication.translate("PublishNotice", "contradicts the identification"),
    }.get(str(role or ""), str(role or ""))


def build_publish_notice_text(location_precision: str | None, facts: PublishFacts) -> str:
    """Plain text of the notice for the chosen settings and facts."""
    # 'region'/'hidden' show less than fuzzed; the approximate text and the
    # photo caveat are the cautious description for them.
    fuzzed = str(location_precision or "").strip().lower() in {"fuzzed", "region", "hidden"}
    exposed = [
        QCoreApplication.translate("PublishNotice", "An approximate location: coordinates rounded to about 1 km and only the "
            "region or country name, not the location name you entered")
        if fuzzed else
        QCoreApplication.translate("PublishNotice", "The exact location: precise coordinates and the location name you entered"),
        QCoreApplication.translate("PublishNotice", "Species, date and time of day, the date you created it, habitat, notes, the "
            "uncertain flag, red-list status and your name"),
        QCoreApplication.translate("PublishNotice", "The AI identification you selected and its probability"),
        QCoreApplication.translate("PublishNotice", "Your photos, as thumbnails and at full size"),
        QCoreApplication.translate("PublishNotice", "Microscope photos (including scale bars) and preparation details"),
    ]
    spore_hidden = facts.spore_data_visibility is not None and facts.spore_data_visibility != "public"
    if not spore_hidden:
        exposed.append(QCoreApplication.translate("PublishNotice", "Spore measurements, statistics, measurement points and the spore mosaic"))
    notes = []
    if fuzzed and facts.has_photos is not False:
        notes.append(QCoreApplication.translate("PublishNotice", "Some photos may still contain the exact position in their file data."))
    if spore_hidden:
        notes.append(QCoreApplication.translate("PublishNotice", "Spore measurements, statistics, measurement points and the spore mosaic "
                         "stay hidden. Microscope photos and preparation details are still public."))
    notes.append(QCoreApplication.translate("PublishNotice", "Signed-in users can read and write comments on it."))
    notes.append(QCoreApplication.translate("PublishNotice", "References attached to this observation stay private unless you share "
                     "them one by one with \"Share publicly…\"."))
    kind, matches = resolve_already_shared(facts)
    if kind == "yes":
        for contribution, role in matches:
            name = " · ".join(
                str(v).strip() for v in (contribution.get("canonical_scientific_name"),
                                         contribution.get("source_short_label"))
                if isinstance(v, str) and v.strip()
            ) or QCoreApplication.translate("PublishNotice", "(unnamed)")
            notes.append(QCoreApplication.translate("PublishNotice", "You have already shared the reference {name}. It will appear on "
                             "this observation as \"{role}\".").format(name=name, role=_role_text(role)))
    elif kind == "unknown":
        notes.append(QCoreApplication.translate("PublishNotice", "References you have shared may appear on this observation."))
    notes.append(QCoreApplication.translate("PublishNotice", "The change takes effect after the next sync."))
    lines = [QCoreApplication.translate("PublishNotice", "Anyone, including people who are not signed in, will be able to see:")]
    lines += [f"• {line}" for line in exposed]
    lines.append("")
    lines += notes
    return "\n".join(lines)


OWNER_LIST_TIMEOUT_S = 3.0


def load_owner_contributions(client, *, timeout: float | None = None) -> list[dict] | None:
    """The owner's contributions, or ``None`` on failure, rate limit or timeout.

    The call runs on a daemon thread and is abandoned after ``timeout``
    seconds so the notice never freezes the dialog; ``None`` gives the
    cautious line.
    """
    if client is None:
        return None
    import threading

    box: dict[str, object] = {}

    def run() -> None:
        try:
            box["result"] = client.list_my_shared_reference_contributions()
        except Exception as exc:  # noqa: BLE001 - any failure is "unknown"
            box["error"] = exc

    worker = threading.Thread(target=run, name="publish-notice-owner-list", daemon=True)
    worker.start()
    worker.join(OWNER_LIST_TIMEOUT_S if timeout is None else timeout)
    if worker.is_alive() or "error" in box:
        return None
    result = box.get("result")
    if not isinstance(result, dict) or result.get("status") != "ok":
        return None
    rows = result.get("contributions")
    return rows if isinstance(rows, list) else None


def _default_client():
    try:
        from utils.cloud_sync import SporelyCloudClient

        return SporelyCloudClient.from_stored_credentials()
    except Exception:
        return None


def load_local_facts(
    observation_id: int | None,
    new_state: dict,
    *,
    client_getter: Callable[[], object] = _default_client,
) -> PublishFacts:
    """Facts for a local observation as it will be after this save."""
    attached: tuple[tuple[str, str], ...] | None = ()
    has_photos: bool | None = None
    spore = new_state.get("spore_data_visibility")
    if observation_id:
        try:
            from database.reference_library import ObservationReferenceUseRepository

            attached = tuple(
                (str(use.reference_measurement_set_id), str(use.role or ""))
                for use in ObservationReferenceUseRepository.list_for_observation(int(observation_id))
            )
        except Exception:
            attached = None
        try:
            from database.models import ImageDB, ObservationDB

            has_photos = bool(ImageDB.get_images_for_observation(int(observation_id)))
            if spore is None:
                row = ObservationDB.get_observation(int(observation_id)) or {}
                spore = row.get("spore_data_visibility") or "public"
        except Exception:
            has_photos = None
    else:
        spore = spore or "public"
    if has_photos is None and new_state.get("has_photos") is not None:
        has_photos = bool(new_state.get("has_photos"))
    taxon_known = "sporely_taxon_id" in new_state
    taxon_id = None
    if taxon_known:
        try:
            taxon_id = int(new_state.get("sporely_taxon_id") or 0) or None
        except (TypeError, ValueError):
            taxon_known = False
    contributions = None
    if attached and taxon_id:
        contributions = load_owner_contributions(client_getter())
    return PublishFacts(
        attached_uses=attached,
        taxon_id=taxon_id,
        taxon_known=taxon_known,
        spore_data_visibility=str(spore).strip().lower() if spore else None,
        has_photos=has_photos,
        contributions=contributions,
    )


def show_publish_notice(parent, text: str) -> bool:
    """Publish/Cancel. Cancel is the default; returns True only on Publish."""
    box = QMessageBox(parent)
    box.setIcon(QMessageBox.Warning)
    box.setWindowTitle(QCoreApplication.translate("PublishNotice", "Publish this observation?"))
    box.setTextFormat(Qt.PlainText)
    box.setText(QCoreApplication.translate("PublishNotice", "Publish this observation?"))
    box.setInformativeText(text)
    publish = box.addButton(QCoreApplication.translate("PublishNotice", "Publish"), QMessageBox.AcceptRole)
    cancel = box.addButton(QMessageBox.Cancel)
    box.setDefaultButton(cancel)
    box.setEscapeButton(cancel)
    box.exec()
    return box.clickedButton() is publish


def confirm_publish_if_needed(
    parent,
    previous: dict | None,
    new: dict,
    load_facts: Callable[[], PublishFacts],
    *,
    show: Callable[[object, str], bool] | None = None,
) -> bool:
    """True when no notice is needed or the owner chose Publish."""
    if not needs_publish_notice(previous, new):
        return True
    try:
        facts = load_facts()
    except Exception:
        facts = PublishFacts()
    text = build_publish_notice_text(new.get("location_precision"), facts)
    return bool((show or show_publish_notice)(parent, text))

