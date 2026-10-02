"""Publish notice (Stage 2c, desktop).

A "Make public"/Cancel confirmation shown every time a desktop change makes
an observation public and not a draft. Desktop never publishes at once: the
change is local and the next sync publishes it, so the texts say "after the
next sync". The texts are deliberately short and match the web notice; the
exposure table below is the evidence behind them. The notice says "date",
not time of day (the server stops exposing time of day separately). Plan: sporely-web
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
* References (sporely-web ``20261001113007_share_references_by_default``):
  a reference set attached to a public, non-draft observation whose spore
  data is public is shared by default, shown with its relationship
  (compared / supports the identification / contradicts the identification)
  on the observation and its plots and in the species-page listing under the
  owner's name, unless the owner stopped sharing that set (My shared
  references) or moderation hid it. There is no separate consent step.

Suppression: one "Don't show this again" preference (``SettingsDB`` key
``show_publish_notice``, per profile database, restorable in Preferences ->
Online publishing) covers publishing, attaching a reference to a public
observation and making spore data public on a public observation. Widening
the location of an already public observation is never suppressible.
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

    spore_data_visibility: str | None = None
    has_photos: bool | None = None


def _approximate(location_precision: str | None) -> bool:
    # 'region'/'hidden' show less than fuzzed; the approximate line is the
    # cautious description for them.
    return str(location_precision or "").strip().lower() in {"fuzzed", "region", "hidden"}


def location_line(location_precision: str | None) -> str:
    if _approximate(location_precision):
        return QCoreApplication.translate(
            "PublishNotice", "An approximate location (about 1 km), shown with region or country only")
    return QCoreApplication.translate(
        "PublishNotice", "The exact location and the location name you entered")


def _photo_caveat(location_precision: str | None, has_photos: bool | None) -> list[str]:
    if _approximate(location_precision) and has_photos is not False:
        return [QCoreApplication.translate(
            "PublishNotice", "Some photos may still contain the exact position in their file data.")]
    return []


def build_publish_notice_text(location_precision: str | None, facts: PublishFacts) -> str:
    """Plain text of the "Make this observation public?" notice."""
    spore_hidden = facts.spore_data_visibility is not None and facts.spore_data_visibility != "public"
    bullets = [
        QCoreApplication.translate("PublishNotice", "Species, date, habitat, notes and your name"),
        location_line(location_precision),
        QCoreApplication.translate(
            "PublishNotice", "Your photos, microscope photos and the AI identification you selected"),
    ]
    if not spore_hidden:
        bullets.append(QCoreApplication.translate("PublishNotice", "Spore measurements and statistics"))
    lines = [QCoreApplication.translate(
        "PublishNotice",
        "After the next sync, anyone, including people who are not signed in, can see:")]
    lines += [f"• {line}" for line in bullets]
    lines.append("")
    if spore_hidden:
        lines.append(QCoreApplication.translate("PublishNotice", "Spore measurements stay hidden."))
    lines += _photo_caveat(location_precision, facts.has_photos)
    lines.append(QCoreApplication.translate(
        "PublishNotice",
        "Attached references are shared by default. You can stop sharing them under "
        "My shared references."))
    return "\n".join(lines)


def build_precision_notice_text(location_precision: str | None, has_photos: bool | None = None) -> str:
    """Plain text of the "Show a more precise location?" notice."""
    if _approximate(location_precision):
        lines = [QCoreApplication.translate(
            "PublishNotice",
            "After the next sync, this public observation will show an approximate "
            "location (about 1 km), shown with region or country only.")]
    else:
        lines = [QCoreApplication.translate(
            "PublishNotice",
            "After the next sync, this public observation will show the exact location "
            "and the location name you entered.")]
    lines += _photo_caveat(location_precision, has_photos)
    return "\n\n".join(lines)


def load_local_facts(observation_id: int | None, new_state: dict, **_ignored) -> PublishFacts:
    """Facts for a local observation as it will be after this save."""
    has_photos: bool | None = None
    spore = new_state.get("spore_data_visibility")
    if observation_id:
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
    return PublishFacts(
        spore_data_visibility=str(spore).strip().lower() if spore else None,
        has_photos=has_photos,
    )


# --- "Don't show this again" preference ---------------------------------------

PUBLISH_NOTICE_SETTING = "show_publish_notice"


def publish_notice_enabled() -> bool:
    """True unless the user chose "Don't show this again". Any read failure
    keeps the notice on (the safe default)."""
    try:
        from database.models import SettingsDB

        raw = SettingsDB.get_setting(PUBLISH_NOTICE_SETTING, "1")
    except Exception:
        return True
    return str(raw if raw is not None else "1").strip().lower() not in {"0", "false", "no", "off"}


def set_publish_notice_enabled(enabled: bool) -> None:
    from database.models import SettingsDB

    SettingsDB.set_setting(PUBLISH_NOTICE_SETTING, "1" if enabled else "0")


def show_notice(parent, title: str, text: str, accept_label: str,
                suppressible: bool = True) -> bool:
    """Accept/Cancel with a "Don't show this again" checkbox (only when
    ``suppressible``). Cancel is the default. The checkbox is saved only
    when the user accepts: cancelling never silences a future warning."""
    from PySide6.QtWidgets import QCheckBox

    box = QMessageBox(parent)
    box.setIcon(QMessageBox.Warning)
    box.setWindowTitle(title)
    box.setTextFormat(Qt.PlainText)
    box.setText(title)
    box.setInformativeText(text)
    dont_show = None
    if suppressible:
        dont_show = QCheckBox(QCoreApplication.translate("PublishNotice", "Don't show this again"))
        box.setCheckBox(dont_show)
    accept = box.addButton(accept_label, QMessageBox.AcceptRole)
    cancel = box.addButton(QMessageBox.Cancel)
    box.setDefaultButton(cancel)
    box.setEscapeButton(cancel)
    box.exec()
    accepted = box.clickedButton() is accept
    if accepted and dont_show is not None and dont_show.isChecked():
        try:
            set_publish_notice_enabled(False)
        except Exception:
            pass
    return accepted


def show_publish_notice(parent, text: str, suppressible: bool = True) -> bool:
    """Make public/Cancel (suppressible), or, with ``suppressible=False``,
    the never-suppressible "Show a more precise location?" notice.
    Cancel is the default; returns True only on the accept button."""
    if suppressible:
        title = QCoreApplication.translate("PublishNotice", "Make this observation public?")
        accept = QCoreApplication.translate("PublishNotice", "Make public")
    else:
        title = QCoreApplication.translate("PublishNotice", "Show a more precise location?")
        accept = QCoreApplication.translate("PublishNotice", "Show precise location")
    return show_notice(parent, title, text, accept, suppressible)


def confirm_publish_if_needed(
    parent,
    previous: dict | None,
    new: dict,
    load_facts: Callable[[], PublishFacts],
    *,
    show: Callable[..., bool] | None = None,
    enabled: Callable[[], bool] | None = None,
) -> bool:
    """True when no notice is needed, it is suppressed, or the owner accepted.

    Only the publishing transition (private/friends/draft -> public) can be
    suppressed. Widening the location of an already public observation
    always asks, without a "Don't show this again" box."""
    if not needs_publish_notice(previous, new):
        return True
    suppressible = not is_public(previous)
    if suppressible and not (enabled or publish_notice_enabled)():
        return True
    try:
        facts = load_facts()
    except Exception:
        facts = PublishFacts()
    if suppressible:
        text = build_publish_notice_text(new.get("location_precision"), facts)
    else:
        text = build_precision_notice_text(new.get("location_precision"), facts.has_photos)
    return bool((show or show_publish_notice)(parent, text, suppressible))


# --- Attaching a reference to an already public observation --------------------

def spore_data_public(state: dict | None) -> bool:
    return str((state or {}).get("spore_data_visibility") or "public").strip().lower() == "public"


def needs_public_attach_notice(observation: dict | None) -> bool:
    """True when a newly attached reference set would be shown publicly at
    once: the observation is public, not a draft, and its spore data is
    public (the server's qualifying-use rule)."""
    return is_public(observation) and spore_data_public(observation)


def build_attach_notice_text() -> str:
    return QCoreApplication.translate(
        "PublishNotice",
        "This observation is public. After the next sync, the attached reference is "
        "shown with it, including its relationship to your identification. You can "
        "stop sharing it under My shared references.")


def show_attach_notice(parent, text: str) -> bool:
    return show_notice(
        parent,
        QCoreApplication.translate("PublishNotice", "Attach to a public observation?"),
        text,
        QCoreApplication.translate("PublishNotice", "Attach"),
    )


def confirm_public_attach_if_needed(
    parent,
    observation: dict | None,
    roles=None,
    *,
    show: Callable[[object, str], bool] | None = None,
    enabled: Callable[[], bool] | None = None,
) -> bool:
    """One notice for one attach action (single item or a whole batch).
    ``roles`` is accepted for callers' convenience; the text names the
    relationship generically. True when no notice is needed, it is
    suppressed, or the owner chose Attach."""
    if not needs_public_attach_notice(observation):
        return True
    if not (enabled or publish_notice_enabled)():
        return True
    return bool((show or show_attach_notice)(parent, build_attach_notice_text()))


# --- Making spore data public on an already public observation -----------------

def needs_spore_public_notice(observation: dict | None, new_visibility: str | None) -> bool:
    """True when spore data turns public on a public, non-draft observation
    (``observation`` holds the state before the change)."""
    new_vis = str(new_visibility or "").strip().lower()
    return new_vis == "public" and is_public(observation) and not spore_data_public(observation)


def build_spore_public_notice_text() -> str:
    return QCoreApplication.translate(
        "PublishNotice",
        "After the next sync, anyone can see this observation's spore measurements and "
        "statistics, and the references attached to it.")


def show_spore_public_notice(parent, text: str) -> bool:
    return show_notice(
        parent,
        QCoreApplication.translate("PublishNotice", "Make spore data public?"),
        text,
        QCoreApplication.translate("PublishNotice", "Make public"),
    )


def confirm_spore_public_if_needed(
    parent,
    observation: dict | None,
    new_visibility: str | None,
    *,
    show: Callable[[object, str], bool] | None = None,
    enabled: Callable[[], bool] | None = None,
) -> bool:
    """True when no notice is needed, it is suppressed, or the owner confirmed."""
    if not needs_spore_public_notice(observation, new_visibility):
        return True
    if not (enabled or publish_notice_enabled)():
        return True
    return bool((show or show_spore_public_notice)(parent, build_spore_public_notice_text()))
