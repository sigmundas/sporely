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
* References (sporely-web ``20261001113007_share_references_by_default``):
  a reference set attached to a public, non-draft observation whose spore
  data is public is shared by default, shown with its relationship
  (compared / supports the identification / contradicts the identification)
  on the observation and its plots and in the species-page listing under the
  owner's name, unless the owner stopped sharing that set (My shared
  references) or moderation hid it. There is no separate consent step.

Suppression: one "Don't show this again" preference (``SettingsDB`` key
``show_publish_notice``, per profile database, restorable in Preferences ->
Online publishing) covers every notice in this module: publishing,
widening the location, attaching a reference to a public observation and
making spore data public on a public observation. They are all the same
"what becomes public" warning, so one switch is what a user expects.
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

    attached_roles: tuple[str, ...] | None = None  # role per attached set
    spore_data_visibility: str | None = None
    has_photos: bool | None = None
    uses_stopped_reference: bool | None = None


def role_text(role: str) -> str:
    return {
        "compared": QCoreApplication.translate("PublishNotice", "compared"),
        "supports_identification": QCoreApplication.translate("PublishNotice", "supports the identification"),
        "contradicts": QCoreApplication.translate("PublishNotice", "contradicts the identification"),
    }.get(str(role or ""), str(role or ""))


def _roles_summary(roles) -> str:
    """``2 × compared, 1 × contradicts the identification`` (stable order)."""
    order = ["compared", "supports_identification", "contradicts"]
    counts: dict[str, int] = {}
    for role in roles:
        counts[str(role or "")] = counts.get(str(role or ""), 0) + 1
    keys = [k for k in order if k in counts] + sorted(k for k in counts if k not in order)
    return ", ".join(f"{counts[k]} × {role_text(k)}" for k in keys)


def references_notes(spore_data_visibility: str | None, attached_roles=None,
                     uses_stopped_reference: bool | None = None) -> list[str]:
    """The reference lines shared by every notice in this module."""
    spore_hidden = spore_data_visibility is not None and spore_data_visibility != "public"
    if spore_hidden:
        return [QCoreApplication.translate(
            "PublishNotice",
            "Reference sets attached to it are not shown on it while its spore data "
            "is not public. A species-page listing already made for one of these "
            "sets stays public until you stop sharing that set under My shared "
            "references.")]
    notes = [QCoreApplication.translate(
        "PublishNotice",
        "Reference sets attached to it are shared by default: shown on the "
        "observation and its plots with their relationship (compared, supports the "
        "identification or contradicts the identification), and in the species-page "
        "listing under your name. This includes references you attach later. You "
        "can stop sharing a reference set under My shared references.")]
    if attached_roles:
        notes.append(QCoreApplication.translate(
            "PublishNotice", "Attached now: {roles}.").format(roles=_roles_summary(attached_roles)))
    if uses_stopped_reference is True:
        notes.append(QCoreApplication.translate(
            "PublishNotice", "References you stopped sharing are not shown publicly."))
    return notes


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
    notes += references_notes(facts.spore_data_visibility, facts.attached_roles,
                              facts.uses_stopped_reference)
    notes.append(QCoreApplication.translate("PublishNotice", "The change takes effect after the next sync."))
    lines = [QCoreApplication.translate("PublishNotice", "Anyone, including people who are not signed in, will be able to see:")]
    lines += [f"• {line}" for line in exposed]
    lines.append("")
    lines += notes
    return "\n".join(lines)


OWNER_LIST_TIMEOUT_S = 3.0


def load_stopped_set_ids(client_or_getter, *, timeout: float | None = None) -> set[str] | None:
    """Set ids the owner stopped sharing, or ``None`` on failure, rate limit
    or timeout.

    The call runs on a daemon thread and is abandoned after ``timeout``
    seconds so the notice never freezes the dialog; ``None`` just omits the
    "stopped" line.
    """
    if client_or_getter is None:
        return None
    import threading

    box: dict[str, object] = {}

    def run() -> None:
        try:
            # Creating the client (stored credentials, token refresh) also
            # happens here, inside the bound, never on the UI thread.
            client = client_or_getter() if callable(client_or_getter) else client_or_getter
            if client is None:
                box["error"] = RuntimeError("no cloud client")
                return
            box["result"] = client.list_my_reference_sharing()
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
    rows = result.get("sets")
    if not isinstance(rows, list):
        return None
    return {
        str(r.get("source_measurement_set_id"))
        for r in rows
        if isinstance(r, dict) and (r.get("stopped_at") or r.get("status") == "stopped")
    }


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
    spore_text = str(spore).strip().lower() if spore else None
    uses_stopped = None
    if attached and spore_text == "public":
        stopped = load_stopped_set_ids(client_getter)
        if stopped is not None:
            uses_stopped = any(set_id in stopped for set_id, _role in attached)
    return PublishFacts(
        attached_roles=tuple(role for _set_id, role in attached) if attached is not None else None,
        spore_data_visibility=spore_text,
        has_photos=has_photos,
        uses_stopped_reference=uses_stopped,
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


def show_notice(parent, title: str, text: str, accept_label: str) -> bool:
    """Accept/Cancel with a "Don't show this again" checkbox. Cancel is the
    default. The checkbox is saved only when the user accepts: cancelling
    never silences a future warning."""
    from PySide6.QtWidgets import QCheckBox

    box = QMessageBox(parent)
    box.setIcon(QMessageBox.Warning)
    box.setWindowTitle(title)
    box.setTextFormat(Qt.PlainText)
    box.setText(title)
    box.setInformativeText(text)
    dont_show = QCheckBox(QCoreApplication.translate("PublishNotice", "Don't show this again"))
    box.setCheckBox(dont_show)
    accept = box.addButton(accept_label, QMessageBox.AcceptRole)
    cancel = box.addButton(QMessageBox.Cancel)
    box.setDefaultButton(cancel)
    box.setEscapeButton(cancel)
    box.exec()
    accepted = box.clickedButton() is accept
    if accepted and dont_show.isChecked():
        try:
            set_publish_notice_enabled(False)
        except Exception:
            pass
    return accepted


def show_publish_notice(parent, text: str) -> bool:
    """Publish/Cancel. Cancel is the default; returns True only on Publish."""
    return show_notice(
        parent,
        QCoreApplication.translate("PublishNotice", "Publish this observation?"),
        text,
        QCoreApplication.translate("PublishNotice", "Publish"),
    )


def confirm_publish_if_needed(
    parent,
    previous: dict | None,
    new: dict,
    load_facts: Callable[[], PublishFacts],
    *,
    show: Callable[[object, str], bool] | None = None,
    enabled: Callable[[], bool] | None = None,
) -> bool:
    """True when no notice is needed, it is suppressed, or the owner chose Publish."""
    if not needs_publish_notice(previous, new):
        return True
    if not (enabled or publish_notice_enabled)():
        return True
    try:
        facts = load_facts()
    except Exception:
        facts = PublishFacts()
    text = build_publish_notice_text(new.get("location_precision"), facts)
    return bool((show or show_publish_notice)(parent, text))


# --- Attaching a reference to an already public observation --------------------

def spore_data_public(state: dict | None) -> bool:
    return str((state or {}).get("spore_data_visibility") or "public").strip().lower() == "public"


def needs_public_attach_notice(observation: dict | None) -> bool:
    """True when a newly attached reference set would be shown publicly at
    once: the observation is public, not a draft, and its spore data is
    public (the server's qualifying-use rule)."""
    return is_public(observation) and spore_data_public(observation)


def build_attach_notice_text(roles) -> str:
    roles = [str(r or "") for r in (roles or [])]
    if len(roles) <= 1:
        lead = QCoreApplication.translate(
            "PublishNotice",
            "This observation is public. The reference set you attach will be shown "
            "publicly on it, with its relationship ({role}), in its plots and in the "
            "species-page listing under your name.").format(
            role=role_text(roles[0] if roles else "compared"))
    else:
        lead = QCoreApplication.translate(
            "PublishNotice",
            "This observation is public. The {count} reference sets you attach will "
            "be shown publicly on it, each with its relationship ({roles}), in its "
            "plots and in the species-page listing under your name.").format(
            count=len(roles), roles=_roles_summary(roles))
    return "\n\n".join([
        lead,
        QCoreApplication.translate(
            "PublishNotice",
            "A reference set you have stopped sharing, or one hidden by moderation, "
            "is not shown. You can stop sharing a reference set under My shared "
            "references."),
        QCoreApplication.translate("PublishNotice", "The change takes effect after the next sync."),
    ])


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
    roles,
    *,
    show: Callable[[object, str], bool] | None = None,
    enabled: Callable[[], bool] | None = None,
) -> bool:
    """One notice for one attach action (single item or a whole batch).
    True when no notice is needed, it is suppressed, or the owner chose Attach."""
    if not needs_public_attach_notice(observation):
        return True
    if not (enabled or publish_notice_enabled)():
        return True
    return bool((show or show_attach_notice)(parent, build_attach_notice_text(roles)))


# --- Making spore data public on an already public observation -----------------

def needs_spore_public_notice(observation: dict | None, new_visibility: str | None) -> bool:
    """True when spore data turns public on a public, non-draft observation
    (``observation`` holds the state before the change)."""
    new_vis = str(new_visibility or "").strip().lower()
    return new_vis == "public" and is_public(observation) and not spore_data_public(observation)


def build_spore_public_notice_text(attached_roles=None) -> str:
    lines = [
        QCoreApplication.translate("PublishNotice", "Anyone, including people who are not signed in, will be able to see:"),
        "• " + QCoreApplication.translate("PublishNotice", "Spore measurements, statistics, measurement points and the spore mosaic"),
        "",
    ]
    lines += references_notes("public", attached_roles)
    lines.append(QCoreApplication.translate("PublishNotice", "The change takes effect after the next sync."))
    return "\n".join(lines)


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
    load_roles: Callable[[], object] | None = None,
    *,
    show: Callable[[object, str], bool] | None = None,
    enabled: Callable[[], bool] | None = None,
) -> bool:
    """True when no notice is needed, it is suppressed, or the owner confirmed."""
    if not needs_spore_public_notice(observation, new_visibility):
        return True
    if not (enabled or publish_notice_enabled)():
        return True
    roles = None
    if load_roles is not None:
        try:
            roles = load_roles()
        except Exception:
            roles = None
    return bool((show or show_spore_public_notice)(parent, build_spore_public_notice_text(roles)))
