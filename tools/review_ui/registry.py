"""Scenario metadata, registration, and selection."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterable

if TYPE_CHECKING:
    from PySide6.QtWidgets import QWidget

    from .context import ReviewContext


_SEMANTIC_ID = re.compile(r"^[a-z0-9]+(?:[.-][a-z0-9]+)*$")


@dataclass(frozen=True)
class ReviewScenario:
    """One meaningful production-widget state and its review metadata."""

    id: str
    group: str
    title: str
    description: str
    build: Callable[["ReviewContext"], "QWidget"]
    viewport: tuple[int, int]
    theme: str = "light"
    locale: str = "en"
    default: bool = True

    def __post_init__(self) -> None:
        if not _SEMANTIC_ID.fullmatch(self.id):
            raise ValueError(f"invalid semantic scenario ID: {self.id!r}")
        if not _SEMANTIC_ID.fullmatch(self.group):
            raise ValueError(f"invalid scenario group: {self.group!r}")
        if not self.title.strip() or not self.description.strip():
            raise ValueError(f"{self.id}: title and description are required")
        if min(self.viewport) <= 0:
            raise ValueError(f"{self.id}: viewport must contain positive dimensions")
        if self.theme not in {"light", "dark"}:
            raise ValueError(f"{self.id}: unsupported theme {self.theme!r}")

    @property
    def filename(self) -> str:
        """Derive a confined filename without making it the scenario identity."""
        return f"{self.id}.png"


class ScenarioRegistry:
    def __init__(self) -> None:
        self._scenarios: dict[str, ReviewScenario] = {}

    def register(self, scenario: ReviewScenario) -> ReviewScenario:
        if scenario.id in self._scenarios:
            raise ValueError(f"duplicate review scenario ID: {scenario.id}")
        self._scenarios[scenario.id] = scenario
        return scenario

    def all(self) -> tuple[ReviewScenario, ...]:
        return tuple(self._scenarios.values())

    def groups(self) -> tuple[str, ...]:
        return tuple(sorted({scenario.group for scenario in self._scenarios.values()}))

    def select(
        self,
        *,
        groups: Iterable[str] = (),
        scenario_ids: Iterable[str] = (),
    ) -> tuple[ReviewScenario, ...]:
        requested_groups = tuple(dict.fromkeys(groups))
        requested_ids = tuple(dict.fromkeys(scenario_ids))

        unknown_groups = sorted(set(requested_groups) - set(self.groups()))
        if unknown_groups:
            raise ValueError(f"unknown scenario group(s): {', '.join(unknown_groups)}")
        unknown_ids = sorted(set(requested_ids) - set(self._scenarios))
        if unknown_ids:
            raise ValueError(f"unknown scenario ID(s): {', '.join(unknown_ids)}")

        if not requested_groups and not requested_ids:
            return tuple(s for s in self.all() if s.default)

        selected_ids = set(requested_ids)
        selected_ids.update(
            scenario.id
            for scenario in self.all()
            if scenario.group in requested_groups
        )
        return tuple(s for s in self.all() if s.id in selected_ids)
