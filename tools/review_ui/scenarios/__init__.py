"""Built-in UI review scenario registry."""
from __future__ import annotations

from ..registry import ScenarioRegistry
from .conflicts import register_conflict_scenarios
from .references import register_reference_scenarios
from .measure import register_measure_scenarios


def create_registry() -> ScenarioRegistry:
    registry = ScenarioRegistry()
    register_conflict_scenarios(registry)
    register_reference_scenarios(registry)
    register_measure_scenarios(registry)
    return registry


__all__ = ["create_registry"]
