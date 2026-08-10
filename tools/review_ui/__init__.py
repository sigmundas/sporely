"""Reusable deterministic renderer for production Qt UI review scenarios."""

from .registry import ReviewScenario, ScenarioRegistry
from .runner import render_scenarios

__all__ = ["ReviewScenario", "ScenarioRegistry", "render_scenarios"]
