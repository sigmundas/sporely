"""Built-in UI review scenario registry."""
from __future__ import annotations

from ..registry import ScenarioRegistry
from .conflicts import register_conflict_scenarios
from .live_lab import register_live_lab_scenarios
from .references import register_reference_scenarios
from .measure import register_measure_scenarios
from .measure_tab import register_measure_tab_scenarios
from .analysis_tab import register_analysis_tab_scenarios
from .observations import register_observation_scenarios
from .raw_processing import register_raw_processing_scenarios
from .portable_import import register_portable_import_scenarios
from .quick_fixes import register_quick_fix_scenarios
from .style_tokens import register_style_token_scenarios
from .taxon_picker import register_taxon_picker_scenarios


def create_registry() -> ScenarioRegistry:
    registry = ScenarioRegistry()
    register_conflict_scenarios(registry)
    register_reference_scenarios(registry)
    register_measure_scenarios(registry)
    register_measure_tab_scenarios(registry)
    register_analysis_tab_scenarios(registry)
    register_observation_scenarios(registry)
    register_raw_processing_scenarios(registry)
    register_portable_import_scenarios(registry)
    register_quick_fix_scenarios(registry)
    register_live_lab_scenarios(registry)
    register_style_token_scenarios(registry)
    register_taxon_picker_scenarios(registry)
    return registry


__all__ = ["create_registry"]
