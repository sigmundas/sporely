from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QLineEdit

from database.taxon_lookup import TaxonChoice
from ui.taxon_input_controller import ROLE_TAXON_CHOICE, TaxonInputController


def _make_app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakeLookup:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.vernacular_db = object()
        self.language_code = "en"

    def suggest_genera(self, prefix: str = "", limit: int = 200) -> list[str]:
        self.calls.append(("suggest_genera", prefix, limit))
        return ["Agaricus"]

    def suggest_species(self, genus: str, prefix: str = "", limit: int = 200) -> list[TaxonChoice]:
        self.calls.append(("suggest_species", genus, prefix, limit))
        return [
            TaxonChoice(genus=genus, species="alpha", common_name="Alpha mushroom"),
            TaxonChoice(genus=genus, species="beta", common_name="Beta mushroom"),
            TaxonChoice(genus=genus, species="zeta", common_name="Zeta mushroom"),
        ]

    def suggest_common_names(
        self,
        prefix: str = "",
        genus: str | None = None,
        species: str | None = None,
        limit: int = 200,
    ) -> list[TaxonChoice]:
        self.calls.append(("suggest_common_names", prefix, genus, species, limit))
        if species == "muscaria":
            return []
        return [
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom"),
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Cultivated mushroom"),
        ]

    def resolve_common_name(
        self,
        name: str,
        genus: str | None = None,
        species: str | None = None,
    ) -> list[TaxonChoice]:
        self.calls.append(("resolve_common_name", name, genus, species))
        if name.casefold() == "button mushroom":
            return [TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom")]
        if name.casefold() == "cultivated mushroom":
            return [TaxonChoice(genus="Agaricus", species="bisporus", common_name="Cultivated mushroom")]
        return []

    def resolve_scientific(self, genus: str, species: str) -> TaxonChoice | None:
        self.calls.append(("resolve_scientific", genus, species))
        if (genus or "").casefold() == "agaricus" and (species or "").casefold() == "bisporus":
            return TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom")
        if (genus or "").casefold() == "agaricus" and (species or "").casefold() == "muscaria":
            return TaxonChoice(genus="Agaricus", species="muscaria", common_name=None)
        return None

    def best_common_name_for_taxon(self, genus: str, species: str) -> TaxonChoice | None:
        self.calls.append(("best_common_name_for_taxon", genus, species))
        if (genus or "").casefold() == "agaricus" and (species or "").casefold() == "bisporus":
            return TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom")
        return None


class _VernacularFallbackLookup(_FakeLookup):
    def suggest_common_names(
        self,
        prefix: str = "",
        genus: str | None = None,
        species: str | None = None,
        limit: int = 200,
    ) -> list[TaxonChoice]:
        self.calls.append(("suggest_common_names", prefix, genus, species, limit))
        if genus or species:
            if (genus or "").casefold() == "fraxinus" and (species or "").casefold() == "excelsior":
                return [TaxonChoice(genus="Fraxinus", species="excelsior", common_name="ask")]
            return []
        if prefix.casefold().startswith("ask"):
            return [TaxonChoice(genus="Fraxinus", species="excelsior", common_name="ask")]
        return []

    def resolve_common_name(
        self,
        name: str,
        genus: str | None = None,
        species: str | None = None,
    ) -> list[TaxonChoice]:
        self.calls.append(("resolve_common_name", name, genus, species))
        if name.casefold() == "ask":
            if (genus or "").casefold() == "fraxinus" and (species or "").casefold() == "excelsior":
                return [TaxonChoice(genus="Fraxinus", species="excelsior", common_name="ask")]
            if genus or species:
                return []
            return [TaxonChoice(genus="Fraxinus", species="excelsior", common_name="ask")]
        return []

    def best_common_name_for_taxon(self, genus: str, species: str) -> TaxonChoice | None:
        self.calls.append(("best_common_name_for_taxon", genus, species))
        if (genus or "").casefold() == "fraxinus" and (species or "").casefold() == "excelsior":
            return TaxonChoice(genus="Fraxinus", species="excelsior", common_name="ask")
        return super().best_common_name_for_taxon(genus, species)

    def resolve_scientific(self, genus: str, species: str) -> TaxonChoice | None:
        self.calls.append(("resolve_scientific", genus, species))
        if (genus or "").casefold() == "fraxinus" and (species or "").casefold() == "excelsior":
            return TaxonChoice(genus="Fraxinus", species="excelsior", common_name="ask")
        return super().resolve_scientific(genus, species)


def test_species_suggestions_are_alphabetical_and_use_the_shared_limit() -> None:
    _make_app()
    lookup = _FakeLookup()
    genus_input = QLineEdit()
    species_input = QLineEdit()
    vernacular_input = QLineEdit()

    controller = TaxonInputController(lookup, genus_input, species_input, vernacular_input)

    genus_input.setText("Agaricus")
    species_input.setText("b")
    controller.refresh_species_suggestions()

    assert controller.species_model.stringList() == ["alpha", "beta", "zeta"]
    assert ("suggest_species", "Agaricus", "b", 200) in lookup.calls


def test_common_name_selection_inserts_only_the_common_name() -> None:
    _make_app()
    lookup = _FakeLookup()
    genus_input = QLineEdit()
    species_input = QLineEdit()
    vernacular_input = QLineEdit()

    controller = TaxonInputController(lookup, genus_input, species_input, vernacular_input)
    lookup.best_common_name_for_taxon = lambda *_args, **_kwargs: None

    genus_input.setText("Agaricus")
    species_input.setText("bisporus")
    controller.refresh_vernacular_suggestions()

    index = controller.vernacular_model.index(0, 0)
    assert controller.vernacular_model.stringList()[0] == "Button mushroom"

    controller.on_vernacular_selected(index)

    assert vernacular_input.text() == "Button mushroom"
    assert genus_input.text() == "Agaricus"
    assert species_input.text() == "bisporus"


def test_common_name_selection_overwrites_stale_scientific_fields() -> None:
    _make_app()
    lookup = _VernacularFallbackLookup()
    genus_input = QLineEdit()
    species_input = QLineEdit()
    vernacular_input = QLineEdit()

    controller = TaxonInputController(lookup, genus_input, species_input, vernacular_input)

    genus_input.setText("Cantharellus")
    species_input.setText("cibarius")

    choice = TaxonChoice(genus="Fraxinus", species="excelsior", common_name="ask")
    controller.vernacular_model.setStringList(["ask"])
    controller.vernacular_model.item(0).setData(choice, ROLE_TAXON_CHOICE)
    index = controller.vernacular_model.index(0, 0)

    controller.on_vernacular_selected(index)

    assert vernacular_input.text() == "ask"
    assert genus_input.text() == "Fraxinus"
    assert species_input.text() == "excelsior"


def test_stale_vernacular_clears_when_species_no_longer_matches() -> None:
    _make_app()
    lookup = _FakeLookup()
    genus_input = QLineEdit()
    species_input = QLineEdit()
    vernacular_input = QLineEdit()

    controller = TaxonInputController(lookup, genus_input, species_input, vernacular_input)

    genus_input.setText("Agaricus")
    species_input.setText("bisporus")
    vernacular_input.setText("Button mushroom")
    controller.sync_vernacular_after_taxon_change()
    assert vernacular_input.text() == "Button mushroom"

    species_input.setText("muscaria")
    controller.sync_vernacular_after_taxon_change()

    assert vernacular_input.text() == ""
    assert controller.vernacular_model.rowCount() == 0


def test_vernacular_typing_clears_stale_taxon_and_broadens_suggestions() -> None:
    _make_app()
    lookup = _VernacularFallbackLookup()
    genus_input = QLineEdit()
    species_input = QLineEdit()
    vernacular_input = QLineEdit()

    controller = TaxonInputController(lookup, genus_input, species_input, vernacular_input)

    genus_input.setText("Cantharellus")
    species_input.setText("cibarius")
    vernacular_input.setText("ask")

    controller.on_vernacular_text_changed("ask")
    controller.refresh_vernacular_suggestions()

    assert genus_input.text() == ""
    assert species_input.text() == ""
    assert controller.vernacular_model.stringList() == ["ask"]
    assert (
        "suggest_common_names",
        "ask",
        "Cantharellus",
        "cibarius",
        200,
    ) in lookup.calls
    assert (
        "suggest_common_names",
        "ask",
        None,
        None,
        200,
    ) in lookup.calls


def test_vernacular_editing_finished_falls_back_to_unfiltered_resolution() -> None:
    _make_app()
    lookup = _VernacularFallbackLookup()
    genus_input = QLineEdit()
    species_input = QLineEdit()
    vernacular_input = QLineEdit()

    controller = TaxonInputController(lookup, genus_input, species_input, vernacular_input)

    genus_input.setText("Cantharellus")
    species_input.setText("cibarius")
    vernacular_input.setText("ask")

    controller.on_vernacular_editing_finished()

    assert vernacular_input.text() == "ask"
    assert genus_input.text() == "Fraxinus"
    assert species_input.text() == "excelsior"
