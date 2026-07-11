from utils.taxon_text import split_scientific_name_text


def test_plain_binomial() -> None:
    assert split_scientific_name_text("Cantharellus cibarius") == ("Cantharellus", "cibarius")


def test_infraspecific_marker_between_genus_and_species_is_skipped() -> None:
    assert split_scientific_name_text("Amanita cf. muscaria") == ("Amanita", "muscaria")


def test_group_qualifier_coll_is_retained() -> None:
    assert split_scientific_name_text("Hygrocybe conica coll.") == ("Hygrocybe", "conica coll.")


def test_group_qualifier_coll_with_variant_number() -> None:
    assert split_scientific_name_text("Cortinarius flavoides coll. 1") == (
        "Cortinarius",
        "flavoides coll. 1",
    )


def test_group_qualifier_agg_is_retained() -> None:
    assert split_scientific_name_text("Boletus edulis agg.") == ("Boletus", "edulis agg.")


def test_sensu_lato_is_retained() -> None:
    assert split_scientific_name_text("Russula sardonia sensu lato") == (
        "Russula",
        "sardonia sensu lato",
    )


def test_sensu_stricto_is_retained() -> None:
    assert split_scientific_name_text("Russula sardonia sensu stricto") == (
        "Russula",
        "sardonia sensu stricto",
    )


def test_short_qualifier_sl_dotted() -> None:
    assert split_scientific_name_text("Russula sardonia s.l.") == ("Russula", "sardonia s.l.")


def test_short_qualifier_sl_with_space() -> None:
    assert split_scientific_name_text("Russula sardonia s. l.") == (
        "Russula",
        "sardonia s. l.",
    )


def test_infraspecific_marker_and_group_qualifier_combined() -> None:
    assert split_scientific_name_text("Russula cf. lepida coll.") == (
        "Russula",
        "lepida coll.",
    )


def test_returns_none_when_only_genus() -> None:
    assert split_scientific_name_text("Boletus") == (None, None)


def test_returns_none_for_empty() -> None:
    assert split_scientific_name_text("") == (None, None)
    assert split_scientific_name_text(None) == (None, None)
