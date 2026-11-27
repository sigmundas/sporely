"""F24: the taxa import script is a dry run unless --apply is given."""
import sqlite3

import pytest

from database import import_taxa_to_supabase as imp


class RecordingSession:
    def __init__(self):
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))

        class _Resp:
            status_code = 201
            text = ""

        return _Resp()


@pytest.fixture
def tiny_db(tmp_path, monkeypatch):
    path = tmp_path / "vernacular.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE taxon_min (taxon_id INTEGER, genus TEXT, specific_epithet TEXT,
            canonical_scientific_name TEXT, family TEXT, taxon_rank TEXT,
            norwegian_taxon_id INTEGER, swedish_taxon_id INTEGER, inaturalist_taxon_id INTEGER);
        CREATE TABLE taxon_external_id_min (taxon_id INTEGER, source_system TEXT,
            external_id TEXT, is_preferred INTEGER);
        CREATE TABLE vernacular_min (taxon_id INTEGER, language_code TEXT,
            vernacular_name TEXT, is_preferred_name INTEGER);
        INSERT INTO taxon_min VALUES (1, 'Examplea', 'testii', 'Examplea testii', 'Exampleaceae', 'species', 1, 2, 3);
        INSERT INTO taxon_min VALUES (2, 'Examplea', 'other', 'Examplea other', 'Exampleaceae', 'species', 4, 5, 6);
        INSERT INTO vernacular_min VALUES (1, 'nb', 'Eksempelsopp', 1);
        """
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(imp, "DB_PATH", path)
    monkeypatch.setattr(imp, "SERVICE_KEY", "dummy-service-key-not-real")
    return path


def test_default_run_sends_nothing_and_reports_counts(tiny_db, capsys):
    session = RecordingSession()
    imp.run(session=session)
    out = capsys.readouterr().out
    assert session.calls == []
    assert "dry run" in out.lower()
    assert "2" in out and "1" in out


def test_main_without_apply_sends_nothing(tiny_db, monkeypatch):
    session = RecordingSession()
    monkeypatch.setattr(imp.requests, "post", session.post)
    assert imp.main([]) == 0
    assert session.calls == []


def test_apply_echoes_target_and_posts(tiny_db, capsys):
    session = RecordingSession()
    imp.run(apply=True, session=session)
    out = capsys.readouterr().out
    assert imp.SUPABASE_URL in out
    assert len(session.calls) == 2
    assert session.calls[0][0].endswith("/rest/v1/taxa")


def test_apply_requires_the_service_key(tiny_db, monkeypatch):
    monkeypatch.setattr(imp, "SERVICE_KEY", "")
    session = RecordingSession()
    with pytest.raises(SystemExit):
        imp.run(apply=True, session=session)
    assert session.calls == []


def test_abbreviated_flag_does_not_enable_writes(tiny_db, monkeypatch):
    seen = []
    monkeypatch.setattr(imp, "run", lambda apply=False, session=None: seen.append(apply))
    with pytest.raises(SystemExit):
        imp.main(["--a"])
    assert seen == []
