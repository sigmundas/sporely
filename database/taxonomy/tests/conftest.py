import os

# Taxonomy-v2 is ON by default in the product; keep these tests from
# installing the bundled artifact into the developer's real app-data profile.
# Tests of the default clear this with monkeypatch.delenv.
os.environ.setdefault("SPORELY_TAXONOMY_V2", "0")
