import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Taxonomy-v2 is ON by default in the product. Unit tests that resolve the
# vernacular DB must not install the ~320 MB artifact into the developer's
# real app-data profile, so the suite runs with the explicit off-override
# unless a caller opts in (SPORELY_TAXONOMY_V2=1). Tests of the default clear
# it with monkeypatch.delenv.
os.environ.setdefault("SPORELY_TAXONOMY_V2", "0")
