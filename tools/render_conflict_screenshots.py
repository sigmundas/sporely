"""Compatibility CLI for the conflict group in the generic UI review renderer."""
from __future__ import annotations

import sys
from pathlib import Path

from tools.render_review_screenshots import main


if __name__ == "__main__":
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/sporely-screens")
    raise SystemExit(main(["--group", "conflict", str(output_dir)]))
