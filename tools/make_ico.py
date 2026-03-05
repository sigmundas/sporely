#!/usr/bin/env python3
"""Generate platform icon files from the source PNG.

Outputs:
  assets/icons/mycolog.ico      - Windows (16/32/48/128/256 px)
  assets/icons/mycolog_256.png  - Linux .deb icon (256 px)
"""
from pathlib import Path
from PIL import Image

src = Path(__file__).parent.parent / "assets" / "icons" / "mycolog icon.png"
icons_dir = Path(__file__).parent.parent / "assets" / "icons"

img = Image.open(src).convert("RGBA")

# Square crop from center
w, h = img.size
side = min(w, h)
left = (w - side) // 2
top = (h - side) // 2
img = img.crop((left, top, left + side, top + side))

# Windows .ico
sizes = [16, 32, 48, 128, 256]
icons = [img.resize((s, s), Image.LANCZOS) for s in sizes]
dst_ico = icons_dir / "mycolog.ico"
icons[0].save(dst_ico, format="ICO", sizes=[(s, s) for s in sizes], append_images=icons[1:])
print(f"Written: {dst_ico}")

# Linux PNG (256 px)
dst_png = icons_dir / "mycolog_256.png"
icons[-1].save(dst_png)
print(f"Written: {dst_png}")
