#!/usr/bin/env python3
"""Generate platform icon files from the source PNG.

Outputs:
  assets/icons/sporely.ico      - Windows (16/32/48/128/256 px)
  assets/icons/sporely_256.png  - Linux .deb icon (256 px)
  assets/icons/sporely.icns     - macOS app icon
"""
from pathlib import Path
from PIL import Image

src = Path(__file__).parent.parent / "assets" / "icons" / "sporely icon.png"
icons_dir = Path(__file__).parent.parent / "assets" / "icons"
CONTENT_SCALE = 0.94  # Fraction of square side used by artwork (leave a small safety margin)

img = Image.open(src).convert("RGBA")

# Trim transparent borders first so artwork fills more icon space.
alpha = img.getchannel("A")
bbox = alpha.getbbox()
if bbox:
    img = img.crop(bbox)

# Place artwork in a square canvas and scale to near full size.
w, h = img.size
side = max(w, h)
canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))

target = max(1, int(round(side * CONTENT_SCALE)))
scale = min(target / max(1, w), target / max(1, h))
new_w = max(1, int(round(w * scale)))
new_h = max(1, int(round(h * scale)))
img = img.resize((new_w, new_h), Image.LANCZOS)
canvas.paste(img, ((side - new_w) // 2, (side - new_h) // 2), img)
img = canvas

# Windows .ico
sizes = [16, 32, 48, 128, 256]
icons = [img.resize((s, s), Image.LANCZOS) for s in sizes]
dst_ico = icons_dir / "sporely.ico"
icons[0].save(dst_ico, format="ICO", sizes=[(s, s) for s in sizes], append_images=icons[1:])
print(f"Written: {dst_ico}")

# Linux PNG (256 px)
dst_png = icons_dir / "sporely_256.png"
icons[-1].save(dst_png)
print(f"Written: {dst_png}")

# macOS .icns
# Use Pillow directly so local iconutil quirks do not block icon generation.
dst_icns = icons_dir / "sporely.icns"
icns_base = img.resize((1024, 1024), Image.LANCZOS)
icns_base.save(
    dst_icns,
    format="ICNS",
    sizes=[(16, 16), (32, 32), (64, 64), (128, 128), (256, 256), (512, 512), (1024, 1024)],
)
print(f"Written: {dst_icns}")
