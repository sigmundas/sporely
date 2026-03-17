# ![MycoLog](docs/images/mycolog-logo.png)

MycoLog is a desktop app for field observations, microscopy calibration, and spore measurements.

## Installation

### Prebuilt application

Download the latest release from:
https://github.com/sigmundas/mycolog/releases/latest

| Platform | File | How to install |
|----------|------|----------------|
| **Windows** | `MycoLog-x.x.x-windows-setup.exe` | Run the installer — creates Start Menu entry and uninstaller |
| **macOS** | `MycoLog-vx.x.x-macos.dmg` | Open the disk image, drag **MycoLog** to **Applications** |
| **Linux (Ubuntu/Debian)** | `mycolog_x.x.x_amd64.deb` | `sudo dpkg -i mycolog_x.x.x_amd64.deb` — adds app menu entry |

### Notes for Windows and macOS

Windows may show a Microsoft Defender SmartScreen warning like "This app isn't commonly downloaded". Choose `Keep` and run the installer. 

If macOS shows a warning like "Apple could not verify ..." or blocks the app because it was downloaded from the Internet, either:

1. In Finder, open `Applications`, right-click `MycoLog.app`, and choose `Open`.
2. Or remove the quarantine attribute in Terminal:

```bash
xattr -dr com.apple.quarantine /Applications/MycoLog.app
```

### Run from source (Python)

This repository does not ship a `.venv` folder or activation scripts.
They are created locally when you run `python -m venv .venv`.

Use `python -m pip` (not plain `pip`) so installs always target the same interpreter you run.

Linux/macOS (first-time setup):

```bash
cd ~/myapps/mycolog
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python main.py
```

Windows PowerShell (first-time setup):

```powershell
cd path\to\mycolog
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python main.py
```

Note for macOS development: when you launch the app from VS Code or with `python main.py`, the menu bar may show `Python` as the app name. The packaged app/installer shows `MycoLog`.

Each new terminal session:

- Linux/macOS: `source .venv/bin/activate`
- Windows PowerShell: `.\.venv\Scripts\Activate.ps1`
- Windows Command Prompt: `.\.venv\Scripts\activate.bat`




## First Run

1. Open `Calibration > Microscope Objectives`.
2. Add or edit objectives (Magnification, NA, Objective name).
3. Calibrate an objective (auto or manual) and set it active.
4. Confirm your database folder in `Settings > Database`.

## Create Your First Observation

1. Click **New Observation**.
2. Add images (field or microscope). Multi-select is supported.
3. For microscope images, choose Objective/Scale, Contrast, Mount, and Sample type.
4. Ctrl+click multiple images in the gallery to apply settings to all selected at once.
5. Save the observation.

## Measure and Analyze

- Use the **Measure** tab to draw rectangles for spores or line measurements for length-only.
- Use **Analysis** to plot distributions and compare with reference datasets.
- Use **Species Plate** to build a composite plate from one observation. Saved plate layouts are stored in the app data folder, not in the project root.

### Species Plate Layouts

- Saved layouts are written as `.mplate` files in the app data folder.
- Current plate state is also remembered automatically per observation.
- macOS: `~/Library/Application Support/MycoLog/plate_layouts`

## Screenshots

Automatic or manual calibration of image scales: 
![Calibrate or pick objective](docs/images/calibration.png)

Create a new observation by importing images: 
![Create a new observation](docs/images/1-new-observation.png)

Search-as-you-type species, or use AI lookup to guess the species: 
![Import images](docs/images/2-new-observation.png)

Measure spores or other features: 
![Measure spores or lengths](docs/images/3-measure-spores.png)

Review plots and compare to references: 
![Review analysis and references](docs/images/4-stats-reference.png)

## Documentation
- [Field photography](docs/field-photography.md)
- [Microscopy workflow](docs/microscopy-workflow.md)
- [Spore measurements](docs/spore-measurements.md)
- [Taxonomy integration](docs/taxonomy-integration.md)
- [Database structure](docs/database-structure.md)
- [Changelog](CHANGELOG.md)


## License

MIT License - feel free to modify and extend.
