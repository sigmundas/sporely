![Sporely](docs/images/sporely-logo-dark.png)

Sporely is a desktop app for field observations, microscopy calibration, and spore measurements.

## What Sporely Is For

Sporely helps you keep mushroom finds, field photos, macro photos, microscope images, measurements, and notes together in one place. The goal is to make it easy to move between platforms and cameras without losing the story of a find, from the first mobile photo in the field to later microscopy and spore analysis.

Sporely Cloud is being built around a simple idea: mushroom spore data and analysis should be open and easily available for everyone. Its main purpose is to make measurements, images, and analysis easier to preserve, compare, and share. A secondary goal is social: connecting with friends, sharing finds, locations, and images when you choose to.

Sporely uses a Free and Pro model for the cloud service. Pro adds more private sync capacity and higher-quality cloud images. Payment details and plan information are on [sporely.no](https://sporely.no).

Regional publishing and place support follows the services that make the most sense locally:

- Norway: Sporely uses Norwegian place-name and reporting support, including Artsobservasjoner-oriented workflows.
- Sweden: Sporely uses Swedish reporting support through Artportalen-oriented workflows.
- Denmark: support is pending, with Danmarks Svampeatlas as the intended regional destination.
- Everywhere else: iNaturalist and Sporely Cloud 


## Installation

### Prebuilt application

Download the latest release from:
https://github.com/sigmundas/sporely/releases/latest

| Platform | File | How to install |
|----------|------|----------------|
| **Windows** | `Sporely-x.x.x-windows-setup.exe` | Run the installer — creates Start Menu entry and uninstaller |
| **macOS** | `Sporely-vx.x.x-macos.dmg` | Open the disk image, drag **Sporely** to **Applications** |
|  **Linux (Ubuntu/Debian)** | `sporely_x.x.x_amd64.deb` | `sudo dpkg -i sporely_x.x.x_amd64.deb` — adds app menu entry |

### Notes for Windows and macOS

Windows may show a Microsoft Defender SmartScreen warning like "This app isn't commonly downloaded". Choose `Keep` and run the installer. 

If macOS shows a warning like "Apple could not verify ..." or blocks the app because it was downloaded from the Internet, either:

1. In Finder, open `Applications`, right-click `Sporely.app`, and choose `Open`.
2. Or remove the quarantine attribute in Terminal:

```bash
xattr -dr com.apple.quarantine /Applications/Sporely.app
```

### Run from source (Python)

This repository does not ship a `.venv` folder or activation scripts.
They are created locally when you run `python -m venv .venv`.

Use `python -m pip` (not plain `pip`) so installs always target the same interpreter you run.

Linux/macOS (first-time setup):

```bash
cd ~/myapps/sporely
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python main.py
```

Windows PowerShell (first-time setup):

```powershell
cd path\to\sporely
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python main.py
```

Note for macOS development: when you launch the app from VS Code or with `python main.py`, the menu bar may show `Python` as the app name. The packaged app/installer shows `Sporely`.

Each new terminal session:

- Linux/macOS: `source .venv/bin/activate`
- Windows PowerShell: `.\.venv\Scripts\Activate.ps1`
- Windows Command Prompt: `.\.venv\Scripts\activate.bat`

## Cloud Media

Normal desktop media sync uses the authenticated Cloudflare upload Worker, not direct R2.

- Set the public Worker URL with `SPORELY_MEDIA_WORKER_URL` if you need to override the default. The default is `https://upload.sporely.no`.
- `SPORELY_MEDIA_WORKER_URL` is public runtime config, not a secret.
- Normal users do not need `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_S3_ENDPOINT`, or `SPORELY_ENABLE_DIRECT_R2`.
- Direct R2 remains admin/developer-only and is only enabled when `SPORELY_ENABLE_DIRECT_R2=1` is set alongside local admin secrets.
- The public image tier is still described as 20 MP, but the client only downsizes when a source image exceeds `21 MP` or `5300 px` on the longest edge, which keeps borderline 20 MP frames intact.

If you need to audit or repair broken media rows:

- `materialize_cloud_media_for_observation(client, local_observation_id)` will re-download missing local media through the public Worker URL or the authenticated Worker download endpoint.
- `download_image_file(storage_path, dest_path)` now surfaces missing remote objects as `Cloud image file is missing from storage (<key>)`.
- For a cloud observation, `pull_image_metadata(obs_cloud_id, include_deleted_for_sync=True)` shows the remote rows to inspect before reuploading or removing broken media entries.



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
- macOS: `~/Library/Application Support/Sporely/plate_layouts`

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
- [Online publishing](docs/artsobservasjoner.md)
- [Field photography](docs/field-photography.md)
- [Microscopy workflow](docs/microscopy-workflow.md)
- [Spore measurements](docs/spore-measurements.md)
- [Taxonomy integration](docs/taxonomy-integration.md)
- [Database structure](docs/database-structure.md)
- [Changelog](CHANGELOG.md)


## License

MIT License - feel free to modify and extend.
