#!/usr/bin/env bash
set -euo pipefail

build_version="${SPORELY_BUILD_VERSION:-$(python tools/release_version.py app-version --app-file main.py)}"

python -m pip install -r requirements.txt
runtime_assets="$(python tools/runtime_assets.py pyinstaller-add-data --separator ':')"

pyinstaller \
  --noconfirm \
  --clean \
  --onedir \
  --windowed \
  --name Sporely \
  --icon "assets/icons/sporely.icns" \
  --add-data "$runtime_assets" \
  --add-data "i18n:i18n" \
  --add-data "database/reference_data:database/reference_data" \
  --hidden-import pillow_heif \
  --exclude-module PySide6.QtQml \
  --exclude-module PySide6.QtQuick \
  --exclude-module PySide6.QtQuickControls2 \
  --exclude-module PySide6.QtQuickWidgets \
  --exclude-module PySide6.QtPdf \
  --exclude-module PySide6.QtPdfWidgets \
  --exclude-module PySide6.QtWebEngineWidgets \
  --exclude-module PySide6.QtWebEngineCore \
  --exclude-module PySide6.QtWebEngineQuick \
  --exclude-module PySide6.QtWebChannel \
  --exclude-module tkinter \
  --exclude-module PyQt5 \
  --exclude-module PyQt6 \
  --exclude-module wx \
  --exclude-module gi \
  --exclude-module kivy \
  main.py

python tools/runtime_assets.py verify-artifact \
  --artifact-root dist/Sporely.app/Contents/Frameworks
python tools/release_version.py set-macos-bundle \
  --plist dist/Sporely.app/Contents/Info.plist \
  --version "$build_version"
codesign --force --deep --sign - dist/Sporely.app
