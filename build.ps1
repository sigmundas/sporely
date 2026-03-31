$ErrorActionPreference = "Stop"

python -m pip install --upgrade pip --disable-pip-version-check
python -m pip install -r requirements.txt --disable-pip-version-check

pyinstaller `
    --noconfirm `
    --clean `
    --onedir `
    --windowed `
    --name Sporely `
    --icon "assets\icons\sporely.ico" `
    --add-data "i18n;i18n" `
    --add-data "database\reference_values.db;database" `
    --hidden-import pillow_heif `
    --exclude-module PySide6.QtQml `
    --exclude-module PySide6.QtQuick `
    --exclude-module PySide6.QtQuickControls2 `
    --exclude-module PySide6.QtQuickWidgets `
    --exclude-module PySide6.QtPdf `
    --exclude-module PySide6.QtPdfWidgets `
    --exclude-module PySide6.QtWebEngineWidgets `
    --exclude-module PySide6.QtWebEngineCore `
    --exclude-module PySide6.QtWebEngineQuick `
    --exclude-module PySide6.QtWebChannel `
    --exclude-module tkinter `
    --exclude-module PyQt5 `
    --exclude-module PyQt6 `
    --exclude-module wx `
    --exclude-module gi `
    --exclude-module kivy `
    main.py
