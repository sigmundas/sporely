$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$venvScripts = Join-Path $root ".venv\\Scripts"
$lupdate = Join-Path $venvScripts "pyside6-lupdate.exe"
$lrelease = Join-Path $venvScripts "pyside6-lrelease.exe"

if (Test-Path $lupdate) {
    $lupdateCmd = $lupdate
} else {
    $lupdateCmd = "pyside6-lupdate"
}

if (Test-Path $lrelease) {
    $lreleaseCmd = $lrelease
} else {
    $lreleaseCmd = "pyside6-lrelease"
}

$files = @(
    "main.py",
    "database\\database_tags.py",
    "ui\\database_settings_dialog.py",
    "ui\\main_window.py",
    "ui\\image_import_dialog.py",
    "ui\\observations_tab.py",
    "ui\\measurement_tool.py",
    "ui\\calibration_dialog.py",
    "ui\\species_plate_dialog.py",
    "ui\\zoomable_image_widget.py",
    "ui\\stats_table_widget.py",
    "ui\\spore_preview_widget.py"
)

& $lupdateCmd $files -no-obsolete -ts i18n\\Sporely_nb_NO.ts i18n\\Sporely_sv_SE.ts i18n\\Sporely_de_DE.ts
& $lreleaseCmd i18n\\Sporely_nb_NO.ts i18n\\Sporely_sv_SE.ts i18n\\Sporely_de_DE.ts
