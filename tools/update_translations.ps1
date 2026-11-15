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
    "ui\\portable_import_dialog.py",
    "ui\\image_import_dialog.py",
    "ui\\raw_processing_controls.py",
    "ui\\observations_tab.py",
    "ui\\measurement_tool.py",
    "ui\\calibration_dialog.py",
    "ui\\cloud_conflict_dialog.py",
    "ui\\species_plate_dialog.py",
    "ui\\zoomable_image_widget.py",
    "ui\\stats_table_widget.py",
    "ui\\spore_preview_widget.py",
    "ui\\reference_library_attach_dialog.py",
    "ui\\add_reference_dialog.py",
    "ui\\library_source_row.py",
    "ui\\reference_entry_editor.py",
    "ui\\reference_library_manager_dialog.py",
    "ui\\curated_reference_catalogue_dialog.py",
    "ui\\comparison_panel.py",
    "ui\\cloud_reference_dialog.py",
    "ui\\reference_preview_pane.py",
    "ui\\reference_comparison_view.py",
    "ui\\measurement_content_view.py",
    "ui\\reference_sharing_dialogs.py",
    "ui\\publish_notice.py",
    "ui\\taxon_picker.py",
    "ui\\live_lab_tab.py",
    "ui\\ingestion_hub_tab.py",
    "ui\\sync_shot_dialog.py",
    "ui\\image_gallery_widget.py",
    "ui\\export_image_dialog.py",
    "ui\\dialog_helpers.py",
    "ui\\observation_list.py",
    "ui\\cloud_sync_dialog.py",
    "ui\\qt_standard_texts.py"
)

& $lupdateCmd $files -no-obsolete -ts i18n\\Sporely_nb_NO.ts i18n\\Sporely_sv_SE.ts i18n\\Sporely_de_DE.ts
& $lreleaseCmd i18n\\Sporely_nb_NO.ts i18n\\Sporely_sv_SE.ts i18n\\Sporely_de_DE.ts
