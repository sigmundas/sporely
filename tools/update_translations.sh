#!/usr/bin/env bash
set -euo pipefail

files=(
  "main.py"
  "database/database_tags.py"
  "ui/database_settings_dialog.py"
  "ui/main_window.py"
  "ui/image_import_dialog.py"
  "ui/observations_tab.py"
  "ui/measurement_tool.py"
  "ui/calibration_dialog.py"
  "ui/species_plate_dialog.py"
  "ui/zoomable_image_widget.py"
  "ui/stats_table_widget.py"
  "ui/spore_preview_widget.py"
)

if [[ -x ".venv/bin/pyside6-lupdate" ]]; then
  lupdate_cmd=".venv/bin/pyside6-lupdate"
else
  lupdate_cmd="pyside6-lupdate"
fi

if [[ -x ".venv/bin/pyside6-lrelease" ]]; then
  lrelease_cmd=".venv/bin/pyside6-lrelease"
else
  lrelease_cmd="pyside6-lrelease"
fi

"$lupdate_cmd" "${files[@]}" -no-obsolete -ts i18n/Sporely_nb_NO.ts i18n/Sporely_sv_SE.ts i18n/Sporely_de_DE.ts
"$lrelease_cmd" i18n/Sporely_nb_NO.ts i18n/Sporely_sv_SE.ts i18n/Sporely_de_DE.ts
