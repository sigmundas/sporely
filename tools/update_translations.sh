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
  "ui/zoomable_image_widget.py"
  "ui/stats_table_widget.py"
  "ui/spore_preview_widget.py"
)

pyside6-lupdate "${files[@]}" -ts i18n/MycoLog_nb_NO.ts i18n/MycoLog_de_DE.ts
pyside6-lrelease i18n/MycoLog_nb_NO.ts i18n/MycoLog_de_DE.ts
