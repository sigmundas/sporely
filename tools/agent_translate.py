#!/usr/bin/env python3
import json
import os
import sys
import xml.etree.ElementTree as ET

LANGUAGES = ['nb_NO', 'sv_SE', 'de_DE']
I18N_DIR = os.path.join(os.path.dirname(__file__), '..', 'i18n')
JSON_FILE = os.path.join(os.path.dirname(__file__), '..', 'missing_translations.json')

def extract_missing():
    missing = {}
    for lang in LANGUAGES:
        filepath = os.path.join(I18N_DIR, f"Sporely_{lang}.ts")
        if not os.path.exists(filepath):
            continue
        tree = ET.parse(filepath)
        root = tree.getroot()
        missing[lang] = {}

        for context in root.findall('context'):
            for message in context.findall('message'):
                source_elem = message.find('source')
                if source_elem is None or not source_elem.text:
                    continue
                source = source_elem.text

                translation_elem = message.find('translation')
                if translation_elem is not None:
                    is_unfinished = translation_elem.get('type') == 'unfinished'
                    is_empty = (not translation_elem.text or translation_elem.text.strip() == "")
                    if is_unfinished or is_empty:
                        missing[lang][source] = ""

    # Remove languages with no missing translations
    missing = {lang: data for lang, data in missing.items() if data}

    if not missing:
        print("No missing translations found.")
        if os.path.exists(JSON_FILE):
            os.remove(JSON_FILE)
        return

    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(missing, f, indent=2, ensure_ascii=False)
    print(f"Extracted missing translations to: {JSON_FILE}")
    print("AI Agent Workflow:")
    print("1. Modify 'missing_translations.json' directly to fill in the blank values.")
    print("2. Run 'python3 tools/agent_translate.py apply'")
    print("3. Run './tools/update_translations.sh' to compile to .qm")

def apply_translations():
    if not os.path.exists(JSON_FILE):
        print(f"File {JSON_FILE} not found. Run extract first.")
        return

    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        translations = json.load(f)

    for lang in LANGUAGES:
        if lang not in translations:
            continue
            
        filepath = os.path.join(I18N_DIR, f"Sporely_{lang}.ts")
        if not os.path.exists(filepath):
            continue
            
        try:
            tree = ET.parse(filepath)
            root = tree.getroot()
            changed = False
            lang_dict = translations[lang]

            for context in root.findall('context'):
                for message in context.findall('message'):
                    source_elem = message.find('source')
                    if source_elem is None or not source_elem.text:
                        continue
                    source = source_elem.text

                    translation_elem = message.find('translation')
                    if translation_elem is not None:
                        is_unfinished = translation_elem.get('type') == 'unfinished'
                        is_empty = (not translation_elem.text or translation_elem.text.strip() == "")

                        if is_unfinished or is_empty:
                            if source in lang_dict and lang_dict[source].strip() != "":
                                translation_elem.text = lang_dict[source]
                                if 'type' in translation_elem.attrib:
                                    del translation_elem.attrib['type']
                                changed = True

            if changed:
                # Need to retain DOCTYPE
                xml_str = ET.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')
                if '<!DOCTYPE' not in xml_str:
                    if "<?xml version='1.0' encoding='utf-8'?>\n" in xml_str:
                        xml_str = xml_str.replace("<?xml version='1.0' encoding='utf-8'?>\n", '<?xml version="1.0" encoding="utf-8"?>\n<!DOCTYPE TS>\n', 1)
                    elif '<?xml version="1.0" encoding="utf-8"?>\n' in xml_str:
                        xml_str = xml_str.replace('<?xml version="1.0" encoding="utf-8"?>\n', '<?xml version="1.0" encoding="utf-8"?>\n<!DOCTYPE TS>\n', 1)

                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(xml_str)
                print(f"Applied translations to {filepath}")
            else:
                print(f"No changes needed for {filepath}")

        except Exception as e:
            print(f"Error applying translations for {lang}: {e}")
            
    # Cleanup after successful apply
    print(f"Removing {JSON_FILE}...")
    os.remove(JSON_FILE)

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ['extract', 'apply']:
        print("Usage: python3 tools/agent_translate.py [extract|apply]")
        sys.exit(1)
        
    if sys.argv[1] == 'extract':
        extract_missing()
    elif sys.argv[1] == 'apply':
        apply_translations()
