import xml.etree.ElementTree as ET

translations = {
    "nb_NO": {
        "Sync content": "Synkroniser innhold",
        "Include measures on synced images": "Inkluder målinger på synkroniserte bilder",
        "Shows a scale bar on synced images": "Viser en målestokk på synkroniserte bilder",
        "Adds a visible watermark on synced images.": "Legger til et synlig vannmerke på synkroniserte bilder.",
        "Enter your iNaturalist application credentials.\nFor open-source apps, leave the Client Secret blank to use the secure PKCE login flow.": "Skriv inn dine iNaturalist-applikasjonsopplysninger.\nFor apper med åpen kildekode, la klienthemmeligheten (Client Secret) stå tom for å bruke den sikre PKCE-innloggingsflyten.",
        "Client Secret (optional for public apps)": "Klienthemmelighet (valgfritt for offentlige apper)",
        "Please enter a Client ID.": "Vennligst skriv inn en klient-ID.",
        "Reporting system:": "Rapporteringssystem:",
        "Calibration (Ctrl+K)": "Kalibrering (Ctrl+K)",
        "Settings (Ctrl+,)": "Innstillinger (Ctrl+,)",
        "Preferences": "Preferanser",
        "Conflict resolution error: {err}": "Konfliktløsningsfeil: {err}",
        "Upload failed: missing iNaturalist Client ID.": "Opplasting mislyktes: mangler iNaturalist klient-ID.",
        "User profile": "Brukerprofil",
        "Database": "Database",
        "Online publishing": "Online publisering",
        "Sporely Cloud": "Sporely Cloud",
        "Language": "Språk",
        "Appearance": "Utseende",
        "Close": "Lukk",
        "Name is used for the copyright watermark on images.\nName and email (optional) are added to observations in the database, useful if you share your observations with others.": "Navn brukes til opphavsrettsvannmerke på bilder.\nNavn og e-post (valgfritt) legges til observasjoner i databasen, nyttig hvis du deler observasjonene dine med andre.",
        "Name": "Navn",
        "Email": "E-post",
        "Save": "Lagre",
        "Language change will apply after restart. Incomplete translations fall back to English.": "Språkendring vil gjelde etter omstart. Ufullstendige oversettelser faller tilbake til engelsk.",
        "English": "Engelsk",
        "Norwegian": "Norsk",
        "Swedish": "Svensk",
        "German": "Tysk",
        "UI language:": "Brukergrensesnitt-språk:",
        "Vernacular names:": "Populærnavn:",
        "Color theme:": "Fargetema:",
        "Auto (follow system)": "Auto (følg systemet)",
        "Light": "Lys",
        "Dark": "Mørk"
    },
    "sv_SE": {
        "Sync content": "Synkronisera innehåll",
        "Include measures on synced images": "Inkludera mätningar på synkroniserade bilder",
        "Shows a scale bar on synced images": "Visar en skalstock på synkroniserade bilder",
        "Adds a visible watermark on synced images.": "Lägger till en synlig vattenstämpel på synkroniserade bilder.",
        "Enter your iNaturalist application credentials.\nFor open-source apps, leave the Client Secret blank to use the secure PKCE login flow.": "Ange dina iNaturalist-applikationsuppgifter.\nFör appar med öppen källkod, lämna klienthemligheten (Client Secret) tom för att använda det säkra PKCE-inloggningsflödet.",
        "Client Secret (optional for public apps)": "Klienthemlighet (valfritt för offentliga appar)",
        "Please enter a Client ID.": "Vänligen ange ett klient-ID.",
        "Reporting system:": "Rapporteringssystem:",
        "Calibration (Ctrl+K)": "Kalibrering (Ctrl+K)",
        "Settings (Ctrl+,)": "Inställningar (Ctrl+,)",
        "Preferences": "Preferenser",
        "Conflict resolution error: {err}": "Konfliktlösningsfel: {err}",
        "Upload failed: missing iNaturalist Client ID.": "Uppladdning misslyckades: saknar iNaturalist klient-ID.",
        "User profile": "Användarprofil",
        "Database": "Databas",
        "Online publishing": "Onlinepublicering",
        "Sporely Cloud": "Sporely Cloud",
        "Language": "Språk",
        "Appearance": "Utseende",
        "Close": "Stäng",
        "Name is used for the copyright watermark on images.\nName and email (optional) are added to observations in the database, useful if you share your observations with others.": "Namn används för upphovsrättsvattenstämpel på bilder.\nNamn och e-post (valfritt) läggs till i observationer i databasen, användbart om du delar dina observationer med andra.",
        "Name": "Namn",
        "Email": "E-post",
        "Save": "Spara",
        "Language change will apply after restart. Incomplete translations fall back to English.": "Språkändring kommer att gälla efter omstart. Ofullständiga översättningar faller tillbaka till engelska.",
        "English": "Engelska",
        "Norwegian": "Norska",
        "Swedish": "Svenska",
        "German": "Tyska",
        "UI language:": "Användargränssnittsspråk:",
        "Vernacular names:": "Trivialnamn:",
        "Color theme:": "Färgtema:",
        "Auto (follow system)": "Auto (följ system)",
        "Light": "Ljus",
        "Dark": "Mörk"
    },
    "de_DE": {
        "Sync content": "Inhalt synchronisieren",
        "Include measures on synced images": "Messungen in synchronisierten Bildern einschließen",
        "Shows a scale bar on synced images": "Zeigt einen Maßstab in synchronisierten Bildern an",
        "Adds a visible watermark on synced images.": "Fügt synchronisierten Bildern ein sichtbares Wasserzeichen hinzu.",
        "Enter your iNaturalist application credentials.\nFor open-source apps, leave the Client Secret blank to use the secure PKCE login flow.": "Geben Sie Ihre iNaturalist-Anmeldeinformationen ein.\nLassen Sie bei Open-Source-Apps das Client-Geheimnis (Client Secret) leer, um den sicheren PKCE-Anmeldeablauf zu verwenden.",
        "Client Secret (optional for public apps)": "Client-Geheimnis (optional für öffentliche Apps)",
        "Please enter a Client ID.": "Bitte geben Sie eine Client-ID ein.",
        "Reporting system:": "Meldesystem:",
        "Calibration (Ctrl+K)": "Kalibrierung (Strg+K)",
        "Settings (Ctrl+,)": "Einstellungen (Strg+,)",
        "Preferences": "Einstellungen",
        "Conflict resolution error: {err}": "Konfliktlösungsfehler: {err}",
        "Upload failed: missing iNaturalist Client ID.": "Upload fehlgeschlagen: fehlende iNaturalist Client-ID.",
        "User profile": "Benutzerprofil",
        "Database": "Datenbank",
        "Online publishing": "Online-Veröffentlichung",
        "Sporely Cloud": "Sporely Cloud",
        "Language": "Sprache",
        "Appearance": "Erscheinungsbild",
        "Close": "Schließen",
        "Name is used for the copyright watermark on images.\nName and email (optional) are added to observations in the database, useful if you share your observations with others.": "Der Name wird für das Copyright-Wasserzeichen auf Bildern verwendet.\nName und E-Mail (optional) werden zu Beobachtungen in der Datenbank hinzugefügt, nützlich, wenn Sie Ihre Beobachtungen mit anderen teilen.",
        "Name": "Name",
        "Email": "E-Mail",
        "Save": "Speichern",
        "Language change will apply after restart. Incomplete translations fall back to English.": "Sprachänderungen werden nach einem Neustart wirksam. Unvollständige Übersetzungen fallen auf Englisch zurück.",
        "English": "Englisch",
        "Norwegian": "Norwegisch",
        "Swedish": "Schwedisch",
        "German": "Deutsch",
        "UI language:": "UI-Sprache:",
        "Vernacular names:": "Trivialnamen:",
        "Color theme:": "Farbthema:",
        "Auto (follow system)": "Auto (System folgen)",
        "Light": "Hell",
        "Dark": "Dunkel"
    }
}

for lang in ['nb_NO', 'sv_SE', 'de_DE']:
    filepath = f"i18n/Sporely_{lang}.ts"
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
                        if source in lang_dict:
                            translation_elem.text = lang_dict[source]
                            if 'type' in translation_elem.attrib:
                                del translation_elem.attrib['type']
                            changed = True
                        else:
                            print(f"[{lang}] Missing translation string for: {repr(source)}")

        if changed:
            xml_str = ET.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')
            if '<!DOCTYPE' not in xml_str:
                if "<?xml version='1.0' encoding='utf-8'?>\n" in xml_str:
                    xml_str = xml_str.replace("<?xml version='1.0' encoding='utf-8'?>\n", '<?xml version="1.0" encoding="utf-8"?>\n<!DOCTYPE TS>\n', 1)
                elif '<?xml version="1.0" encoding="utf-8"?>\n' in xml_str:
                    xml_str = xml_str.replace('<?xml version="1.0" encoding="utf-8"?>\n', '<?xml version="1.0" encoding="utf-8"?>\n<!DOCTYPE TS>\n', 1)

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(xml_str)
            print(f"Updated {filepath}")
    except Exception as e:
        print(f"Error {lang}: {e}")

