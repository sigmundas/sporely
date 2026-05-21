import csv
import requests
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict, List

# The 11 languages in the local database
LOCAL_LANGUAGES = ["en", "de", "fr", "es", "da", "sv", "no", "fi", "pl", "pt", "it"]

class GBIFTaxonomyService:
    """Service for fetching taxonomy data from GBIF"""

    BASE_URL = "https://api.gbif.org/v1/species"

    # Extended language code mapping for GBIF
    LANG_MAP = {
        'nob': 'no',  # Norwegian Bokmål
        'nno': 'no',  # Norwegian Nynorsk
        'eng': 'en',
        'swe': 'sv',
        'dut': 'nl',
        'spa': 'es',
        'ger': 'de',
        'deu': 'de',
        'fra': 'fr',
        'fre': 'fr',
        'dan': 'da',
        'fin': 'fi',
        'pol': 'pl',
        'por': 'pt',
        'ita': 'it',
    }

    @staticmethod
    def parse_gbif_id(gbif_id: str) -> str:
        """Extract numeric ID from 'GBIF:3341441' or return as-is"""
        return gbif_id.split(':')[1] if ':' in str(gbif_id) else str(gbif_id)
    
    @classmethod
    def search_by_name(cls, scientific_name: str) -> Optional[int]:
        """
        Search GBIF for a species by scientific name.
        Returns the GBIF taxon key (numeric ID) or None if not found.
        """
        url = f"{cls.BASE_URL}/match"
        try:
            response = requests.get(url, params={
                'name': scientific_name,
                'strict': 'false',
                'kingdom': 'Fungi'
            }, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('matchType') != 'NONE' and data.get('usageKey'):
                return data['usageKey']
            return None
        except requests.RequestException as e:
            print(f"Error searching GBIF for '{scientific_name}': {e}")
            return None

    @classmethod
    def get_vernacular_names(cls, gbif_id: str, languages: List[str] = None) -> Dict[str, List[str]]:
        """
        Get vernacular names for a taxon.
        Returns dict of {language_code: [list of names]}
        If languages is provided, only returns names for those languages.
        """
        numeric_id = cls.parse_gbif_id(gbif_id)
        url = f"{cls.BASE_URL}/{numeric_id}/vernacularNames"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            names_by_lang: Dict[str, List[str]] = {}

            for item in data.get('results', []):
                lang = item.get('language', '').lower()
                name = item.get('vernacularName', '').strip()

                if name and lang:
                    normalized_lang = cls.LANG_MAP.get(lang, lang)

                    # Skip if we're filtering by languages and this one isn't wanted
                    if languages and normalized_lang not in languages:
                        continue

                    if normalized_lang not in names_by_lang:
                        names_by_lang[normalized_lang] = []
                    if name not in names_by_lang[normalized_lang]:
                        names_by_lang[normalized_lang].append(name)

            return names_by_lang

        except requests.RequestException as e:
            print(f"Error fetching vernacular names: {e}")
            return {}
    
    @classmethod
    def get_full_taxonomy(cls, gbif_id: str) -> Optional[Dict]:
        """Get complete taxonomy information"""
        numeric_id = cls.parse_gbif_id(gbif_id)
        url = f"{cls.BASE_URL}/{numeric_id}"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            vernacular = cls.get_vernacular_names(gbif_id)
            
            return {
                'gbif_id': f"GBIF:{numeric_id}",
                'scientific_name': data.get('scientificName'),
                'canonical_name': data.get('canonicalName'),
                'family': data.get('family'),
                'genus': data.get('genus'),
                'vernacular_names': vernacular  # Dict[str, List[str]]
            }
            
        except requests.RequestException as e:
            print(f"Error fetching taxonomy: {e}")
            return None


def load_local_species(db_path: Path, limit: int = 100) -> List[Dict]:
    """
    Load species from local database with all vernacular names.
    Returns list of dicts with 'scientific_name' and 'vernacular' (Dict[str, List[str]]).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Get first N species
        taxa = conn.execute(
            """
            SELECT taxon_id, genus, specific_epithet
            FROM taxon_min
            ORDER BY taxon_id
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

        results = []
        for taxon in taxa:
            scientific_name = f"{taxon['genus']} {taxon['specific_epithet']}"

            # Get all vernacular names for this taxon
            names = conn.execute(
                """
                SELECT language_code, vernacular_name
                FROM vernacular_min
                WHERE taxon_id = ?
                ORDER BY language_code, is_preferred_name DESC
                """,
                (taxon['taxon_id'],)
            ).fetchall()

            vernacular: Dict[str, List[str]] = {}
            for row in names:
                lang = row['language_code']
                name = row['vernacular_name']
                if lang not in vernacular:
                    vernacular[lang] = []
                if name not in vernacular[lang]:
                    vernacular[lang].append(name)

            results.append({
                'scientific_name': scientific_name,
                'vernacular': vernacular
            })

        return results
    finally:
        conn.close()


def compare_vernaculars(local: Dict[str, List[str]], gbif: Dict[str, List[str]], languages: List[str]) -> Dict:
    """
    Compare local vs GBIF vernacular names for given languages.
    Returns comparison stats.
    """
    comparison = {
        'matches': [],      # Languages where local and GBIF have overlapping names
        'local_only': [],   # Languages with names only in local
        'gbif_only': [],    # Languages with names only in GBIF
        'different': [],    # Languages with names in both but no overlap
        'both_empty': [],   # Languages with no names in either
        'details': {}       # Detailed comparison per language
    }

    for lang in languages:
        local_names = set(n.lower() for n in local.get(lang, []))
        gbif_names = set(n.lower() for n in gbif.get(lang, []))

        comparison['details'][lang] = {
            'local': local.get(lang, []),
            'gbif': gbif.get(lang, [])
        }

        if not local_names and not gbif_names:
            comparison['both_empty'].append(lang)
        elif not local_names:
            comparison['gbif_only'].append(lang)
        elif not gbif_names:
            comparison['local_only'].append(lang)
        elif local_names & gbif_names:  # Intersection
            comparison['matches'].append(lang)
        else:
            comparison['different'].append(lang)

    return comparison


def save_results_to_csv(results: List[Dict], output_path: Path) -> None:
    """Save comparison results to CSV."""
    # Build header: species, gbif_key, then local_XX and gbif_XX for each language
    header = ['species', 'gbif_key']
    for lang in LOCAL_LANGUAGES:
        header.append(f'local_{lang}')
        header.append(f'gbif_{lang}')

    with output_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for entry in results:
            row = [entry['species'], entry.get('gbif_key', '')]
            for lang in LOCAL_LANGUAGES:
                local_names = entry['local'].get(lang, [])
                gbif_names = entry['gbif'].get(lang, [])
                # Join multiple names with semicolon
                row.append('; '.join(local_names))
                row.append('; '.join(gbif_names))
            writer.writerow(row)

    print(f"Results saved to {output_path}")


def load_results_from_csv(csv_path: Path) -> List[Dict]:
    """Load previously saved comparison results from CSV."""
    results = []
    with csv_path.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            entry = {
                'species': row['species'],
                'gbif_key': row['gbif_key'],
                'local': {},
                'gbif': {}
            }
            for lang in LOCAL_LANGUAGES:
                local_val = row.get(f'local_{lang}', '')
                gbif_val = row.get(f'gbif_{lang}', '')
                entry['local'][lang] = [n.strip() for n in local_val.split(';') if n.strip()] if local_val else []
                entry['gbif'][lang] = [n.strip() for n in gbif_val.split(';') if n.strip()] if gbif_val else []
            results.append(entry)
    return results


# Usage example
if __name__ == "__main__":
    # Find the database
    db_path = Path(__file__).resolve().with_name("reference_data") / "generated" / "vernacular_multilanguage.sqlite3"
    if not db_path.exists():
        db_path = Path(__file__).parent / "vernacular_multilanguage.sqlite3"
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        exit(1)

    csv_path = Path(__file__).resolve().with_name("reference_data") / "generated" / "gbif_comparison_results.csv"

    # Check if we have cached results
    if csv_path.exists():
        print(f"Loading cached results from {csv_path}...")
        all_results = load_results_from_csv(csv_path)
        print(f"Loaded {len(all_results)} species from cache\n")
    else:
        print(f"Loading first 100 species from {db_path}...")
        species_list = load_local_species(db_path, limit=100)
        print(f"Loaded {len(species_list)} species\n")

        service = GBIFTaxonomyService()
        all_results: List[Dict] = []

        for i, species in enumerate(species_list):
            sci_name = species['scientific_name']
            local_vern = species['vernacular']

            print(f"[{i+1}/100] {sci_name}")

            # Search GBIF for this species
            gbif_key = service.search_by_name(sci_name)
            if not gbif_key:
                print(f"  -> Not found in GBIF")
                all_results.append({
                    'species': sci_name,
                    'gbif_key': '',
                    'local': local_vern,
                    'gbif': {}
                })
                continue

            # Get GBIF vernacular names (only for our 11 languages)
            gbif_vern = service.get_vernacular_names(str(gbif_key), LOCAL_LANGUAGES)

            all_results.append({
                'species': sci_name,
                'gbif_key': str(gbif_key),
                'local': local_vern,
                'gbif': gbif_vern
            })

            # Rate limit to be nice to GBIF API
            time.sleep(0.1)

        # Save results to CSV
        save_results_to_csv(all_results, csv_path)

    # Now analyze the results
    total_compared = 0
    not_found_in_gbif = 0
    lang_stats = {lang: {'match': 0, 'local_only': 0, 'gbif_only': 0, 'different': 0, 'both_empty': 0}
                  for lang in LOCAL_LANGUAGES}
    local_only_entries: List[Dict] = []
    gbif_only_entries: List[Dict] = []

    for entry in all_results:
        if not entry['gbif_key']:
            not_found_in_gbif += 1
            continue

        total_compared += 1
        comparison = compare_vernaculars(entry['local'], entry['gbif'], LOCAL_LANGUAGES)

        for lang in comparison['matches']:
            lang_stats[lang]['match'] += 1
        for lang in comparison['local_only']:
            lang_stats[lang]['local_only'] += 1
        for lang in comparison['gbif_only']:
            lang_stats[lang]['gbif_only'] += 1
        for lang in comparison['different']:
            lang_stats[lang]['different'] += 1
        for lang in comparison['both_empty']:
            lang_stats[lang]['both_empty'] += 1

        if comparison['local_only']:
            local_only_entries.append({
                'species': entry['species'],
                'languages': comparison['local_only'],
                'details': {lang: comparison['details'][lang] for lang in comparison['local_only']}
            })
        if comparison['gbif_only']:
            gbif_only_entries.append({
                'species': entry['species'],
                'languages': comparison['gbif_only'],
                'details': {lang: comparison['details'][lang] for lang in comparison['gbif_only']}
            })

    # Print final summary
    print("\n" + "=" * 60)
    print("COMPARISON SUMMARY")
    print("=" * 60)
    print(f"Total species: {len(all_results)}")
    print(f"Found in GBIF: {total_compared}")
    print(f"Not found in GBIF: {not_found_in_gbif}")
    print()

    print(f"{'Language':<8} {'Match':>8} {'Local Only':>12} {'GBIF Only':>12} {'Different':>12} {'Both Empty':>12}")
    print("-" * 68)
    for lang in LOCAL_LANGUAGES:
        s = lang_stats[lang]
        print(f"{lang:<8} {s['match']:>8} {s['local_only']:>12} {s['gbif_only']:>12} {s['different']:>12} {s['both_empty']:>12}")

    # Print local-only entries
    print("\n" + "=" * 60)
    print(f"LOCAL ONLY ENTRIES ({len(local_only_entries)} species)")
    print("=" * 60)
    for entry in local_only_entries:
        print(f"\n{entry['species']}:")
        for lang in entry['languages']:
            names = entry['details'][lang]['local']
            print(f"  {lang}: {names}")

    # Print GBIF-only entries
    print("\n" + "=" * 60)
    print(f"GBIF ONLY ENTRIES ({len(gbif_only_entries)} species)")
    print("=" * 60)
    for entry in gbif_only_entries:
        print(f"\n{entry['species']}:")
        for lang in entry['languages']:
            names = entry['details'][lang]['gbif']
            print(f"  {lang}: {names}")
