-- Stage 0 baseline queries. Run against:
-- database/reference_data/generated/vernacular_multilanguage.sqlite3
PRAGMA integrity_check;
PRAGMA foreign_key_check;

SELECT name, type, tbl_name, sql
FROM sqlite_master
WHERE type IN ('table', 'index')
ORDER BY type, name;

SELECT 'taxon_min', count(*) FROM taxon_min
UNION ALL SELECT 'vernacular_min', count(*) FROM vernacular_min
UNION ALL SELECT 'scientific_name_min', count(*) FROM scientific_name_min
UNION ALL SELECT 'taxon_external_id_min', count(*) FROM taxon_external_id_min;

SELECT language_code, count(*) FROM vernacular_min GROUP BY language_code ORDER BY language_code;
SELECT source_system, count(*) FROM taxon_external_id_min GROUP BY source_system ORDER BY source_system;

SELECT * FROM taxon_min WHERE norwegian_taxon_id = 54995;
SELECT * FROM taxon_external_id_min WHERE external_id = 54995 ORDER BY source_system, taxon_id;
SELECT * FROM taxon_external_id_min WHERE cast(external_id AS text) = 'NBIC:54995';
SELECT * FROM taxon_min WHERE lower(canonical_scientific_name) = lower('Candolleomyces candolleanus');
SELECT * FROM scientific_name_min WHERE lower(scientific_name) = lower('Psathyrella candolleana');
SELECT * FROM vernacular_min WHERE lower(vernacular_name) = lower('hvit sprøsopp');

SELECT source_system, external_id, count(DISTINCT taxon_id) AS mapped_taxa
FROM taxon_external_id_min
GROUP BY source_system, external_id
HAVING mapped_taxa > 1;

SELECT taxon_id, language_code, lower(vernacular_name), count(*) AS copies
FROM vernacular_min
GROUP BY taxon_id, language_code, lower(vernacular_name)
HAVING copies > 1;
