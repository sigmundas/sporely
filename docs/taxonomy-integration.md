# Taxonomy Integration

## Common Names and Scientific Names

- Enter a common name and MycoLog will look up matching taxonomy.
- Genus and species fields provide suggestions as you type.

## Taxon DB Build (Brief)

`database/build_multilang_vernacular_db.py` builds `database/vernacular_multilanguage.sqlite3` from:

- `database/taxon.txt` (required): only accepted species (`taxonRank=species`, `taxonomicStatus=valid`) are kept.
- `database/vernacular_inat_11lang.csv`: vernacular names are linked to accepted taxa only.
- `database/vernacularname.txt` (optional): Norwegian (`no`) names from Artsdatabanken override CSV Norwegian names.

`taxon.txt` and `vernacularname.txt` are downloaded from:
`https://ipt.artsdatabanken.no/resource?r=artsnavnebase&v=1.252`

Result:

- `taxon_min` contains the accepted species backbone.
- `vernacular_min` contains multilingual common names linked by taxon.
- Names not present in accepted `taxon.txt` are skipped.

## AI Suggestions

MycoLog can query Artsdatabanken (Artsorakelet) to suggest species based on images.

- Use AI suggestions as guidance, not as a definitive ID.
- You can crop images for better results.

## Reference Values

Reference values can be linked to specific species and sources, and plotted alongside your observations.

## See also
- [Artsobservasjoner login and upload](./artsobservasjoner.md)
- [Field photography](./field-photography.md)
- [Microscopy workflow](./microscopy-workflow.md)
- [Spore measurements](./spore-measurements.md)
- [Database structure](./database-structure.md)
