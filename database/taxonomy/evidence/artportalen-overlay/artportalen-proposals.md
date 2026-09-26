# Artportalen publishing-ID proposals

Target-specific publishing metadata: the Artportalen taxon id to report for the Sporely concept. Not evidence of Sporely identity.

Candidate `tax-2026.09.26-01` (SQLite `01e699ebfa9a…`), legacy database `724686455d3f…`.

| | Concepts |
|---|---:|
| Lacking an Artportalen id | 626906 |
| Of these, a same-named concept already carries the exact-name id (not proposed) | 2257 |
| With an exact-name or variant candidate (proposed) | 1263 |
| Uniquely obvious (`unique_exact`) | 814 |
| Needing review | 449 |
| &nbsp;&nbsp;`split` | 190 |
| &nbsp;&nbsp;`ambiguous_multiple_ids` | 0 |
| &nbsp;&nbsp;`ambiguous_homonym` | 4 |
| &nbsp;&nbsp;`ambiguous_id_in_use` | 255 |

Nothing here is accepted. Only entries copied into the overlay with `artportalen_overlay.py accept` are used by a build.

## Amanita muscaria

Sporely `78915` (accepted) — **split**

- Artportalen `2976` Amanita muscaria s.lat. (variant; attached to no concept)
- Artportalen `236537` Amanita muscaria s.str. (variant; attached to no concept)

Sporely `624588` (valid) — **split**

- Artportalen `2976` Amanita muscaria s.lat. (variant; attached to no concept)
- Artportalen `236537` Amanita muscaria s.str. (variant; attached to no concept)

## Sample: split

- `234` Cuphophyllus pratensis → 4392 Cuphophyllus pratensis s.lat., 239218 Cuphophyllus pratensis s.str.
- `240` Cuphophyllus virgineus → 4398 Cuphophyllus virgineus s.lat., 239228 Cuphophyllus virgineus s.str.
- `1776` Daldinia concentrica → 3855 Daldinia concentrica s. lat., 6003879 Daldinia concentrica s.str.
- `2668` Dialonectria episphaeria → 6033557 Dialonectria episphaeria s.str.
- `3177` Diatrype stigma → 3880 Diatrype stigma s.lat., 6324832 Diatrype stigma s.str.
- `7681` Entoloma bloxamii → 603 Entoloma bloxamii s.lat., 236704 Entoloma bloxamii s.str.
- `7746` Entoloma callichroum → 236723 Entoloma callichroum s.lat., 6051211 Entoloma callichroum s.str.
- `8289` Entoloma madidum → 236705 Entoloma madidum s.str.
- `8500` Erysiphe alphitoides → 259334 Erysiphe alphitoides s. lat., 6033545 Erysiphe alphitoides s. str.
- `10523` Gautieria graveolens → 202845 Gautieria graveolens s.str.

## Sample: ambiguous_homonym

- `604771` Sclerotium denigrans → 6028364 Sclerotium denigrans
- `604772` Sclerotium denigrans → 6028364 Sclerotium denigrans
- `608482` Phellodon frondosoniger → 6331650 Phellodon frondosoniger
- `624484` Phellodon frondosoniger → 6331650 Phellodon frondosoniger

## Sample: ambiguous_id_in_use

- `1315` Cytospora leucostoma → 6380 Cytospora leucostoma
- `3223` Diatrypella pulvinata → 258501 Diatrypella pulvinata
- `7808` Entoloma coeruleoflocculosum → 236689 Entoloma coeruleoflocculosum
- `7960` Entoloma farinasprellum → 237191 Entoloma farinasprellum
- `8090` Entoloma graphitipes → 6051184 Entoloma graphitipes
- `8235` Entoloma leochromus → 237237 Entoloma leochromus
- `8237` Entoloma lepiotosmum → 236714 Entoloma lepiotosmum
- `8266` Entoloma longistriatum → 3992 Entoloma longistriatum
- `8326` Entoloma melenosmum → 236895 Entoloma melenosmum
- `9167` Eutypella cerviculata → 4069 Eutypella cerviculata
