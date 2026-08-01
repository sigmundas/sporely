# W2C Phase-A global macrofungi scope evidence

## Verdict

Phase A produced a deterministic candidate, but Phase B is blocked pending
clade review. The candidate contains 52,881 selectable concepts rather than the
rejected W1 total of 634,894. It contains zero selectable plants, animals, or
other non-Fungi concepts.

The requested subphylum concepts are not present in the pinned COL release.
The executable policy therefore uses `Pucciniomycetes` (`H7`) and
`Ustilaginomycetes` (`K9`) as the authorized durable class-level exclusions.
`Gymnosporangium` (`4RXL`) overrides `H7`. Accepted `Mycosarcoma maydis`
(`B24TM`) overrides `K9`; pinned NameUsage evidence attaches `Ustilago maydis`
as a searchable synonym and does not create another identity.

## Scope and source audit

| Measure | Rejected W1 | Phase-A candidate |
|---|---:|---:|
| Concepts | 634,894 | 52,881 selectable + 36 ancestors |
| Species | 183,271 across W1 sources | 49,557 |
| Genera | 16,549 across W1 sources | 2,478 |
| Basidiomycota concepts | 221,099 COL | 47,447 |
| Ascomycota concepts | 329,139 COL | 5,434 |
| Other fungal phyla | 70,737 COL | 0 |
| Scientific aliases | 27,755 | 4,888 |
| Vernacular names | 10,294 | 3,923 |
| Authoritative mappings | 634,894 | 52,881 |

The first measured attempt exposed 143,509 UNITE sequence hypotheses beneath
the broadly included Agaricomycetes alone. The final engine applies a
source-characteristic exclusion to all 430,621 pinned `SH*.10FU` unranked
sequence-cluster concepts before clade inheritance. These concepts are not
organismal observation candidates.

## Principal winning rules

| Rule | COL ID | State | Concepts | Species |
|---|---|---:|---:|---:|
| Agaricomycetes | `7C` | include | 46,815 | 44,003 |
| Dacrymycetes | `622DM` | include | 250 | 228 |
| Pezizomycetes | `G4` | include | 3,666 | 3,238 |
| Geoglossomycetes | `BS` | include | 130 | 111 |
| Neolectomycetes | `F9` | include | 10 | 5 |
| Tremella | `7YQS` | include | 263 | 259 |
| Phaeotremella | `6MLH` | include | 18 | 17 |
| Naematelia | `5X2M` | include | 7 | 6 |
| Sirobasidium | `7HSB` | include | 12 | 11 |
| Pucciniomycetes remainder | `H7` | exclude | 9,043 | 8,680 |
| Gymnosporangium exception | `4RXL` | include | 81 | 80 |
| Ustilaginomycetes remainder | `K9` | exclude | 1,619 | 1,503 |
| Mycosarcoma maydis exception | `B24TM` | include | 1 | 1 |
| Elaphomycetaceae | `9LN` | include | 110 | 104 |

Selective Leotiomycetes and Sordariomycetes inclusions and their exact counts
are recorded in `desktop-candidate.json`.

## Export and desktop candidate

The seven-file applicable W1 dataset shape passed ordering, hash, row-count,
child-reference, and unique-taxon validation. Namespace-lost legacy mappings
are intentionally empty. The immutable W1 release was not changed.

| Measure | Result |
|---|---:|
| Semantic manifest SHA-256 | `72758b2c574e8aea27432b6b55c62dfb6ad87f3fadc11ad1c892a61abf23ac4e` |
| Uncompressed export bytes | 52,215,056 |
| Compressed export bytes | 2,688,182 |
| Desktop SQLite bytes | 19,001,344 |
| Desktop gzip bytes | 5,598,748 |
| Desktop SHA-256 | `bf70ae05735619f258db0cbbc598199c9b7decccacb24f34dbafed8dc14148dd` |
| Build time | 1.843222 seconds |

Clean repeated builds produced byte-identical semantic manifests, compressed
exports, and SQLite files. Indexed warm p50 measurements were approximately
0.0055 ms canonical exact, 0.0093 ms canonical prefix, 0.0049 ms synonym exact,
0.0078 ms vernacular exact, 0.0105 ms vernacular prefix, 0.1133 ms genus
autocomplete, and 0.0095 ms COL external resolution.

No desktop activation or release-pointer change occurred.

## Historical compatibility

The read-only local audit covered 337 observations. None has a populated stable
Sporely taxonomy ID, so no identity was inferred from scientific names. There
are 227 unresolved legacy external identities, 87 manual-name observations
without resolved identity, and 23 observations without identity evidence.
These remain eligible for later sparse registration; observations were not
changed.

## Blocking clade review

The final policy retains 42,420 concepts in `review`, including:

* 755 remaining Tremellomycetes concepts after the four approved seed genera;
* 11,898 Leotiomycetes concepts after explicit observable-genus inclusions;
* 27,114 Sordariomycetes concepts after explicit stromatic-genus inclusions;
* mixed Xylariaceae, Hypoxylaceae, and Diatrypaceae remainders;
* `Tolypocladium`, mould-dominated `Trichoderma`, and Atractiellomycetes.

Family-wide Xylariaceae and Hypoxylaceae inclusion is unsafe in the pinned
classification because both contain substantial non-macrofruiting/anamorphic
content. Leotiomycetes and Sordariomycetes still require a reviewed, bounded
family/genus decision set. Phase B must not begin until that burden is approved
as manageable.

Production writes performed: false. Phase B authorized: false. W3 authorized:
false.
