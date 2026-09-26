# Sporely's species database

Sporely has a built-in list of fungus names, called the **species database**
(or *taxonomy*). Whenever you type a name in Sporely, it looks the name up in
this list. This page explains what is in it, where the names come from, how
it gets updated, and how another country's species list could be added.

It is written for people who use or look after Sporely, not for programmers.
Commands and file formats are in the
[technical taxonomy README](../database/taxonomy/README.md).

## What the database is for

The species database is what lets Sporely:

- suggest species while you type, from a scientific name, an old name
  (synonym) or a common name;
- show Norwegian (Bokmål and Nynorsk) and Sámi common names;
- show the Norwegian Red List category for a species;
- understand the species numbers that other systems use, for example the
  number the Norwegian Artsorakel image recogniser returns;
- keep the desktop app and the web/phone app agreeing on which species an
  observation belongs to.

## One permanent number per species

Every species in the database has a permanent **Sporely number**. Sporely
stores that number in an observation, not the name. When a species is
renamed, split or merged, the number still points to the right species, and
the observation keeps a record of the name you originally used.

Other systems have their own numbers. For example, Artsdatabanken's number for
*Entoloma conferendum* is 53482; Sporely's is 7821. Sporely keeps a
"bridge" that says the two refer to the same species. It never assumes that
two identical numbers from different systems mean the same thing, and it never
matches species by name alone.

## Where the names come from

| Source | Who publishes it | What Sporely takes from it | Version in use |
|---|---|---|---|
| **Catalogue of Life**, extended release (COL XR) | Catalogue of Life partnership | The worldwide list of fungi: accepted scientific names, synonyms and classification | 2026-07-17 |
| **NorTaxa** (Artsnavnebasen) | Artsdatabanken (Norway) | Norwegian species, Norwegian and Sámi common names, and Artsdatabanken species numbers | 1.284 (2026-07-17) |
| **Norsk rødliste for arter 2021** | Artsdatabanken | Red List categories for Norwegian species | 2021 edition |

The desktop app contains the full list: every fungus in the Catalogue of Life
plus the Norwegian NorTaxa species (about 635,000 entries). The web and phone
app use a smaller cloud copy, limited to mushroom-forming and other
larger fungi ("macrofungi", about 53,000 species). Both copies come from the
same build.

### Older names database

Before the current database there was an older one, which also held Swedish
names, Artportalen (Swedish) species numbers, and common names in English,
German, French, Spanish, Danish, Finnish, Polish, Portuguese and Italian taken
from iNaturalist. That older database still ships with the desktop app as a
backup, but **the current database does not yet include those extra languages
or the Artportalen and iNaturalist species numbers** (see
[Known gaps](#known-gaps)).

## Releases

Each version of the database is a **release**, named by its build date, for
example `tax-2026.09.23-01`. A release is never changed after it is published.
A correction becomes a new release.

- **Desktop:** the release is packed inside the app. The first time a new
  Sporely version starts, it unpacks the database into your user folder and
  checks that it is intact. After that it starts straight away.
- **Web and phone:** the same release is loaded into the cloud database and
  switched on there. The previous release is kept, so the cloud can be
  switched back at once if something is wrong.

The desktop app and the cloud should always use the same release. The cloud
release is switched on before a desktop version that depends on it is
published.

## How an update happens

An update is done by a developer and reviewed before anything reaches users.
In plain terms:

1. **Get the new source version.** Download a new NorTaxa or Catalogue of Life
   release. Sporely records exactly which file was used (its fingerprint,
   size, licence and date), so a build can always be traced back to its input.
2. **Convert it to a common format.** Each source has its own layout. A small
   converter turns it into the one format the rest of the process understands.
3. **Build the release.** The builder combines the sources. Species already in
   Sporely keep their Sporely number, and new species get new numbers.
   Uncertain matches are never guessed: they are either confirmed in a
   reviewed list of manual decisions or left unmatched and reported.
4. **Check it.** The build is run twice to confirm it gives identical results,
   and the result is compared with the previous release: how many species were
   added, renamed or merged, and whether known test cases still work.
5. **Package it for the desktop app.** The finished database is compressed and
   added to the app together with a description of what it contains.
6. **Prepare the cloud copy.** The macrofungi part is exported for the cloud
   database.
7. **Test end to end.** Two devices are synchronised against a test copy of the
   cloud, to show that observations still link to the right species.
8. **Switch on the cloud release, then release the desktop app.** The cloud
   step is done by hand by an operator, with a written rollback plan.

Previously stored observations are not rewritten by name during an update. If
a species is merged or split, the change is recorded as an explicit, reviewed
decision.

## Adding another country's species list

Norway's list is the first national list in Sporely. The process is designed
so that other countries, for example Sweden (Dyntaxa), Denmark or Finland,
can be added the same way. A national list adds:

- common names in that country's languages;
- that country's species numbers, so Sporely can understand results from, and
  report to, that country's recording portal;
- species that are recorded in that country but missing from the worldwide
  list.

A national list does **not** get its own species numbers. Every species keeps
one Sporely number, and the national list is linked to it.

### What is needed

- **The data file.** Preferably a *Darwin Core Archive*, the standard
  download format most national species lists (and GBIF) offer. It needs
  a species table and, if common names are wanted, a common-name table.
- **A fixed version.** A dated or numbered release, not "latest", so the build
  can be repeated.
- **Permission.** The licence must allow Sporely to redistribute the names
  (for example CC BY 4.0), and the required citation must be recorded.
- **Someone who knows the list**, to review the species that cannot be matched
  automatically.

### The steps

1. **Describe the source.** A developer creates a short description file (a
   "profile") for the new list: its name, version, licence, and which columns
   hold the name, rank, status, parent and common names. A tool can inspect
   the archive and suggest most of this.
2. **Check and convert it.** The same tool checks that the archive is
   complete and consistent, then converts it to Sporely's common format. It
   stops with a clear report rather than guessing when something is unusual.
3. **Connect it to the builder.** A developer registers the new source with
   the builder: what its species numbers are called, and in which order it is
   trusted relative to the others. Today this is a small, reviewed code change,
   because only NorTaxa has been added so far.
4. **Review the matches.** The builder links each national species to a
   Sporely species using the source's own references, never by name alone.
   Species it cannot match are listed for review. Reviewed decisions are
   recorded and reused in every later build.
5. **Add the languages.** The new languages are added to Sporely's language
   list so they appear in the common-name setting.
6. **Build, test and release** as described in
   [How an update happens](#how-an-update-happens). The desktop app and the web
   app both need to understand the new kind of species number, so the web side
   is updated in the same release.

## Known gaps

- **Swedish and other extra languages.** The current release
  (`tax-2026.09.23-01`) only has Norwegian Bokmål, Nynorsk and Northern Sámi
  common names. Swedish, English and the other iNaturalist languages are only
  in the older names database, which the app no longer reads while the new
  database is active.
- **Artportalen and iNaturalist numbers.** For the same reason, Sporely
  cannot currently look up an Artportalen or iNaturalist species number from
  the local database. Publishing to Artportalen relies on this lookup.
- **Red List licence.** The licence of the 2021 Red List file has not yet been
  independently confirmed. The citation is recorded.

The builder already has an option to carry the older database's extra
languages and species numbers into a new release; it was not used for the
current one. Using it, or adding Sweden's Dyntaxa list as a national source,
would close the first two gaps.

## Further reading

- [Technical taxonomy README](../database/taxonomy/README.md): build commands,
  file layout and contracts.
- [National source kit](../database/taxonomy/national_sources/README.md): the
  profile format used when adding a country.
- [Identity contract](../database/taxonomy/docs/identity-contract.md): the
  rules for Sporely numbers and bridges.
- [Red List overlay](../database/taxonomy/docs/redlist-overlay.md): how Red
  List categories are attached.
- [Taxonomy identity repair runbook](taxonomy-identity-repair-runbook.md):
  auditing and repairing observation identities.
