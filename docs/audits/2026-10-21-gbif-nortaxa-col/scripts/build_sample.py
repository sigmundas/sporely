"""Draw the stratified NorTaxa sample and record Sporely's current identity for each.

Read-only. Inputs:
  --workdb   rev5 work DB (usages/taxa/nt_raw/col_raw/proposals), opened mode=ro
  --audit    docs/audits/2026-10-20-taxonomy-reconciliation
  --out      data/sample.csv (UTF-8 with BOM)

Strata (seed 20261021, each stratum split roughly half lichen-proxy / half non-lichen
where the population allows; lichen proxy = NorTaxa class in LICHEN_CLASSES):
  A1 approved manual bridge (manual_approved_exact)            20
  A2 approved supersession (reviewed_supersession, accepted)   20
  B1 automatic name bind, strict authorship                    20
  B2 automatic name bind, missing authorship                   10
  C1 NorTaxa-only, audit v2 verified candidate                 15
  C2 NorTaxa-only, audit v2 pragmatic candidate                20
  C3 NorTaxa-only, audit v2 unresolved pair                    20
  D  NorTaxa-only, compiler 'national_only' (no same-name COL) 25
  E  NorTaxa-only, compiler 'ambiguous' (several COL usages)   10
  F  NorTaxa synonym usage (intra_source_synonym)              15
  X  named worked examples                                      7
"""
import argparse, csv, os, random, sqlite3

LICHEN_CLASSES = {"Lecanoromycetes", "Arthoniomycetes", "Lichinomycetes", "Candelariomycetes", "Coniocybomycetes"}
EXAMPLES = ["294404", "53677", "52406", "69234", "57654", "172237", "172927"]
SIZES = dict(A1=20, A2=20, B1=20, B2=10, C1=15, C2=20, C3=20, D=25, E=10, F=15)


def read_csv(p):
    with open(p, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdb", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    c = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    nt = {r[0]: r for r in c.execute(
        "select taxon_id,name,authorship,rank,status,class,accepted_id,family,genus,kingdom,phylum,ord from nt_raw")}
    col_of_sid = dict(c.execute("select sid, ident from taxa where src='col_xr'"))
    usages = {r[0]: r for r in c.execute(
        "select ident,sid,binding,bec,status from usages where src='nortaxa'")}
    prop = {r[0]: r for r in c.execute("select nt_ident,proposal_class,candidates from proposals")}
    v2 = {}
    for kind, fn in [("verified", "verified_candidates"), ("pragmatic", "pragmatic_candidates"), ("unresolved", "unresolved_pairs")]:
        for r in read_csv(os.path.join(a.audit, "relationship-pass/v2/data", fn + ".csv")):
            v2.setdefault(r["nortaxa_taxon_id"], (kind, r["col_usage_id"], r["reason"]))

    def pop(pred):
        return sorted(i for i, u in usages.items() if pred(i, u))

    pops = {
        "A1": pop(lambda i, u: u[3] == "manual_approved_exact" and u[4] != "synonym"),
        "A2": pop(lambda i, u: u[3] == "reviewed_supersession" and u[4] != "synonym"),
        "B1": pop(lambda i, u: u[3] == "cross_source_automatic_exact_strict"),
        "B2": pop(lambda i, u: u[3] == "cross_source_automatic_exact_missing_authorship"),
        "C1": pop(lambda i, u: u[2] == "anchor" and v2.get(i, ("",))[0] == "verified"),
        "C2": pop(lambda i, u: u[2] == "anchor" and v2.get(i, ("",))[0] == "pragmatic"),
        "C3": pop(lambda i, u: u[2] == "anchor" and v2.get(i, ("",))[0] == "unresolved"),
        "D": pop(lambda i, u: u[2] == "anchor" and i not in v2 and prop.get(i, ("", ""))[1] == "national_only"),
        "E": pop(lambda i, u: u[2] == "anchor" and prop.get(i, ("", ""))[1] == "ambiguous"),
        "F": pop(lambda i, u: u[3] == "intra_source_synonym"),
    }
    rng = random.Random(20261021)
    chosen = []
    taken = set(EXAMPLES)
    for s, n in SIZES.items():
        p = [i for i in pops[s] if i not in taken]
        lich = [i for i in p if nt[i][5] in LICHEN_CLASSES]
        non = [i for i in p if nt[i][5] not in LICHEN_CLASSES]
        k = min(len(lich), n // 2)
        pick = rng.sample(lich, k) + rng.sample(non, min(len(non), n - k))
        for i in pick:
            chosen.append((s, i, len(pops[s])))
            taken.add(i)
    for i in EXAMPLES:
        chosen.append(("X", i, len(EXAMPLES)))

    cols = ["stratum", "stratum_population", "nortaxa_taxon_id", "scientific_name", "authorship", "rank",
            "nortaxa_status", "nortaxa_accepted_id", "kingdom", "phylum", "class", "order", "family", "genus",
            "lichen_proxy", "sporely_taxon_id", "sporely_binding", "sporely_bridge_class", "sporely_col_usage_id",
            "compiler_proposal", "compiler_candidates", "audit_v2_outcome", "audit_v2_col_usage_id", "audit_v2_reason"]
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for s, i, n in chosen:
            r = nt[i]
            u = usages[i]
            pr = prop.get(i, (None, "", ""))
            v = v2.get(i, ("", "", ""))
            w.writerow([s, n, i, r[1], r[2], r[3], r[4], r[6], r[9], r[10], r[5], r[11], r[7], r[8],
                        int(r[5] in LICHEN_CLASSES), u[1], u[2], u[3], col_of_sid.get(u[1], ""),
                        pr[1], pr[2], v[0], v[1], v[2]])
    print("sample rows:", len(chosen), {k: len(v) for k, v in pops.items()})


if __name__ == "__main__":
    main()
