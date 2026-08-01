# W2D reconciliation fixtures

These are **synthetic, hand-authored** JSONL files. They are not exports
of real observations — every `observation_id` is a fixture label of the
form `fixture-<state>-NN` and every signal was constructed to exercise
one branch of the resolver.

Each JSONL file begins with a sentinel line:

```json
{"__synthetic__": true, "purpose": "..."}
```

The CLI loader drops the sentinel before feeding rows to
`ReconciliationInput.from_dict`. The sentinel is also the visible marker
that these fixtures must never be treated as real user data.

## Files

* `all_states.jsonl` — one record per reconciliation state (contract §4),
  plus a `resolved_exact` multi-signal-agreement fixture. This is the
  file the CLI verifier and end-to-end tests consume.

The `resolved_exact_via_synonym_relationship` fixture is intentionally
marked SKIPPED — see the `notes` field on that record and
`test_w2d_reconciliation.py` for the pinned-release reason.

## Taxonomy references

Fixture identifiers reference real Sporely IDs and COL usage IDs from
the pinned macrofungi release `tax-2026.08.01-01`:

| taxon_id | col_usage_id | scientific_name              | scope_state       |
|---------:|--------------|------------------------------|-------------------|
| 167      | 323XQ        | Crystallocystidium albescens | include           |
| 168      | 323XR        | Crystallocystidium albobadium| include           |
| 169      | 323XS        | Crystallocystidium albopurp. | include           |
| 931      | 33D          | Cyttariales                  | required_ancestor |

The `required_ancestor` row is used to test snapshot preservation for a
concept outside the macrofungi cache (contract §7).
