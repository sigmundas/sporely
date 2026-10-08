# Stage 4 timeout investigation: raw queries, plans and outputs

Byte-identical copy of the files in
`~/sporely-scratch/vernacular-2026-10-07-stage4/timeout-investigation/` that
were not already committed, preserved before that scratch folder was removed
on 2026-10-08. The summary of this investigation is
`../timeout-investigation-report.md` (the scratch `report.md`).

Each `<name>.sql` is a read-only query that was run against production, and
`<name>.out` is its output. The matching `<name>.err` files were all empty and
are not copied. `final_checks.py`, `second_pass.py` and `alternative_plan.py`
drove the runs, and `candidate-plan-bound.json` records the candidate plan
bound for the validator fix.

`settings.out` is the production `pg_settings` snapshot. Its only file paths
are Supabase platform configuration paths (`/etc/postgresql-custom/...`). None
of these files contains credentials or observation data.
