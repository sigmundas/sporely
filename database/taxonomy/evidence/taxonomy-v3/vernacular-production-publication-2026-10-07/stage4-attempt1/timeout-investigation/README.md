# Stage 4 timeout investigation: raw queries, plans and outputs

Byte-identical copy of the files in
`~/sporely-scratch/vernacular-2026-10-07-stage4/timeout-investigation/` that
were not already committed, preserved before that scratch folder was removed
on 2026-10-08. The summary of this investigation is
`../timeout-investigation-report.md` (the scratch `report.md`).

Each `<name>.sql` is a read-only query that was run against production, and
`<name>.out` is its output. The matching `<name>.err` files were all empty and
are not copied. Every `.sql` file begins `BEGIN READ ONLY;` and ends
`ROLLBACK;`. `../investigate_readonly.py` ran `settings`, `plan_target_absent`
and `plan_active_measured`; `final_checks.py`, `second_pass.py` and
`alternative_plan.py` (which import it) ran the others, except the two
`stage4b_*` queries, whose driver was not retained. `candidate-plan-bound.json`
records the candidate plan bound for the validator fix.

`settings.out` is the production `pg_settings` snapshot. Its only file paths
are Supabase platform configuration paths (`/etc/postgresql-custom/...`). None
of these files contains credentials or observation data.
