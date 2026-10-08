# Stage 4B preparation evidence

Byte-identical copy of the local working folder
`~/sporely-scratch/vernacular-2026-10-07-stage4b/`, preserved here before that
scratch folder was removed on 2026-10-08.

It is the local, pre-deployment evidence for the Stage 4B validator fix
(sporely-web `3f57439`, "preserve indexed parent validation for fresh
releases"): the full frozen-release validation against a local Supabase
database (`full_frozen_local.py`, `full-frozen-local.*`), validator timing
measurements (`measure_local.py`, `local-measure.*`), the regression and
taxonomy_v2 SQL suites (`sql_suite.py`, `regression.*`,
`taxonomy_v2_*_test.log`), the Node test suite log, Supabase advisor output
and the function security catalogue checks (`security_direct.py`,
`security-*`).

No production database was contacted to produce these files. The deployment
itself and its production checks are in `../stage4b-deploy/`, and the web
deployment report is
`sporely-web/docs/deployments/2026-10-07-taxonomy-validator-parent-reference.md`.
