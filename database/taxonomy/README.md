# Sporely taxonomy contracts

This directory owns the versioned policy inputs for taxonomy schema version 2.
It does not contain acquisition code, source archives, the stable-ID registry,
or compiled taxonomy-v2 databases.

Policy files use JSON-compatible YAML so clean environments can validate them
with Python's standard library. Their meaning is documented in `docs/`, while
architecture rationale lives in `docs/architecture/decisions/`.

Validate offline:

```text
./.venv/bin/python database/taxonomy/validate_policies.py
./.venv/bin/pytest -q database/taxonomy/tests/test_policy_validation.py
```

`TAXONOMY_SCHEMA_VERSION` is `2`. Content releases use
`tax-YYYY.MM.DD-NN`; schema, content, source, and application versions are
independent.
