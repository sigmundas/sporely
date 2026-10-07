# Database-specific agent instructions

These rules apply to work under `database/` and supplement the repository-level `AGENTS.md`.

## Frozen and release artifacts

Temporary directories such as `/tmp` may be used for reproducible scratch work, intermediate builds, unpacked sources, temporary databases and comparison runs.

Do not leave the only copy of an accepted or stage-gating artifact in ephemeral storage.

Once a stage declares an artifact set frozen, accepted, a publication candidate, or otherwise requires later stages to consume those exact bytes:

- materialize the accepted artifact set in durable storage outside `/tmp`;
- record its durable location in the stage report/handoff;
- record cryptographic fingerprints for the complete accepted set;
- downstream stages must consume those verified frozen bytes rather than silently rebuilding them;
- a freeze stage must not report success while the only authoritative copy is in ephemeral storage.

If an accepted frozen set is missing:
- do not silently regenerate it;
- stop and report the missing artifact;
- re-materialization is permitted only from pinned inputs and unchanged code/policy;
- verify every previously accepted fingerprint;
- any mismatch is a hard stop.

## Release-critical reproducibility

Release-critical stage reports must record enough information to reproduce the stage without reconstructing the procedure from source inspection.

Prefer checked-in scripts or runbooks. Otherwise record the exact commands, inputs, output location and relevant environment assumptions used for the accepted run.

A later stage must not have to infer a release-critical command sequence from memory or implementation code when it can be recorded explicitly.

## Stage handoff

At a database/release stage boundary, record:
- accepted artifact location;
- artifact/manifest fingerprints;
- exact reproduction or verification command;
- whether the artifact is scratch, frozen or publication-authoritative;
- remaining blockers and the next permitted action.

Then stop at the assigned stage boundary.