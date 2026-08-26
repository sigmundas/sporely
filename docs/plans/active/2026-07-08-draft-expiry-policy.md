# Draft expiry policy

Status: Proposed; implementation not found.

## Agent handoff

- Status: Proposed.
- Last completed stage: None verified.
- Current/next stage: Confirm product policy, retention periods, grace behavior, and dependency on media garbage collection.
- Important decisions: Expiry is a warned soft-delete through existing tombstones, never an immediate hard delete.
- Do not: Expire published observations or silently delete drafts without the grace period.
- Remaining acceptance criteria: The policy and rollout requirements below.

Goal:
Give users a soft push toward either publishing an observation or
letting go of it, so long-abandoned drafts stop consuming R2 media
and DB rows. The paid-tier promise is "unlimited slots"; the
free-tier promise is "20 private slots + drafts that get cleaned up
if you never come back to them".

Policy sketch:

- Draft observations that have had no edits and no measurements
  added for D months are candidates for cleanup. Starting point:
  D = 6 months on free tier, D = 12 months on paid.
- Grace period: candidate observations are marked
  `expires_at = now() + 30 days` and the user is emailed once with
  a "keep", "publish now", or "let it go" link. A gentle in-app
  banner appears while `expires_at` is in the future.
- On `expires_at`:
  - Soft-delete the observation via the existing tombstone path
    (`deleted_at`), so a short recovery window applies.
  - R2 media garbage collection (see Stage E3) removes the image
    bytes when the tombstone crosses the media retention window.
- Drafts that flip `spore_data_visibility='public'` (Stage L) are
  exempt: they are contributing to the community dataset and should
  survive the expiry sweep as long as spore data is opted in.

Non-goals:

- Do not hard-delete anything at expiry — always go through the
  existing tombstone + recycle bin flow.
- Do not touch measurements on published observations; expiry is
  scoped to `is_draft = true` rows.
- Do not silently expire drafts without the email/banner grace
  window; the whole point is fair warning.

Rollout order:

1. sporely-web: add `observations.expires_at timestamptz NULL`,
   RPC + Edge Function to identify candidates and set expiry, RLS
   updates so users still see their own expiring drafts.
2. sporely-web: email hook + landing banner (or reuse existing
   notification surface).
3. sporely-py: banner + preferences copy explaining the free-tier
   draft policy; add "keep this draft" one-click action.
4. Enable the sweep in dry-run first (log candidates, no
   expiry set) and audit before flipping the switch.
