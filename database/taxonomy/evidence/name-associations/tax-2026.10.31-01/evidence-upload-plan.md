# Durable copy of the tax-2026.10.31-01 evidence archive: upload plan

Status: **prepared, not executed.** The upload is a production write and needs
the owner's approval for this operation. Prepared 2026-11-05.

Follows the procedure in `database/taxonomy/README.md` (frozen sets) and the
`tax-2026.10.23-11` precedent (`tax-2026.10.23-11.evidence-location.json`,
commit `733ffb5`).

## Object

| | |
|---|---|
| Source | `<releases>/tax-2026.10.31-01/frozen/tax-2026.10.31-01.evidence.tar.gz` |
| SHA-256 | `08e7ecac525f820771e1481753293a8d64066f23a35681e1be28ee5c77fb91d4` |
| Bytes | 2,139,480 |
| Freeze entry | `tax-2026.10.31-01.freeze.json` → `files["tax-2026.10.31-01.evidence.tar.gz"]` (same hash and size) |
| Freeze descriptor SHA-256 | `3b9aaa9ce00fe17cb9f47224fed04e68669976658ecdcf726039ab681c7425a5` (local frozen copy equals the committed one) |

## Destination

| | |
|---|---|
| Provider | Cloudflare R2, private |
| Bucket | `sporely-release-evidence` |
| Key | `taxonomy/evidence/sha256/08e7ecac525f820771e1481753293a8d64066f23a35681e1be28ee5c77fb91d4.tar.gz` |
| Immutability | bucket lock rule `taxonomy` on prefix `taxonomy/evidence/sha256/`, retention indefinite |
| Credentials | `~/.config/sporely/r2-release-evidence.env` (bucket-scoped S3 key); never printed or committed |

## Prechecks (done 2026-11-05, read-only)

- `verify_frozen` passes on the local frozen set, including the derivation
  evidence and its link to the base `tax-2026.10.23-11` freeze.
- Local archive SHA-256 equals the freeze entry above.
- `HEAD` on the destination key returns 404, so nothing is overwritten. `HEAD` on
  the `-11` key returns 200, so the credentials and bucket are right.

## Steps (after approval)

1. Re-run the prechecks: the archive hash, `verify_frozen` and `HEAD` 404.
2. Upload once, conditionally:

   ```sh
   set -a; . ~/.config/sporely/r2-release-evidence.env; set +a
   K=taxonomy/evidence/sha256/08e7ecac525f820771e1481753293a8d64066f23a35681e1be28ee5c77fb91d4.tar.gz
   curl -sSf --aws-sigv4 aws:amz:auto:s3 --user "$R2_ACCESS_KEY_ID:$R2_SECRET_ACCESS_KEY" \
     -H 'If-None-Match: *' -H 'Content-Type: application/gzip' \
     --upload-file "<releases>/tax-2026.10.31-01/frozen/tax-2026.10.31-01.evidence.tar.gz" \
     "https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com/$R2_BUCKET/$K"
   ```

3. Download it again into an empty directory and check the SHA-256 and size.
4. Copy the downloaded archive with `freeze.json`, `manifest.json`,
   `compatibility.json` and the `sqlite3.gz` from the frozen set into a new
   directory. Run `promote_desktop_bundle.verify_frozen` on it; it must pass.
5. Repeat the upload from step 2. It must be refused with HTTP 412 or 409, and
   the object must be unchanged.
6. Commit `database/reference_data/generated/taxonomy_v2/tax-2026.10.31-01.evidence-location.json`
   in the `sporely-evidence-location-v1` format (as for `-11`) with the results
   from steps 3–5. Remove the INBOX entry.

## Rollback

There is none, and none is needed. The key is content-addressed and locked; a
wrong object cannot be written under this key because the hash is checked
before upload and after download. If step 3 or 4 fails, stop and report: the
local frozen set is still the reference copy.
