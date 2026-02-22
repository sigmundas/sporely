# Online Publishing (Artsobservasjoner + iNaturalist)

This guide explains login and upload from MycoLog using:

- **Artsobservasjoner (mobile)**
- **Artsobservasjoner (web)**
- **iNaturalist**

All publishing settings are in **Settings -> Online publishing**.

## Login

1. Open **Settings -> Online publishing**.
2. Select a target in the **Websites** table.
3. Click **Log in**.
4. **Log out** only logs out the currently selected service.

### Artsobservasjoner (mobile)

- Uses embedded browser login.
- Cookies are cached in the MycoLog app data folder.

### Artsobservasjoner (web)

- Uses username/password prompt.
- Optional **Save login info on this device**.
- Username is stored in app settings.
- Password is stored via OS keyring (not plain text) when available.

### iNaturalist

- Click **Log in** and complete sign-in in your web browser.
- After a successful sign-in, MycoLog keeps you signed in so you do not need to sign in every day.
- If you click **Log out**, only the selected service is logged out.

## iNaturalist sign-in

1. Open **Settings -> Online publishing**.
2. Select **iNaturalist**.
3. Click **Log in**.
4. In the browser window, sign in to your iNaturalist account and approve access.
5. Return to MycoLog and continue publishing as normal.

If MycoLog asks for app credentials:

- Regular users should not create these.
- Use the values provided with the app, or contact the app maintainer.

## Cached files

Common paths:

- Windows: `%APPDATA%\\MycoLog\\...`
- macOS: `~/Library/Application Support/MycoLog/...`
- Linux: `~/.local/share/MycoLog/...`

Files:

- `artsobservasjoner_cookies_mobile.json`
- `artsobservasjoner_cookies_web.json`
- `inaturalist_oauth_tokens.json`

## Upload observations

1. Go to **Observations**.
2. Select one or more rows.
3. Click **Publish** and choose target.

Notes:

- Artsobservasjoner targets are disabled for observations already uploaded to Artsobservasjoner.
- iNaturalist can still be used for those observations.

## Requirements

### Artsobservasjoner

- Genus and species set (for Artsdatabanken taxon id).
- Observation date.
- GPS coordinates.
- At least one image.

### iNaturalist

- Observation date.
- GPS coordinates.
- At least one image.
- Active iNaturalist sign-in.

## After upload

- Artsobservasjoner ID is stored in `observations.artsdata_id`.
- iNaturalist ID is stored in `observations.inaturalist_id`.
- Observations table Artsobs web link opens:
  `https://www.artsobservasjoner.no/ReviewSighting`

## Add new uploaders

Upload targets are registered in `utils/artsobs_uploaders.py`.
Each uploader implements `upload(...)` with a unique `key`, `label`, and `login_url`.

## See also

- [Database structure](./database-structure.md)
- [Taxonomy integration](./taxonomy-integration.md)
- [Field photography](./field-photography.md)
- [Microscopy workflow](./microscopy-workflow.md)
