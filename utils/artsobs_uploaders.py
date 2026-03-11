"""Uploader registry for Artsobservasjoner targets."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Protocol

import requests

from utils.artsobservasjoner_submit import ArtsObservasjonerClient, ArtsObservasjonerWebClient

ProgressCallback = Callable[[str, int, int], None]


@dataclass
class UploadResult:
    sighting_id: Optional[int]
    raw: dict | None


class ObservationUploader(Protocol):
    key: str
    label: str
    login_url: str

    def upload(
        self,
        observation: dict,
        image_paths: list[str],
        cookies: dict,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> UploadResult:
        ...


class ArtsobsMobileUploader:
    key = "mobile"
    label = "Artsobservasjoner (mobile)"
    login_url = "https://mobil.artsobservasjoner.no/bff/login?returnUrl=/my-page"

    def upload(
        self,
        observation: dict,
        image_paths: list[str],
        cookies: dict,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> UploadResult:
        client = ArtsObservasjonerClient(use_api=False)
        client.set_cookies_from_browser(cookies)

        submit_kwargs = dict(
            taxon_id=observation["taxon_id"],
            latitude=float(observation["latitude"]),
            longitude=float(observation["longitude"]),
            observed_datetime=observation["observed_datetime"],
            count=observation.get("count", 1),
            comment=observation.get("comment") or "",
            accuracy_meters=observation.get("accuracy_meters") or 25,
        )
        site_name = (observation.get("site_name") or "").strip()
        if site_name:
            submit_kwargs["site_name"] = site_name

        if hasattr(client, "create_sighting_mobile") and hasattr(client, "upload_image_mobile"):
            if progress_cb:
                progress_cb("Creating observation...", 1, len(image_paths) + 2)
            sighting_id, result = client.create_sighting_mobile(**submit_kwargs)

            for idx, path in enumerate(image_paths, start=1):
                if progress_cb:
                    progress_cb(
                        f"Uploading image {idx}/{len(image_paths)}...",
                        1 + idx,
                        len(image_paths) + 2
                    )
                client.upload_image_mobile(sighting_id, path)

            if progress_cb:
                progress_cb("Upload complete.", len(image_paths) + 2, len(image_paths) + 2)
            return UploadResult(sighting_id=sighting_id, raw=result)

        if progress_cb:
            progress_cb("Uploading observation...", 1, 1)
        result = client.submit_observation_mobile(
            **submit_kwargs,
            image_paths=image_paths,
        )
        sighting_id = ArtsObservasjonerClient._extract_sighting_id(result)
        return UploadResult(sighting_id=sighting_id, raw=result)


class ArtsobsWebUploader:
    key = "web"
    label = "Artsobservasjoner"
    login_url = "https://www.artsobservasjoner.no/Account/Login?ReturnUrl=%2FSubmitSighting%2FReport"

    def upload(
        self,
        observation: dict,
        image_paths: list[str],
        cookies: dict,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> UploadResult:
        client = ArtsObservasjonerWebClient()
        client.set_cookies_from_browser(cookies)
        result = client.submit_observation_web(
            taxon_id=observation["taxon_id"],
            observed_datetime=observation["observed_datetime"],
            site_id=observation.get("site_id"),
            site_name=observation.get("site_name"),
            latitude=observation.get("latitude"),
            longitude=observation.get("longitude"),
            accuracy_meters=observation.get("accuracy_meters"),
            count=observation.get("count", 1),
            habitat=observation.get("habitat"),
            notes=observation.get("notes"),
            open_comment=observation.get("open_comment"),
            private_comment=observation.get("private_comment"),
            interesting_comment=bool(observation.get("interesting_comment")),
            uncertain=bool(observation.get("uncertain")),
            unspontaneous=bool(observation.get("unspontaneous")),
            determination_method=observation.get("determination_method"),
            habitat_nin2_path=observation.get("habitat_nin2_path"),
            habitat_substrate_path=observation.get("habitat_substrate_path"),
            habitat_nin2_note=observation.get("habitat_nin2_note"),
            habitat_substrate_note=observation.get("habitat_substrate_note"),
            habitat_grows_on_note=observation.get("habitat_grows_on_note"),
            habitat_host_scientific=observation.get("habitat_host_scientific"),
            habitat_host_common_name=observation.get("habitat_host_common_name"),
            habitat_host_taxon_id=observation.get("habitat_host_taxon_id"),
            image_paths=image_paths,
            media_license=observation.get("image_license_code"),
            progress_cb=progress_cb,
        )
        return UploadResult(sighting_id=result.get("sighting_id"), raw=result)


class INaturalistUploader:
    key = "inat"
    label = "iNaturalist"
    login_url = "https://www.inaturalist.org/oauth/authorize"

    API_BASE_URL = "https://api.inaturalist.org/v1"

    def upload(
        self,
        observation: dict,
        image_paths: list[str],
        cookies: dict,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> UploadResult:
        access_token = (cookies or {}).get("access_token")
        if not access_token:
            raise RuntimeError("Missing iNaturalist access token.")

        headers = {"Authorization": f"Bearer {access_token}"}
        species_guess = (
            (observation.get("species_guess") or "").strip()
            or " ".join(
                part for part in [
                    (observation.get("genus") or "").strip(),
                    (observation.get("species") or "").strip(),
                ] if part
            ).strip()
            or "Fungi sp."
        )
        description = (observation.get("comment") or "").strip()
        create_data = {
            "observation[species_guess]": species_guess,
            "observation[description]": description,
            "observation[observed_on_string]": observation.get("observed_datetime") or "",
            "observation[latitude]": str(observation.get("latitude")),
            "observation[longitude]": str(observation.get("longitude")),
            "observation[positional_accuracy]": str(observation.get("accuracy_meters") or 25),
        }
        taxon_id = observation.get("inaturalist_taxon_id")
        if taxon_id:
            create_data["observation[taxon_id]"] = str(taxon_id)

        if progress_cb:
            progress_cb("Creating observation...", 1, max(2, len(image_paths) + 1))

        create_response = requests.post(
            f"{self.API_BASE_URL}/observations",
            headers=headers,
            data=create_data,
            timeout=30,
        )
        if create_response.status_code >= 400:
            raise RuntimeError(
                f"iNaturalist create observation failed ({create_response.status_code}): {create_response.text}"
            )
        create_payload = create_response.json()
        obs_id = None
        if isinstance(create_payload, dict):
            results = create_payload.get("results")
            if isinstance(results, list) and results:
                result0 = results[0] or {}
                obs_id = result0.get("id")
            if obs_id is None:
                obs_id = create_payload.get("id")
        if not obs_id:
            raise RuntimeError("iNaturalist response did not include observation id.")

        total_steps = max(2, len(image_paths) + 1)
        for idx, path in enumerate(image_paths or [], start=1):
            if progress_cb:
                progress_cb(f"Uploading image {idx}/{len(image_paths)}...", min(total_steps - 1, idx + 1), total_steps)
            with open(path, "rb") as handle:
                image_response = requests.post(
                    f"{self.API_BASE_URL}/observation_photos",
                    headers=headers,
                    data={"observation_photo[observation_id]": str(obs_id)},
                    files={"file": handle},
                    timeout=60,
                )
            if image_response.status_code >= 400:
                raise RuntimeError(
                    f"iNaturalist image upload failed ({image_response.status_code}): {image_response.text}"
                )

        if progress_cb:
            progress_cb("Upload complete.", total_steps, total_steps)
        return UploadResult(sighting_id=int(obs_id), raw=create_payload)


class MushroomObserverUploader:
    key = "mo"
    label = "Mushroom Observer"
    login_url = "https://mushroomobserver.org/api_keys"

    API_BASE_URL = "https://mushroomobserver.org/api2"

    @staticmethod
    def _safe_json(response: requests.Response) -> dict | list | None:
        try:
            payload = response.json()
        except Exception:
            return None
        if isinstance(payload, (dict, list)):
            return payload
        return None

    @staticmethod
    def _int_or_none(value) -> int | None:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    @classmethod
    def _extract_observation_id(cls, payload: dict | list | None) -> int | None:
        if isinstance(payload, list) and payload:
            return cls._extract_observation_id(payload[0])
        if not isinstance(payload, dict):
            return None

        direct = cls._int_or_none(payload.get("id"))
        if direct:
            return direct

        for key in ("observation_id",):
            value = cls._int_or_none(payload.get(key))
            if value:
                return value

        nested = payload.get("observation")
        if isinstance(nested, dict):
            value = cls._int_or_none(nested.get("id"))
            if value:
                return value

        for key in ("results", "result", "data", "observations"):
            nested_value = payload.get(key)
            value = cls._extract_observation_id(nested_value)
            if value:
                return value

        return None

    def upload(
        self,
        observation: dict,
        image_paths: list[str],
        cookies: dict,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> UploadResult:
        app_key = (cookies or {}).get("app_key")
        user_key = (cookies or {}).get("user_key")
        if not app_key or not user_key:
            raise RuntimeError("Missing Mushroom Observer app key or user key.")

        species_guess = (
            (observation.get("species_guess") or "").strip()
            or " ".join(
                part for part in [
                    (observation.get("genus") or "").strip(),
                    (observation.get("species") or "").strip(),
                ] if part
            ).strip()
            or "Fungi sp."
        )
        latitude = observation.get("latitude")
        longitude = observation.get("longitude")
        location_name = (observation.get("site_name") or "").strip()
        where_text = location_name
        if latitude is not None and longitude is not None:
            coord_text = f"{float(latitude):.6f}, {float(longitude):.6f}"
            where_text = f"{location_name} ({coord_text})" if location_name else coord_text
        notes = (observation.get("comment") or observation.get("open_comment") or "").strip()
        when_text = (observation.get("observed_datetime") or "").strip()

        params = {
            "api_key": app_key,
            "user": user_key,
            "format": "json",
        }
        create_data = {
            "observation[name]": species_guess,
            "observation[where]": where_text,
            "observation[when]": when_text,
            "observation[notes]": notes,
        }

        if progress_cb:
            progress_cb("Creating observation...", 1, max(2, len(image_paths) + 1))

        session = requests.Session()
        create_payload = None
        if image_paths:
            first_image = image_paths[0]
            with open(first_image, "rb") as handle:
                create_response = session.post(
                    f"{self.API_BASE_URL}/observations",
                    params=params,
                    data=create_data,
                    files={"image[image]": handle},
                    timeout=90,
                )
        else:
            create_response = session.post(
                f"{self.API_BASE_URL}/observations",
                params=params,
                data=create_data,
                timeout=60,
            )
        create_payload = self._safe_json(create_response)
        if create_response.status_code >= 400:
            raise RuntimeError(
                f"Mushroom Observer create observation failed ({create_response.status_code}): {create_response.text}"
            )
        obs_id = self._extract_observation_id(create_payload)
        if not obs_id:
            raise RuntimeError("Mushroom Observer response did not include observation id.")

        extra_images = image_paths[1:] if image_paths else []
        total_steps = max(2, len(image_paths) + 1)
        for idx, path in enumerate(extra_images, start=1):
            if progress_cb:
                progress_cb(
                    f"Uploading image {idx + 1}/{len(image_paths)}...",
                    min(total_steps - 1, idx + 1),
                    total_steps,
                )
            with open(path, "rb") as handle:
                image_response = session.post(
                    f"{self.API_BASE_URL}/images",
                    params=params,
                    data={"image[observation]": str(obs_id)},
                    files={"image[image]": handle},
                    timeout=90,
                )
            if image_response.status_code >= 400:
                raise RuntimeError(
                    f"Mushroom Observer image upload failed ({image_response.status_code}): {image_response.text}"
                )

        if progress_cb:
            progress_cb("Upload complete.", total_steps, total_steps)
        return UploadResult(
            sighting_id=int(obs_id),
            raw={
                "observation": create_payload,
                "images_uploaded": len(image_paths),
            },
        )


_UPLOADERS = {
    ArtsobsWebUploader.key: ArtsobsWebUploader(),
    INaturalistUploader.key: INaturalistUploader(),
    MushroomObserverUploader.key: MushroomObserverUploader(),
}


def list_uploaders() -> list[ObservationUploader]:
    return list(_UPLOADERS.values())


def get_uploader(key: str | None) -> ObservationUploader | None:
    normalized = (key or "").strip().lower()
    if not normalized:
        normalized = ArtsobsWebUploader.key
    if normalized == ArtsobsMobileUploader.key:
        normalized = ArtsobsWebUploader.key
    return _UPLOADERS.get(normalized)
