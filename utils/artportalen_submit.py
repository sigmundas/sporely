"""Artportalen web submission helpers."""
from __future__ import annotations

import json
import mimetypes
import re
from pathlib import Path
from typing import Any, Dict, Optional

from utils.artsobservasjoner_submit import ArtsObservasjonerWebClient


class ArtportalenWebClient(ArtsObservasjonerWebClient):
    """Client for submitting observations via www.artportalen.se."""

    BASE_URL = "https://www.artportalen.se"
    DEFAULT_WEB_MEDIA_LICENSE = "60"

    def set_cookies_from_browser(self, cookies_dict: Dict[str, str]):
        for name, value in (cookies_dict or {}).items():
            self.session.cookies.set(name, value, domain=".artportalen.se")

    def submit_observation_web(
        self,
        taxon_id: int,
        observed_datetime,
        site_id: Optional[int] = None,
        site_name: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        accuracy_meters: Optional[int] = None,
        count: int = 1,
        habitat: Optional[str] = None,
        notes: Optional[str] = None,
        open_comment: Optional[str] = None,
        private_comment: Optional[str] = None,
        interesting_comment: bool = False,
        uncertain: bool = False,
        unspontaneous: bool = False,
        determination_method: Optional[int] = None,
        habitat_nin2_path: Optional[str] = None,
        habitat_substrate_path: Optional[str] = None,
        habitat_nin2_note: Optional[str] = None,
        habitat_substrate_note: Optional[str] = None,
        habitat_grows_on_note: Optional[str] = None,
        habitat_host_scientific: Optional[str] = None,
        habitat_host_common_name: Optional[str] = None,
        habitat_host_taxon_id: Optional[int] = None,
        image_paths: Optional[list[str]] = None,
        media_license: Optional[str] = None,
        progress_cb=None,
    ) -> Dict[str, Any]:
        if progress_cb:
            progress_cb("Loading report form...", 1, 1)
        report_html = self._load_report_form_html()
        token = self._extract_request_verification_token(report_html)
        if not token:
            raise RuntimeError("Could not find __RequestVerificationToken on the Artportalen report form.")
        before_save_ids = self._extract_sighting_ids_from_text(report_html)

        total_steps = 4 if (site_id is None and latitude is not None and longitude is not None) else 3
        if progress_cb:
            progress_cb("Validating date/time...", 1, total_steps)
        self._validate_start_datetime(observed_datetime)
        if progress_cb:
            progress_cb("Validating taxon...", 2, total_steps)
        self._validate_taxon(taxon_id)

        resolved_site_id = site_id
        resolved_site_name = (site_name or "").strip()
        new_site_context: Dict[str, Any] | None = None

        if resolved_site_id is None and latitude is not None and longitude is not None:
            if progress_cb:
                progress_cb("Preparing site...", 3, total_steps)
            new_site_context = self._prepare_new_site_context(
                token=token,
                latitude=float(latitude),
                longitude=float(longitude),
                site_name=resolved_site_name,
                accuracy_meters=accuracy_meters,
            )
            resolved_site_id = new_site_context.get("parent_site_id")
            parent_site_name = (new_site_context.get("parent_site_name") or "").strip()
            if not resolved_site_name:
                resolved_site_name = parent_site_name or new_site_context["site_name"]
        elif not resolved_site_id:
            resolved_site_id, found_site_name = self._resolve_site()
            if resolved_site_id and not resolved_site_name:
                resolved_site_name = found_site_name or ""
            if not resolved_site_id:
                resolved_site_id, cookie_site_name = self._resolve_site_from_cookies()
                if resolved_site_id and not resolved_site_name:
                    resolved_site_name = cookie_site_name or ""
            if not resolved_site_id:
                raise RuntimeError(
                    "No site selected and no coordinates are available for creating a new Artportalen site."
                )

        if progress_cb:
            progress_cb("Submitting observation...", total_steps, total_steps)

        payload = self._build_save_payload(
            token=token,
            taxon_id=taxon_id,
            observed_datetime=observed_datetime,
            site_id=resolved_site_id,
            site_name=resolved_site_name,
            count=count,
            habitat=habitat,
            notes=notes,
            open_comment=open_comment,
            private_comment=private_comment,
            interesting_comment=interesting_comment,
            uncertain=uncertain,
            unspontaneous=unspontaneous,
            determination_method=determination_method,
            habitat_nin2_path=habitat_nin2_path,
            habitat_substrate_path=habitat_substrate_path,
            habitat_nin2_note=habitat_nin2_note,
            habitat_substrate_note=habitat_substrate_note,
            habitat_grows_on_note=habitat_grows_on_note,
            habitat_host_scientific=habitat_host_scientific,
            habitat_host_common_name=habitat_host_common_name,
            habitat_host_taxon_id=habitat_host_taxon_id,
            new_site_context=new_site_context,
        )
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        response = self.session.post(
            f"{self.BASE_URL}/SubmitSighting/Report",
            data=payload,
            headers=headers,
            allow_redirects=True,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            raise RuntimeError(
                f"Artportalen upload failed ({response.status_code}): {response.text[:500]}"
            )

        sighting_id = self._extract_sighting_id(response.text)
        temporary_sighting_id = self._extract_temporary_sighting_id(response.text)
        if not sighting_id and temporary_sighting_id:
            sighting_id = self._resolve_sighting_id_from_temporary(temporary_sighting_id)
        if not sighting_id:
            sighting_id = self._extract_sighting_id(response.url or "")
        if not sighting_id:
            sighting_id = self._recover_sighting_id_from_grid()
        if not sighting_id:
            sighting_id = self._recover_saved_sighting_id(before_save_ids)
        if not sighting_id:
            raise RuntimeError("Artportalen upload succeeded, but no sighting ID could be found.")

        result: Dict[str, Any] = {"sighting_id": sighting_id}
        if image_paths:
            result["uploaded_images"] = self.upload_images_web(
                sighting_id=sighting_id,
                image_paths=image_paths,
                media_license=media_license,
                progress_cb=progress_cb,
            )
        return result

    def upload_images_web(
        self,
        sighting_id: int,
        image_paths: list[str],
        media_license: Optional[str] = None,
        progress_cb=None,
    ) -> list[Dict[str, Any]]:
        existing = []
        for path in image_paths:
            file_path = Path(path or "")
            if file_path.exists() and file_path.is_file():
                existing.append(str(file_path))
        if not existing:
            return []

        if progress_cb:
            progress_cb("Loading image editor...", 1, len(existing) + 2)

        try:
            editor_payload = self._load_artportalen_image_editor(sighting_id)
            token = self._extract_request_verification_token(editor_payload) or self._get_request_verification_token()
            upload_url = self._discover_artportalen_upload_url(editor_payload)
        except Exception:
            editor_payload = ""
            token = self._get_request_verification_token()
            upload_url = f"{self.BASE_URL}/Media/UploadImageAction"

        uploaded: list[Dict[str, Any]] = []
        for idx, image_path in enumerate(existing, start=1):
            if progress_cb:
                progress_cb(f"Uploading image {idx}/{len(existing)}...", idx + 1, len(existing) + 2)
            uploaded.append(
                self._upload_single_artportalen_image(
                    sighting_id=sighting_id,
                    image_path=image_path,
                    upload_url=upload_url,
                    token=token,
                    media_license=media_license,
                )
            )

        self._finalize_artportalen_images(sighting_id=sighting_id, token=token)
        return uploaded

    def _load_artportalen_image_editor(self, sighting_id: int) -> str:
        headers = {
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        url = f"{self.BASE_URL}/Media/ImagesForSightingOrDiaryEntry/"
        response = self.session.post(
            url,
            data={"sightingId": str(sighting_id)},
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            raise RuntimeError(
                f"Could not open Artportalen image editor ({response.status_code}): {response.text[:300]}"
            )
        return response.text or ""

    def _discover_artportalen_upload_url(self, payload: str) -> str:
        for url in self._extract_upload_targets_from_editor(payload):
            if "UploadImageAction" in url:
                return url
        return f"{self.BASE_URL}/Media/UploadImageAction"

    def _upload_single_artportalen_image(
        self,
        sighting_id: int,
        image_path: str,
        upload_url: str,
        token: str,
        media_license: Optional[str] = None,
    ) -> Dict[str, Any]:
        file_path = Path(image_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Image not found: {image_path}")
        file_mime, _encoding = mimetypes.guess_type(str(file_path))
        content_type = file_mime or "image/jpeg"
        headers = {
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        data = {
            "__RequestVerificationToken": token,
            "UploadImageViewModel.Sighting.Id": str(sighting_id),
            "UploadImageViewModel.MediaLicense": str(media_license or self.DEFAULT_WEB_MEDIA_LICENSE),
        }
        with file_path.open("rb") as handle:
            files = {
                "UploadImageViewModel.Image": (file_path.name, handle, content_type),
            }
            response = self.session.post(
                upload_url,
                data=data,
                files=files,
                headers=headers,
                timeout=self.UPLOAD_TIMEOUT,
            )
        if not response.ok:
            raise RuntimeError(
                f"Artportalen image upload failed ({response.status_code}): {response.text[:300]}"
            )
        return {
            "filename": file_path.name,
            "url": upload_url,
            "status_code": response.status_code,
        }

    def _finalize_artportalen_images(self, sighting_id: int, token: str) -> None:
        headers = {
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        payloads = [
            {"__RequestVerificationToken": token, "sightingId": str(sighting_id)},
            {"__RequestVerificationToken": token, "id": str(sighting_id)},
            json.dumps({"sightingId": str(sighting_id)}),
        ]
        last_error = None
        for payload in payloads:
            try:
                response = self.session.post(
                    f"{self.BASE_URL}/Media/SaveAllImages",
                    data=payload,
                    headers=headers,
                    timeout=self.REQUEST_TIMEOUT,
                )
            except Exception as exc:
                last_error = str(exc)
                continue
            if response.ok:
                return
            last_error = f"{response.status_code}: {response.text[:180]}"
        if last_error:
            raise RuntimeError(f"Artportalen image finalize failed: {last_error}")

    def _validate_site_coordinates(self, token: str, latitude: float, longitude: float) -> None:
        data = {
            "__RequestVerificationToken": token,
            "SiteViewModel.NewSite.NewSiteCoordinate.Latitude": self._format_coordinate(latitude),
            "SiteViewModel.NewSite.NewSiteCoordinate.Longitude": self._format_coordinate(longitude),
            "SiteViewModel.NewSite.NewSiteCoordinate.CoordinateSystem": "10",
            "SiteViewModel.NewSite.NewSiteCoordinate.CoordinateSystemNotation.Id": "",
            "SiteViewModel.NewSite.NewSiteCoordinate.MgrsNotation": "",
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        response = self.session.post(
            f"{self.BASE_URL}/SightingValidation/ValidateNewSiteCoordinates",
            data=data,
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            raise RuntimeError(
                f"Site coordinate validation failed ({response.status_code}): {response.text[:400]}"
            )
        lowered = (response.text or "").lower()
        if "outside sweden" in lowered or "utanför sverige" in lowered:
            raise RuntimeError("Coordinates rejected: outside Sweden.")

    def _build_save_payload(self, *args, **kwargs) -> Dict[str, str]:
        payload = super()._build_save_payload(*args, **kwargs)
        biotope_value = payload.pop("SightingViewModel.TemporarySighting.Sighting.BiotopeNiN2", "")
        payload["SightingViewModel.TemporarySighting.Sighting.Biotope"] = biotope_value
        payload["SightingViewModel.EditableProperties.Biotope.IsEditable"] = "True"
        payload["SightingViewModel.EditableProperties.BiotopeDescription.IsEditable"] = "True"
        payload["SightingViewModel.EditableProperties.Substrate.IsEditable"] = "True"
        payload["SightingViewModel.EditableProperties.SubstrateDescription.IsEditable"] = "True"
        payload["SightingViewModel.EditableProperties.SubstrateSpecies.IsEditable"] = "True"
        payload["SightingViewModel.EditableProperties.SubstrateSpeciesDescription.IsEditable"] = "True"
        return payload

    @staticmethod
    def _extract_sighting_id(text: str) -> Optional[int]:
        sighting_id = ArtsObservasjonerWebClient._extract_sighting_id(text)
        if sighting_id:
            return sighting_id
        match = re.search(r"/Sighting/(\d+)", str(text or ""))
        if match:
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                return None
        return None
