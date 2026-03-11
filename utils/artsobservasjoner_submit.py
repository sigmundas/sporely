"""
Artsobservasjoner Observation Submission Script

This script provides two approaches for submitting observations:
1. Cookie-based authentication (reusing browser session)
2. OAuth authentication (official API approach)
"""

import requests
import json
import re
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import base64


class ArtsObservasjonerClient:
    """Client for submitting observations to Artsobservasjoner"""
    
    # Mobile site endpoints (cookie-based)
    MOBILE_BASE_URL = "https://mobil.artsobservasjoner.no"
    
    # Official API endpoints (OAuth-based)
    API_BASE_URL = "https://api.artsobservasjoner.no/v1"
    API_TEST_URL = "https://apitest.artsobservasjoner.no/v1"
    
    def __init__(self, use_api: bool = False, api_test: bool = False):
        """
        Initialize the client
        
        Args:
            use_api: If True, use official API (requires OAuth). If False, use mobile site (requires cookies)
            api_test: If True and use_api=True, use test API endpoint
        """
        self.session = requests.Session()
        self.use_api = use_api
        
        if use_api:
            self.base_url = self.API_TEST_URL if api_test else self.API_BASE_URL
        else:
            self.base_url = self.MOBILE_BASE_URL
    
    # ========== APPROACH 1: Cookie-based (Mobile Site) ==========
    
    def set_cookies_from_browser(self, cookies_dict: Dict[str, str]):
        """
        Set cookies from your browser session
        
        Args:
            cookies_dict: Dictionary of cookie names and values
                         e.g., {'__Host-bff': 'chunks-2', '__Host-bffC1': '...', '__Host-bffC2': '...'}
        """
        for name, value in cookies_dict.items():
            self.session.cookies.set(name, value, domain='mobil.artsobservasjoner.no')
    
    def submit_observation_mobile(
        self,
        taxon_id: int,
        latitude: float,
        longitude: float,
        observed_datetime: datetime | str,
        image_path: Optional[str] = None,
        image_paths: Optional[list[str]] = None,
        site_id: Optional[int] = None,
        site_name: Optional[str] = None,
        count: int = 1,
        comment: Optional[str] = None,
        accuracy_meters: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Submit an observation using mobile site API (requires cookies)
        
        Args:
            taxon_id: Species ID from Artsdatabanken
            latitude: Observation latitude (WGS84)
            longitude: Observation longitude (WGS84)
            observed_datetime: When the observation was made (datetime or ISO string)
            image_path: Optional path to image file
            image_paths: Optional list of image paths
            site_id: Optional site ID (uses last used site if not provided)
            site_name: Optional site name to create a new site when site_id is not set
            count: Number of individuals observed
            comment: Optional observation comment
            accuracy_meters: GPS accuracy in meters
            
        Returns:
            Response from API containing observation ID
        """
        sighting_id, result = self.create_sighting_mobile(
            taxon_id=taxon_id,
            latitude=latitude,
            longitude=longitude,
            observed_datetime=observed_datetime,
            site_id=site_id,
            site_name=site_name,
            count=count,
            comment=comment,
            accuracy_meters=accuracy_meters,
        )
        
        # Upload image(s) if provided
        paths = []
        if image_paths:
            paths.extend([p for p in image_paths if p])
        elif image_path:
            paths.append(image_path)
        for path in paths:
            self._upload_image_mobile(sighting_id, path)

        return result if result is not None else {}

    def create_sighting_mobile(
        self,
        taxon_id: int,
        latitude: float,
        longitude: float,
        observed_datetime: datetime | str,
        site_id: Optional[int] = None,
        site_name: Optional[str] = None,
        count: int = 1,
        comment: Optional[str] = None,
        accuracy_meters: Optional[int] = None,
    ) -> tuple[int, Dict[str, Any] | None]:
        """Create a sighting on the mobile site and return (sighting_id, response)."""
        resolved_site_id = site_id
        new_site_info = None
        if resolved_site_id is None:
            resolved_site_id = self._get_last_used_site_id()
        if not resolved_site_id:
            site_label = (site_name or "").strip()
            if not site_label:
                site_label = f"MycoLog {latitude:.5f}, {longitude:.5f}"
            resolved_site_id = 0
            new_site_info = {
                "siteName": site_label,
                "longitude": longitude,
                "latitude": latitude,
                "isPolygon": False,
                "polygonCoordinates": None,
                "accuracy": accuracy_meters or 25,
            }

        observation = {
            "taxonId": taxon_id,
            "latitude": latitude,
            "longitude": longitude,
            "siteId": resolved_site_id,
            "startDate": self._format_start_date(observed_datetime),
            "startTime": self._format_start_time(observed_datetime),
            "quantity": count if count else None,
            "comment": comment or "",
        }
        if new_site_info:
            observation["newSiteInfo"] = new_site_info

        headers = {
            'Content-Type': 'application/json',
            'X-Csrf': '1',
            'Accept': 'application/json',
            'Referer': f'{self.MOBILE_BASE_URL}/contribute/submit-sightings'
        }

        url = f"{self.base_url}/core/Sightings"
        response = self.session.post(url, json=observation, headers=headers)
        if not response.ok:
            raise RuntimeError(
                f"Observation upload failed ({response.status_code}): {response.text}"
            )

        try:
            result = response.json()
        except ValueError:
            result = None

        sighting_id = self._extract_sighting_id(result)
        if not sighting_id:
            payload_preview = response.text.strip()
            if payload_preview:
                payload_preview = payload_preview[:1000]
            raise RuntimeError(
                "Observation upload did not return a sighting ID. "
                f"Response: {payload_preview or result}"
            )

        print(f"✓ Observation created with ID: {sighting_id}")
        return sighting_id, result

    def upload_image_mobile(self, sighting_id: int, image_path: str, license_code: str = "CC_BY_4"):
        """Public wrapper for uploading a single image to a sighting."""
        return self._upload_image_mobile(sighting_id, image_path, license_code=license_code)


    def _upload_image_mobile(self, sighting_id: int, image_path: str, license_code: str = "CC_BY_4"):
        """Upload an image to an existing observation"""
        
        image_file = Path(image_path)
        if not image_file.exists():
            raise FileNotFoundError(f"Image not found: {image_path}")
        
        # Prepare multipart upload (matches mobile UI)
        with open(image_path, 'rb') as handle:
            files = {
                'MediaFiles[0].File': (image_file.name, handle, 'image/jpeg')
            }
            data = {
                "sightingId": str(sighting_id),
                "MediaFiles[0].ImageLicense": license_code,
            }

            headers = {
                'X-Csrf': '1',
                'Referer': f'{self.MOBILE_BASE_URL}/contribute/submit-sightings'
            }

            url = f"{self.base_url}/core/MediaFiles/UploadImages?sightingId={sighting_id}"
            response = self.session.post(url, data=data, files=files, headers=headers)
            if not response.ok:
                raise RuntimeError(
                    f"Image upload failed ({response.status_code}): {response.text}"
                )
            
            print(f"✓ Image uploaded successfully")
            
            return response.json()

    @staticmethod
    def _extract_sighting_id(result: Any) -> int | None:
        if isinstance(result, dict):
            for key in ("Id", "id", "SightingId", "sightingId"):
                if key in result:
                    try:
                        return int(result[key])
                    except (TypeError, ValueError):
                        return None
            nested = result.get("data") or result.get("sighting") or result.get("Sighting")
            if isinstance(nested, dict):
                for key in ("Id", "id", "SightingId", "sightingId"):
                    if key in nested:
                        try:
                            return int(nested[key])
                        except (TypeError, ValueError):
                            return None
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict):
                    found = ArtsObservasjonerClient._extract_sighting_id(item)
                    if found:
                        return found
        return None

    def _format_observed_datetime(self, value: datetime | str) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).isoformat()
            except ValueError:
                return value
        return str(value)

    def _format_start_date(self, value: datetime | str) -> str:
        if isinstance(value, datetime):
            return f"{value.date().isoformat()}T00:00:00.000Z"
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
                return f"{parsed.date().isoformat()}T00:00:00.000Z"
            except ValueError:
                try:
                    parsed = datetime.strptime(value, "%d.%m.%Y %H:%M")
                    return f"{parsed.date().isoformat()}T00:00:00.000Z"
                except ValueError:
                    try:
                        parsed = datetime.strptime(value, "%d.%m.%Y %H:%M:%S")
                        return f"{parsed.date().isoformat()}T00:00:00.000Z"
                    except ValueError:
                        pass
                date_part = value.split("T")[0].split(" ")[0]
                return f"{date_part}T00:00:00.000Z"
        return str(value)

    def _format_start_time(self, value: datetime | str) -> str | None:
        if isinstance(value, datetime):
            return value.strftime("%H:%M")
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).strftime("%H:%M")
            except ValueError:
                try:
                    return datetime.strptime(value, "%d.%m.%Y %H:%M").strftime("%H:%M")
                except ValueError:
                    try:
                        return datetime.strptime(value, "%d.%m.%Y %H:%M:%S").strftime("%H:%M")
                    except ValueError:
                        pass
                if " " in value:
                    time_part = value.split(" ", 1)[1]
                    return time_part.split(":")[0] + ":" + time_part.split(":")[1]
        return None

    def _get_last_used_site_id(self) -> Optional[int]:
        headers = {
            'Accept': 'application/json',
            'X-Csrf': '1',
        }
        url = f"{self.base_url}/core/Sites/ByUser/LastUsed?top=1"
        response = self.session.get(url, headers=headers, timeout=10)
        if not response.ok:
            return None
        try:
            data = response.json()
        except Exception:
            return None
        if isinstance(data, list) and data:
            site_id = data[0].get("Id") or data[0].get("SiteId")
            try:
                return int(site_id)
            except (TypeError, ValueError):
                return None
        if isinstance(data, dict):
            site_id = data.get("Id") or data.get("SiteId")
            try:
                return int(site_id)
            except (TypeError, ValueError):
                return None
        return None
    
    # ========== APPROACH 2: OAuth API ==========
    
    def authenticate_oauth(
        self,
        client_id: str,
        client_secret: str,
        authorization_code: Optional[str] = None,
        access_token: Optional[str] = None
    ):
        """
        Authenticate using OAuth flow
        
        To get client_id and client_secret:
        - Contact Artsobservasjoner/Artsdatabanken to register your application
        
        Args:
            client_id: Your application's client ID
            client_secret: Your application's client secret
            authorization_code: Code from authorization step (if doing full OAuth)
            access_token: Previously obtained access token (to skip authorization)
        """
        if access_token:
            # Use existing token
            self.session.headers.update({
                'Authorization': f'Basic {access_token}'
            })
            return
        
        if not authorization_code:
            # Start OAuth flow
            auth_url = (
                f"{self.base_url}/authentication/authorize"
                f"?client_id={client_id}"
                f"&redirect_uri=YOUR_REDIRECT_URI"
                f"&state=RANDOM_STATE_STRING"
            )
            print(f"Visit this URL to authorize:\n{auth_url}")
            print("\nAfter authorization, you'll get a 'code' parameter in the redirect URL.")
            print("Pass that code to this function as 'authorization_code'")
            return
        
        # Exchange authorization code for access token
        url = (
            f"{self.base_url}/authentication/access_token"
            f"?client_id={client_id}"
            f"&client_secret={client_secret}"
            f"&code={authorization_code}"
            f"&state=RANDOM_STATE_STRING"
        )
        
        response = requests.get(url)
        response.raise_for_status()
        
        token_data = response.json()
        access_token = token_data['access_token']
        scheme = token_data['scheme']
        
        self.session.headers.update({
            'Authorization': f'{scheme} {access_token}'
        })
        
        print(f"✓ Authenticated as: {token_data['name']}")
        print(f"Token expires in: {token_data['expires_in']} seconds")
        
        return token_data
    
    def submit_observation_api(
        self,
        taxon_id: int,
        latitude: float,
        longitude: float,
        observed_datetime: datetime,
        image_path: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Submit observation using official API (requires OAuth authentication)
        
        Similar to submit_observation_mobile but uses official API endpoints
        """
        # This would use the official v1/sightings endpoint
        # The exact structure depends on the API specification
        
        observation = {
            "TaxonId": taxon_id,
            "Latitude": latitude,
            "Longitude": longitude,
            "ObservationDateTime": observed_datetime.isoformat(),
            **kwargs
        }
        
        url = f"{self.base_url}/sightings"
        response = self.session.post(url, json=observation)
        response.raise_for_status()
        
        return response.json()


class ArtsObservasjonerWebClient:
    """Client for submitting observations via www.artsobservasjoner.no (form post)."""

    BASE_URL = "https://www.artsobservasjoner.no"
    DEFAULT_WEB_MEDIA_LICENSE = "10"
    REQUEST_TIMEOUT = (10, 30)
    UPLOAD_TIMEOUT = (20, 300)

    def __init__(self):
        self.session = requests.Session()

    def set_cookies_from_browser(self, cookies_dict: Dict[str, str]):
        for name, value in cookies_dict.items():
            self.session.cookies.set(name, value, domain=".artsobservasjoner.no")

    def submit_observation_web(
        self,
        taxon_id: int,
        observed_datetime: datetime | str,
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
        progress_cb: Optional[callable] = None,
    ) -> Dict[str, Any]:
        if progress_cb:
            progress_cb("Loading report form...", 1, 1)
        report_html = self._load_report_form_html()
        token = self._extract_request_verification_token(report_html)
        if not token:
            raise RuntimeError("Could not find __RequestVerificationToken on report form.")
        before_save_ids = self._extract_sighting_ids_from_text(report_html)
        has_images = bool(image_paths)
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
                    "No site selected and no coordinates available for creating a new site."
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
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        url = f"{self.BASE_URL}/SubmitSighting/SaveSighting"
        response = self.session.post(
            url,
            data=payload,
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            raise RuntimeError(
                f"Observation upload failed ({response.status_code}): {response.text}"
            )
        sighting_id = self._extract_sighting_id(response.text)
        temporary_sighting_id = self._extract_temporary_sighting_id(response.text)
        if not sighting_id and temporary_sighting_id:
            sighting_id = self._resolve_sighting_id_from_temporary(temporary_sighting_id)
        if not sighting_id:
            if progress_cb:
                progress_cb("Resolving saved observation id...", total_steps, total_steps)
            sighting_id = self._recover_sighting_id_from_grid()
        if not sighting_id:
            sighting_id = self._recover_saved_sighting_id(before_save_ids)
        result: Dict[str, Any] = {"sighting_id": sighting_id}
        if has_images:
            if not sighting_id:
                raise RuntimeError(
                    "Observation was saved, but no sighting ID was returned or discovered; "
                    "cannot upload images."
                )
            uploaded = self.upload_images_web(
                sighting_id=sighting_id,
                image_paths=image_paths or [],
                media_license=media_license,
                progress_cb=progress_cb,
            )
            result["uploaded_images"] = uploaded
        return result

    def upload_images_web(
        self,
        sighting_id: int,
        image_paths: list[str],
        media_license: Optional[str] = None,
        progress_cb: Optional[callable] = None,
    ) -> list[Dict[str, Any]]:
        existing = []
        for path in image_paths:
            if not path:
                continue
            p = Path(path)
            if p.exists() and p.is_file():
                existing.append(str(p))
        if not existing:
            return []

        if progress_cb:
            progress_cb("Loading web image upload form...", 1, len(existing) + 1)
        editor_payload = self._load_editable_images_for_sighting(sighting_id)
        upload_targets = self._extract_upload_targets_from_editor(editor_payload)
        if not upload_targets:
            upload_targets = [
                f"{self.BASE_URL}/Media/UploadImage",
                f"{self.BASE_URL}/Media/UploadImages",
                f"{self.BASE_URL}/Media/UploadImageForSighting",
                f"{self.BASE_URL}/Media/UploadImagesForSighting",
            ]

        token = self._extract_request_verification_token(editor_payload)
        if not token:
            # EditableImagesForSightingId returns [] for a brand-new sighting with no
            # images yet — just an empty JSON array, so no HTML token to extract.
            # ASP.NET MVC anti-forgery cookie token != form token; they must be paired.
            # Sending the cookie value as the form field gives a 500.
            try:
                report_html = self._load_report_form_html()
                token = self._extract_request_verification_token(report_html)
            except Exception:
                pass

        uploaded: list[Dict[str, Any]] = []
        failures: list[str] = []
        for idx, image_path in enumerate(existing, start=1):
            if progress_cb:
                progress_cb(
                    f"Uploading web image {idx}/{len(existing)}...",
                    idx + 1,
                    len(existing) + 1,
                )
            try:
                action_error: Optional[str] = None
                try:
                    details = self._upload_single_web_image_action(
                        sighting_id=sighting_id,
                        image_path=image_path,
                        token=token,
                        media_license=media_license,
                    )
                except Exception as action_exc:
                    action_error = str(action_exc)
                    details = self._upload_single_web_image(
                        sighting_id=sighting_id,
                        image_path=image_path,
                        upload_targets=upload_targets,
                        token=token,
                    )
                uploaded.append(details)
            except Exception as exc:
                prefix = f"UploadImageAction({action_error}) | " if action_error else ""
                failures.append(f"{Path(image_path).name}: {prefix}{exc}")

        if failures:
            failure_text = "; ".join(failures[:3])
            raise RuntimeError(
                "One or more web image uploads failed. "
                f"{failure_text}."
            )
        return uploaded

    def _load_editable_images_for_sighting(self, sighting_id: int) -> str:
        url = f"{self.BASE_URL}/Media/EditableImagesForSightingId/"
        headers = {
            "Accept": "*/*",
            "Content-Type": "application/json; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        response = self.session.post(
            url,
            data=json.dumps({"sightingId": str(sighting_id)}),
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            raise RuntimeError(
                f"Could not open web image upload form ({response.status_code}): {response.text}"
            )
        return response.text or ""

    def _extract_upload_targets_from_editor(self, payload: str) -> list[str]:
        if not payload:
            return []
        patterns = [
            r'action=["\']([^"\']*Media[^"\']*(?:Upload|Image)[^"\']*)["\']',
            r'["\'](?:url|uploadUrl|action)["\']\s*:\s*["\']([^"\']*Media[^"\']*(?:Upload|Image)[^"\']*)["\']',
            r'(/Media/[A-Za-z0-9_/\-]*(?:Upload|Image)[A-Za-z0-9_/\-]*)',
        ]
        found: list[str] = []
        for pattern in patterns:
            for match in re.findall(pattern, payload, flags=re.IGNORECASE):
                if not match:
                    continue
                url = match if isinstance(match, str) else match[0]
                if not url:
                    continue
                if url.startswith("//"):
                    url = "https:" + url
                elif url.startswith("/"):
                    url = f"{self.BASE_URL}{url}"
                elif not url.startswith("http"):
                    url = f"{self.BASE_URL}/{url.lstrip('/')}"
                if url not in found:
                    found.append(url)
        return found

    def _extract_request_verification_token(self, html: str) -> Optional[str]:
        if not html:
            return None
        match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            html,
            flags=re.IGNORECASE,
        )
        if match:
            return match.group(1)
        return None

    def _upload_single_web_image_action(
        self,
        sighting_id: int,
        image_path: str,
        token: Optional[str],
        media_license: Optional[str] = None,
    ) -> Dict[str, Any]:
        file_path = Path(image_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Image not found: {image_path}")

        file_mime, _encoding = mimetypes.guess_type(str(file_path))
        content_type = file_mime or "image/jpeg"
        url = f"{self.BASE_URL}/Media/UploadImageAction"
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
            "Upgrade-Insecure-Requests": "1",
        }
        data = {
            "__RequestVerificationToken": token or "",
            "UploadImageViewModel.Sighting.Id": str(sighting_id),
        }
        # Browser captures show UploadImageAction accepts UploadImageViewModel.MediaLicense.
        license_value = str(media_license or self.DEFAULT_WEB_MEDIA_LICENSE).strip()
        if license_value not in {"10", "20", "30", "60"}:
            license_value = self.DEFAULT_WEB_MEDIA_LICENSE
        data["UploadImageViewModel.MediaLicense"] = license_value
        with open(file_path, "rb") as handle:
            files = {
                "UploadImageViewModel.Image": (file_path.name, handle, content_type),
            }
            response = self.session.post(
                url,
                data=data,
                files=files,
                headers=headers,
                timeout=self.UPLOAD_TIMEOUT,
            )

        # Trust the HTTP status code directly for this known endpoint.
        # _web_upload_response_ok scans the body for words like "error"/"feil"/
        # "exception" which appear in navigation and JS on virtually every page,
        # causing false negatives even when the upload succeeded.
        if not response.ok:
            raise RuntimeError(f"{response.status_code}: {response.text[:300]}")
        return {
            "filename": file_path.name,
            "url": url,
            "status_code": response.status_code,
            "field_name": "UploadImageViewModel.Image",
        }

    def _upload_single_web_image(
        self,
        sighting_id: int,
        image_path: str,
        upload_targets: list[str],
        token: Optional[str],
    ) -> Dict[str, Any]:
        file_path = Path(image_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Image not found: {image_path}")

        file_mime, _encoding = mimetypes.guess_type(str(file_path))
        content_type = file_mime or "image/jpeg"

        file_field_names = [
            "file",
            "File",
            "image",
            "Image",
            "qqfile",
            "MediaFile",
            "MediaFiles[0].File",
        ]
        id_field_names = ["sightingId", "SightingId", "id"]

        last_error: Optional[str] = None
        for url in upload_targets:
            for file_field in file_field_names:
                for id_field in id_field_names:
                    data = {
                        id_field: str(sighting_id),
                    }
                    if token:
                        data["__RequestVerificationToken"] = token
                    headers = {
                        "X-Requested-With": "XMLHttpRequest",
                        "Origin": self.BASE_URL,
                        "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
                    }
                    if token:
                        headers["RequestVerificationToken"] = token

                    with open(file_path, "rb") as handle:
                        files = {
                            file_field: (file_path.name, handle, content_type),
                        }
                        response = self.session.post(
                            url,
                            data=data,
                            files=files,
                            headers=headers,
                            timeout=self.UPLOAD_TIMEOUT,
                        )
                    if self._web_upload_response_ok(response):
                        return {
                            "filename": file_path.name,
                            "url": url,
                            "status_code": response.status_code,
                            "field_name": file_field,
                        }
                    last_error = (
                        f"{url} [{file_field}/{id_field}] -> "
                        f"{response.status_code}: {response.text[:180]}"
                    )

        raise RuntimeError(last_error or "No accepted upload endpoint was found.")

    @staticmethod
    def _web_upload_response_ok(response: requests.Response) -> bool:
        if response.status_code < 200 or response.status_code >= 300:
            return False

        content_type = (response.headers.get("Content-Type") or "").lower()
        text = (response.text or "").strip()

        if "application/json" in content_type:
            try:
                payload = response.json()
            except ValueError:
                return False
            if isinstance(payload, dict):
                if payload.get("success") is False:
                    return False
                errors = payload.get("errors") or payload.get("error")
                if errors:
                    return False
            return True

        lowered = text.lower()
        failure_markers = ["exception", "validation", "failed", "error", "feil", "ugyldig"]
        return not any(marker in lowered for marker in failure_markers)

    def _load_report_form_html(self) -> str:
        url = f"{self.BASE_URL}/SubmitSighting/Report"
        response = self.session.get(url, timeout=self.REQUEST_TIMEOUT)
        if not response.ok:
            raise RuntimeError(
                f"Failed to load report form ({response.status_code}): {response.text}"
            )
        return response.text or ""

    def _get_request_verification_token(self) -> str:
        html = self._load_report_form_html()
        token = self._extract_request_verification_token(html)
        if not token:
            raise RuntimeError("Could not find __RequestVerificationToken on report form.")
        return token

    def _recover_sighting_id_from_grid(self) -> Optional[int]:
        """Call BindSubmitSightingsGrid as the browser does after SaveSighting."""
        url = f"{self.BASE_URL}/SubmitSighting/BindSubmitSightingsGrid"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/plain, */*; q=0.01",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        try:
            response = self.session.post(
                url,
                data="page=1&size=25",
                headers=headers,
                timeout=self.REQUEST_TIMEOUT,
            )
        except Exception:
            return None
        if not response.ok:
            return None
        ids = self._extract_sighting_ids_from_text(response.text)
        return max(ids) if ids else None

    def _recover_saved_sighting_id(self, previous_ids: set[int] | None = None) -> Optional[int]:
        try:
            html = self._load_report_form_html()
        except Exception:
            return None
        current_ids = self._extract_sighting_ids_from_text(html)
        if not current_ids:
            return None
        if previous_ids:
            new_ids = [value for value in current_ids if value not in previous_ids]
            if new_ids:
                return max(new_ids)
        return max(current_ids)

    def _resolve_sighting_id_from_temporary(self, temporary_sighting_id: int) -> Optional[int]:
        url = f"{self.BASE_URL}/SightingDetail/FromTemporarySighting"
        headers = {
            "Accept": "*/*",
            "Content-Type": "application/json; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        payload = {"temporarySightingId": int(temporary_sighting_id)}
        response = self.session.post(
            url,
            data=json.dumps(payload),
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            return None
        return self._extract_sighting_id(response.text)

    def _validate_start_datetime(self, observed_datetime: datetime | str) -> None:
        date_str = self._format_date_ddmmyyyy(observed_datetime)
        time_str = self._format_time_hhmm(observed_datetime) or ""
        data = {
            "SightingViewModel.TemporarySighting.Sighting.StartDate": date_str,
            "SightingViewModel.TemporarySighting.Sighting.StartTime": time_str,
            "SightingViewModel.TemporarySighting.Sighting.EndDate": date_str,
            "SightingViewModel.TemporarySighting.Sighting.EndTime": time_str,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        url = f"{self.BASE_URL}/SightingValidation/ValidateStartDateTime"
        response = self.session.post(
            url,
            data=data,
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            raise RuntimeError(
                f"Start date validation failed ({response.status_code}): {response.text}"
            )

    def _validate_taxon(self, taxon_id: int) -> None:
        data = {
            "SightingViewModel.TemporarySighting.Sighting.Taxon": str(taxon_id)
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        url = f"{self.BASE_URL}/SightingValidation/ValidateTaxonReportable"
        response = self.session.post(
            url,
            data=data,
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            raise RuntimeError(
                f"Taxon validation failed ({response.status_code}): {response.text}"
            )

    def _prepare_new_site_context(
        self,
        token: str,
        latitude: float,
        longitude: float,
        site_name: str,
        accuracy_meters: Optional[int],
    ) -> Dict[str, Any]:
        self._validate_site_coordinates(
            token=token,
            latitude=latitude,
            longitude=longitude,
        )
        parent_site_id, parent_site_name = self._get_nearest_parent_site(
            latitude=latitude,
            longitude=longitude,
        )
        if not parent_site_id:
            parent_site_id, cookie_site_name = self._resolve_site_from_cookies()
            if not parent_site_name:
                parent_site_name = cookie_site_name
        if not parent_site_id:
            parent_site_id, user_site_name = self._resolve_site()
            if not parent_site_name:
                parent_site_name = user_site_name

        resolved_name = site_name.strip() if site_name else ""
        if not resolved_name:
            resolved_name = f"MycoLog {latitude:.5f}, {longitude:.5f}"

        accuracy = int(accuracy_meters) if accuracy_meters is not None else 25
        if accuracy < 0:
            accuracy = 0

        return {
            "site_name": resolved_name,
            "latitude": latitude,
            "longitude": longitude,
            "accuracy": accuracy,
            "parent_site_id": parent_site_id,
            "parent_site_name": parent_site_name or "",
        }

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
        url = f"{self.BASE_URL}/Site/ValidateSiteCoordinates"
        response = self.session.post(
            url,
            data=data,
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            raise RuntimeError(
                f"Site coordinate validation failed ({response.status_code}): {response.text}"
            )
        lowered = (response.text or "").lower()
        if "outside norway" in lowered or "utenfor norge" in lowered:
            raise RuntimeError("Coordinates rejected: outside Norway.")

    def _get_nearest_parent_site(
        self,
        latitude: float,
        longitude: float,
    ) -> tuple[Optional[int], Optional[str]]:
        payload = {
            "XCoord": self._format_coordinate(longitude),
            "YCoord": self._format_coordinate(latitude),
            "CoordinateSystemId": "10",
            "CoordinateSystemNotationId": "4",
            "currentSpeciesGroupId": "4",
            "currentTaxonId": "",
        }
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        url = f"{self.BASE_URL}/Site/GetNearestParentSites"
        response = self.session.post(
            url,
            data=json.dumps(payload),
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            return None, None
        try:
            data = response.json()
        except Exception:
            return None, None

        if isinstance(data, dict):
            for key in ("data", "results", "items", "sites"):
                candidate = data.get(key)
                if isinstance(candidate, list):
                    data = candidate
                    break
            if isinstance(data, dict):
                site_id = (
                    data.get("Id")
                    or data.get("id")
                    or data.get("SiteId")
                    or data.get("siteId")
                )
                name = data.get("Name") or data.get("name") or data.get("SiteName")
                try:
                    return int(site_id), name
                except (TypeError, ValueError):
                    return None, None

        if isinstance(data, list):
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                site_id = (
                    entry.get("Id")
                    or entry.get("id")
                    or entry.get("SiteId")
                    or entry.get("siteId")
                )
                name = entry.get("Name") or entry.get("name") or entry.get("SiteName")
                try:
                    return int(site_id), name
                except (TypeError, ValueError):
                    continue

        return None, None

    def _resolve_site(self) -> tuple[Optional[int], Optional[str]]:
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/SubmitSighting/Report",
        }
        url = f"{self.BASE_URL}/Site/GetUserSites"
        response = self.session.post(
            url,
            data="null",
            headers=headers,
            timeout=self.REQUEST_TIMEOUT,
        )
        if not response.ok:
            return None, None
        try:
            data = response.json()
        except Exception:
            return None, None

        if isinstance(data, dict):
            for key in ("data", "results", "items", "sites"):
                candidate = data.get(key)
                if isinstance(candidate, list):
                    data = candidate
                    break

        if isinstance(data, list) and data:
            for site in data:
                if not isinstance(site, dict):
                    continue
                site_id = site.get("Id") or site.get("id")
                name = site.get("Name") or site.get("name")
                try:
                    return int(site_id), name
                except (TypeError, ValueError):
                    continue
        return None, None

    def _resolve_site_from_cookies(self) -> tuple[Optional[int], Optional[str]]:
        site_id_raw = (
            self.session.cookies.get("SelectedSiteId")
            or self.session.cookies.get("selectedSiteId")
        )
        if not site_id_raw:
            return None, None
        try:
            return int(str(site_id_raw).strip()), None
        except (TypeError, ValueError):
            return None, None

    def _build_save_payload(
        self,
        token: str,
        taxon_id: int,
        observed_datetime: datetime | str,
        site_id: Optional[int],
        site_name: str,
        count: int,
        habitat: Optional[str],
        notes: Optional[str],
        open_comment: Optional[str],
        private_comment: Optional[str],
        interesting_comment: bool,
        uncertain: bool,
        unspontaneous: bool,
        determination_method: Optional[int],
        habitat_nin2_path: Optional[str] = None,
        habitat_substrate_path: Optional[str] = None,
        habitat_nin2_note: Optional[str] = None,
        habitat_substrate_note: Optional[str] = None,
        habitat_grows_on_note: Optional[str] = None,
        habitat_host_scientific: Optional[str] = None,
        habitat_host_common_name: Optional[str] = None,
        habitat_host_taxon_id: Optional[int] = None,
        new_site_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        date_str = self._format_date_ddmmyyyy(observed_datetime)
        time_str = self._format_time_hhmm(observed_datetime)
        open_comment_parts: list[str] = []
        if open_comment:
            open_comment_parts.append(open_comment.strip())
        elif notes:
            open_comment_parts.append(notes.strip())
        combined_open_comment = "\n".join([part for part in open_comment_parts if part]).strip()
        combined_private_comment = (private_comment or "").strip()
        selected_site_id = int(site_id) if site_id is not None else -1
        selected_site_name = site_name or ""
        if new_site_context and not selected_site_name:
            selected_site_name = new_site_context.get("parent_site_name") or new_site_context.get("site_name") or ""
        try:
            method_value = int(determination_method) if determination_method is not None else 0
        except (TypeError, ValueError):
            method_value = 0
        if method_value not in (1, 2, 3):
            method_value = 0
        biotope_id = self._extract_last_id_from_path(habitat_nin2_path)
        substrate_id = self._extract_last_id_from_path(habitat_substrate_path)
        substrate_species_id = None
        try:
            substrate_species_id = int(habitat_host_taxon_id) if habitat_host_taxon_id is not None else None
        except (TypeError, ValueError):
            substrate_species_id = None
        substrate_description = (habitat_substrate_note or "").strip()
        biotope_description = (habitat_nin2_note or "").strip()
        payload = {
            "__RequestVerificationToken": token,
            "SightingViewModel.CopyFromSightingId": "0",
            "SightingViewModel.ExternalMetadataId": "",
            "SightingViewModel.TemporarySighting.Id": "0",
            "SightingViewModel.EditableProperties.Taxon.IsEditable": "True",
            "_ignore_SightingViewModel.TemporarySighting.Sighting.Taxon": str(taxon_id),
            "SightingViewModel.TemporarySighting.Sighting.Taxon": str(taxon_id),
            "SightingViewModel.TemporarySighting.Sighting.Taxon_autoselect": "false",
            "SightingViewModel.TemporarySighting.Sighting.UnsureDetermination": "true" if uncertain else "false",
            "SightingViewModel.TemporarySighting.Sighting.Unspontaneous": "true" if unspontaneous else "false",
            "SightingViewModel.TemporarySighting.Sighting.StartDate": date_str,
            "SightingViewModel.TemporarySighting.Sighting.StartTime": time_str or "",
            "SightingViewModel.TemporarySighting.Sighting.EndDate": date_str,
            "SightingViewModel.TemporarySighting.Sighting.EndTime": time_str or "",
            "SightingViewModel.TemporarySighting.Sighting.Quantity": str(count),
            "SightingViewModel.TemporarySighting.Sighting.Unit": "0",
            "SightingViewModel.TemporarySighting.Sighting.PublicComment.Comment": combined_open_comment,
            "SightingViewModel.TemporarySighting.Sighting.PrivateComment.Comment": combined_private_comment,
            "SightingViewModel.TemporarySighting.Sighting.NoteOfInterest": "true" if interesting_comment else "false",
            "SightingViewModel.EditableProperties.NoteOfInterest.IsEditable": "True",
            "SightingViewModel.TemporarySighting.Sighting.BiotopeNiN2": str(biotope_id or ""),
            "SightingViewModel.TemporarySighting.Sighting.BiotopeDescription.Id": "0",
            "SightingViewModel.TemporarySighting.Sighting.BiotopeDescription.Description": biotope_description,
            "SightingViewModel.TemporarySighting.Sighting.Substrate": str(substrate_id or ""),
            "SightingViewModel.TemporarySighting.Sighting.SubstrateDescription.Id": "0",
            "SightingViewModel.TemporarySighting.Sighting.SubstrateDescription.Description": substrate_description,
            "SightingViewModel.TemporarySighting.Sighting.SubstrateSpecies": str(substrate_species_id or ""),
            "SightingViewModel.TemporarySighting.Sighting.SubstrateSpeciesDescription.Id": "0",
            "SightingViewModel.TemporarySighting.Sighting.SubstrateSpeciesDescription.Description": (habitat_grows_on_note or "").strip(),
            "SightingViewModel.TemporarySighting.Sighting.DeterminationMethod": str(method_value),
            "SightingViewModel.EditableProperties.DeterminationMethod.IsEditable": "True",
            "SightingViewModel.SelectedSite.Id": str(selected_site_id),
            "selectedSiteName": selected_site_name,
            "selectedSiteIsPrivate": "true",
            "selectedSiteIsFavorite": "false",
            "selectedSiteSpeciesGroupId": "0",
            "SightingViewModel.IsNewSite": "true" if new_site_context else "false",
            "SightingViewModel.NewSite.NewSiteCoordinate.Accuracy": "0",
            "currentSpeciesGroupId": "4",
        }
        if new_site_context:
            new_site_name = new_site_context["site_name"]
            new_site_lat = self._format_coordinate(float(new_site_context["latitude"]))
            new_site_lon = self._format_coordinate(float(new_site_context["longitude"]))
            accuracy_str = str(int(new_site_context.get("accuracy", 25)))
            parent_id = int(new_site_context.get("parent_site_id") or -1)
            payload.update(
                {
                    "SiteViewModel.NewSite.Site.Name": new_site_name,
                    "SightingViewModel.NewSite.Site.Name": new_site_name,
                    "SiteViewModel.NewSite.NewSiteCoordinate.CoordinateSystem": "10",
                    "SiteViewModel.NewSite.NewSiteCoordinate.CoordinateSystem_csgroup": "3",
                    "SiteViewModel.NewSite.NewSiteCoordinate.CoordinateSystem_csnotation": "4",
                    "SiteViewModel.NewSite.NewSiteCoordinate.CoordinateSystem_selectedNotation": "4",
                    "SightingViewModel.NewSite.NewSiteCoordinate.CoordinateSystem": "10",
                    "SightingViewModel.NewSite.NewSiteCoordinate.CoordinateSystem_csgroup": "3",
                    "SightingViewModel.NewSite.NewSiteCoordinate.CoordinateSystem_csnotation": "4",
                    "SightingViewModel.NewSite.NewSiteCoordinate.CoordinateSystem_selectedNotation": "4",
                    "SightingViewModel.NewSite.NewSiteCoordinate.CoordinateSystemNotation.Id": "4",
                    "SiteViewModel.NewSite.NewSiteCoordinate.CoordEast": "0",
                    "SightingViewModel.NewSite.NewSiteCoordinate.CoordEast": "0",
                    "SiteViewModel.NewSite.NewSiteCoordinate.CoordNorth": "0",
                    "SightingViewModel.NewSite.NewSiteCoordinate.CoordNorth": "0",
                    "SiteViewModel.NewSite.NewSiteCoordinate.Longitude": new_site_lon,
                    "SightingViewModel.NewSite.NewSiteCoordinate.Longitude": new_site_lon,
                    "SiteViewModel.NewSite.NewSiteCoordinate.Latitude": new_site_lat,
                    "SightingViewModel.NewSite.NewSiteCoordinate.Latitude": new_site_lat,
                    "SiteViewModel.NewSite.NewSiteCoordinate.MgrsNotation": "",
                    "SightingViewModel.NewSite.NewSiteCoordinate.MgrsNotation": "",
                    "SiteViewModel.NewSite.Site.AccuracyDisplay": accuracy_str,
                    "SightingViewModel.NewSite.Site.AccuracyDisplay": accuracy_str,
                    "SightingViewModel.NewSite.NewSiteCoordinate.Accuracy": accuracy_str,
                    "SightingViewModel.NewSite.Site.Parent": str(parent_id),
                }
            )
        return payload

    @staticmethod
    def _extract_last_id_from_path(path_value: Any) -> Optional[int]:
        if path_value is None:
            return None
        parsed = path_value
        if isinstance(path_value, str):
            raw = path_value.strip()
            if not raw:
                return None
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = raw
        if isinstance(parsed, list):
            for item in reversed(parsed):
                try:
                    return int(item)
                except (TypeError, ValueError):
                    continue
            return None
        try:
            return int(parsed)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_sighting_id(text: str) -> Optional[int]:
        ids = ArtsObservasjonerWebClient._extract_sighting_ids_from_text(text)
        if not ids:
            return None
        return max(ids)

    @staticmethod
    def _extract_sighting_ids_from_text(text: str) -> set[int]:
        if not text:
            return set()
        found: set[int] = set()

        try:
            payload = json.loads(text)
        except Exception:
            payload = None
        if payload is not None:
            ArtsObservasjonerWebClient._collect_sighting_ids_from_payload(payload, found)

        patterns = [
            r"data-sighting-id=[\"'](\d+)[\"']",
            r"/Sighting/(\d+)",
            r"SightingId\\D+(\\d+)",
            r"sightingId[\"']?\s*[:=]\s*[\"']?(\d+)",
            r"\"SightingId\"\s*:\s*(\d+)",
        ]
        for pattern in patterns:
            for match in re.findall(pattern, text):
                try:
                    found.add(int(match))
                except (TypeError, ValueError):
                    continue
        return found

    @staticmethod
    def _extract_temporary_sighting_id(text: str) -> Optional[int]:
        if not text:
            return None

        try:
            payload = json.loads(text)
        except Exception:
            payload = None

        value = ArtsObservasjonerWebClient._find_numeric_key_in_payload(
            payload,
            key_names={"temporarySightingId", "TemporarySightingId"},
        )
        if value is not None:
            return value

        patterns = [
            r"temporarySightingId[\"']?\s*[:=]\s*[\"']?(\d+)",
            r"TemporarySightingId\\D+(\\d+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    return int(match.group(1))
                except (TypeError, ValueError):
                    continue
        return None

    @staticmethod
    def _find_numeric_key_in_payload(payload: Any, key_names: set[str]) -> Optional[int]:
        if payload is None:
            return None
        if isinstance(payload, dict):
            for key, value in payload.items():
                if key in key_names:
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        pass
                nested = ArtsObservasjonerWebClient._find_numeric_key_in_payload(value, key_names)
                if nested is not None:
                    return nested
            return None
        if isinstance(payload, list):
            for value in payload:
                nested = ArtsObservasjonerWebClient._find_numeric_key_in_payload(value, key_names)
                if nested is not None:
                    return nested
        return None

    @staticmethod
    def _collect_sighting_ids_from_payload(payload: Any, sink: set[int]) -> None:
        if isinstance(payload, dict):
            # Common direct keys.
            for key in ("SightingId", "sightingId"):
                value = payload.get(key)
                try:
                    if value is not None:
                        sink.add(int(value))
                except (TypeError, ValueError):
                    pass

            # Common wrapper shape: {"Sighting": {"Id": ...}}
            for wrapper in ("Sighting", "sighting", "SavedSighting", "savedSighting"):
                nested = payload.get(wrapper)
                if isinstance(nested, dict):
                    try:
                        nested_id = nested.get("Id") or nested.get("id")
                        if nested_id is not None:
                            sink.add(int(nested_id))
                    except (TypeError, ValueError):
                        pass

            # Fallback for simple payloads with a top-level id.
            if ("Id" in payload or "id" in payload) and len(payload) <= 4:
                top = payload.get("Id") if "Id" in payload else payload.get("id")
                try:
                    if top is not None:
                        sink.add(int(top))
                except (TypeError, ValueError):
                    pass

            for value in payload.values():
                ArtsObservasjonerWebClient._collect_sighting_ids_from_payload(value, sink)
            return

        if isinstance(payload, list):
            for value in payload:
                ArtsObservasjonerWebClient._collect_sighting_ids_from_payload(value, sink)

    @staticmethod
    def _format_date_ddmmyyyy(value: datetime | str) -> str:
        if isinstance(value, datetime):
            return value.strftime("%d.%m.%Y")
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
                return parsed.strftime("%d.%m.%Y")
            except ValueError:
                try:
                    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M")
                    return parsed.strftime("%d.%m.%Y")
                except ValueError:
                    try:
                        parsed = datetime.strptime(value, "%Y-%m-%d")
                        return parsed.strftime("%d.%m.%Y")
                    except ValueError:
                        return value.split(" ")[0]
        return str(value)

    @staticmethod
    def _format_time_hhmm(value: datetime | str) -> str | None:
        if isinstance(value, datetime):
            return value.strftime("%H:%M")
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
                return parsed.strftime("%H:%M")
            except ValueError:
                try:
                    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M")
                    return parsed.strftime("%H:%M")
                except ValueError:
                    pass
                if " " in value:
                    time_part = value.split(" ", 1)[1]
                    return ":".join(time_part.split(":")[:2])
        return None

    @staticmethod
    def _format_coordinate(value: float | str) -> str:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return str(value)
        text = f"{num:.7f}".rstrip("0").rstrip(".")
        return text or "0"


# ========== HELPER FUNCTIONS ==========

def extract_cookies_from_browser():
    """
    Helper to extract cookies from your browser
    
    FIREFOX:
    1. Open Firefox
    2. Visit https://mobil.artsobservasjoner.no (logged in)
    3. Press F12 to open Developer Tools
    4. Go to Storage tab -> Cookies
    5. Copy the values for __Host-bff, __Host-bffC1, __Host-bffC2
    
    CHROME:
    1. Open Chrome
    2. Visit https://mobil.artsobservasjoner.no (logged in)
    3. Press F12 to open Developer Tools
    4. Go to Application tab -> Cookies
    5. Copy the values
    
    Returns a dict like:
    {
        '__Host-bff': 'chunks-2',
        '__Host-bffC1': 'CfDJ8H96...',
        '__Host-bffC2': 'CwjI1fy0...'
    }
    """
    print("To extract cookies from your browser:")
    print("1. Visit https://mobil.artsobservasjoner.no (while logged in)")
    print("2. Open Developer Tools (F12)")
    print("3. Firefox: Storage -> Cookies | Chrome: Application -> Cookies")
    print("4. Copy the values for __Host-bff, __Host-bffC1, __Host-bffC2")
    print("\nReturn them in this format:")
    print("""
    cookies = {
        '__Host-bff': 'chunks-2',
        '__Host-bffC1': 'YOUR_LONG_VALUE_HERE...',
        '__Host-bffC2': 'YOUR_LONG_VALUE_HERE...'
    }
    """)


# ========== USAGE EXAMPLES ==========

def example_cookie_based():
    """Example: Using cookie-based authentication (easiest for personal use)"""
    
    # 1. Extract cookies from your browser (see extract_cookies_from_browser())
    cookies = {
        '__Host-bff': 'chunks-2',
        '__Host-bffC1': 'YOUR_VALUE_HERE',  # Long encrypted value from browser
        '__Host-bffC2': 'YOUR_VALUE_HERE',  # Long encrypted value from browser
    }
    
    # 2. Create client and set cookies
    client = ArtsObservasjonerClient(use_api=False)
    client.set_cookies_from_browser(cookies)
    
    # 3. Submit observation
    observation = client.submit_observation_mobile(
        taxon_id=123456,  # Get this from species identification
        latitude=59.9139,  # Oslo coordinates
        longitude=10.7522,
        observed_datetime=datetime.now(),
        image_path="/path/to/mushroom_photo.jpg",
        count=3,
        comment="Found in moss near oak tree",
        accuracy_meters=5
    )
    
    print(f"Observation ID: {observation['Id']}")
    print(f"View at: https://mobil.artsobservasjoner.no/sighting/{observation['Id']}")


def example_oauth_based():
    """Example: Using OAuth authentication (recommended for apps)"""
    
    # 1. Register your app with Artsobservasjoner to get credentials
    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"
    
    # 2. Create client and authenticate
    client = ArtsObservasjonerClient(use_api=True, api_test=True)
    
    # First time: Get authorization URL
    # client.authenticate_oauth(CLIENT_ID, CLIENT_SECRET)
    # User visits URL, authorizes, you get code
    
    # Then: Exchange code for token
    # token_data = client.authenticate_oauth(
    #     CLIENT_ID, 
    #     CLIENT_SECRET, 
    #     authorization_code="CODE_FROM_REDIRECT"
    # )
    
    # Or: Use previously obtained token
    client.authenticate_oauth(
        CLIENT_ID,
        CLIENT_SECRET,
        access_token="YOUR_SAVED_TOKEN"
    )
    
    # 3. Submit observation
    observation = client.submit_observation_api(
        taxon_id=123456,
        latitude=59.9139,
        longitude=10.7522,
        observed_datetime=datetime.now(),
        image_path="/path/to/photo.jpg"
    )


if __name__ == "__main__":
    print("Artsobservasjoner Observation Submission Tool\n")
    print("Choose your approach:")
    print("1. Cookie-based (easier, for personal use)")
    print("2. OAuth-based (better, for apps)")
    print("\nSee example_cookie_based() and example_oauth_based() for usage")
    print("\nTo extract cookies, run: extract_cookies_from_browser()")
