"""Database terms with canonical names and Qt translations."""

import re

from PySide6.QtCore import QCoreApplication, QT_TRANSLATE_NOOP


class DatabaseTerms:
    """Translatable database terms for microscopy observations."""

    TERM_ALIASES = {
        "Trypan_Blue": ("Tryptan Blue", "Tryptan_Blue"),
    }
    
    @staticmethod
    def tr(text: str) -> str:
        """Translate text in DatabaseTerms context."""
        return QCoreApplication.translate("DatabaseTerms", text)
    
    # Canonical English names (stored in database)
    CONTRAST_METHODS = ["Not_set", "BF", "DF", "DIC", "Oblique", "Phase", "HMC"]
    MOUNT_MEDIA = [
        "Not_set",
        "Water",
        "KOH",
        "NH3",
        "Glycerine",
        "L4",
    ]
    STAIN_TYPES = [
        "Not_set",
        "Melzer",
        "Congo_Red",
        "Cotton_Blue",
        "Lactofuchsin",
        "Cresyl_Blue",
        "Trypan_Blue",
        "Chlorazol_Black_E",
    ]
    SAMPLE_TYPES = ["Not_set", "Fresh", "Dried", "Spore_print"]
    MEASURE_CATEGORIES = [
        "Spores", "Field", "Basidia", "Pileipellis",
        "Pleurocystidia", "Cheilocystidia", "Caulocystidia", "Other", "Calibration"
    ]
    
    # Display name mappings (for translation)
    CONTRAST_DISPLAY = {
        "Not_set": QT_TRANSLATE_NOOP("DatabaseTerms", "Not set"),
        "BF": QT_TRANSLATE_NOOP("DatabaseTerms", "BF"),
        "DF": QT_TRANSLATE_NOOP("DatabaseTerms", "DF"),
        "DIC": QT_TRANSLATE_NOOP("DatabaseTerms", "DIC"),
        "Oblique": QT_TRANSLATE_NOOP("DatabaseTerms", "Oblique"),
        "Phase": QT_TRANSLATE_NOOP("DatabaseTerms", "Phase"),
        "HMC": QT_TRANSLATE_NOOP("DatabaseTerms", "HMC"),
    }
    
    MOUNT_DISPLAY = {
        "Not_set": QT_TRANSLATE_NOOP("DatabaseTerms", "Not set"),
        "Water": QT_TRANSLATE_NOOP("DatabaseTerms", "Water"),
        "KOH": QT_TRANSLATE_NOOP("DatabaseTerms", "KOH"),
        "NH3": QT_TRANSLATE_NOOP("DatabaseTerms", "NH₃"),
        "Glycerine": QT_TRANSLATE_NOOP("DatabaseTerms", "Glycerine"),
        "L4": QT_TRANSLATE_NOOP("DatabaseTerms", "L4"),
    }

    STAIN_DISPLAY = {
        "Not_set": QT_TRANSLATE_NOOP("DatabaseTerms", "Not set"),
        "Melzer": QT_TRANSLATE_NOOP("DatabaseTerms", "Melzer"),
        "Congo_Red": QT_TRANSLATE_NOOP("DatabaseTerms", "Congo Red"),
        "Cotton_Blue": QT_TRANSLATE_NOOP("DatabaseTerms", "Cotton Blue"),
        "Lactofuchsin": QT_TRANSLATE_NOOP("DatabaseTerms", "Lactofuchsin"),
        "Cresyl_Blue": QT_TRANSLATE_NOOP("DatabaseTerms", "Cresyl Blue"),
        "Trypan_Blue": QT_TRANSLATE_NOOP("DatabaseTerms", "Trypan Blue"),
        "Chlorazol_Black_E": QT_TRANSLATE_NOOP("DatabaseTerms", "Chlorazol Black E"),
    }
    
    SAMPLE_DISPLAY = {
        "Not_set": QT_TRANSLATE_NOOP("DatabaseTerms", "Not set"),
        "Fresh": QT_TRANSLATE_NOOP("DatabaseTerms", "Fresh"),
        "Dried": QT_TRANSLATE_NOOP("DatabaseTerms", "Dried"),
        "Spore_print": QT_TRANSLATE_NOOP("DatabaseTerms", "Spore print"),
    }
    
    MEASURE_DISPLAY = {
        "Spores": QT_TRANSLATE_NOOP("DatabaseTerms", "Spores"),
        "Field": QT_TRANSLATE_NOOP("DatabaseTerms", "Field"),
        "Basidia": QT_TRANSLATE_NOOP("DatabaseTerms", "Basidia"),
        "Pileipellis": QT_TRANSLATE_NOOP("DatabaseTerms", "Pileipellis"),
        "Pleurocystidia": QT_TRANSLATE_NOOP("DatabaseTerms", "Pleurocystidia"),
        "Cheilocystidia": QT_TRANSLATE_NOOP("DatabaseTerms", "Cheilocystidia"),
        "Caulocystidia": QT_TRANSLATE_NOOP("DatabaseTerms", "Caulocystidia"),
        "Other": QT_TRANSLATE_NOOP("DatabaseTerms", "Other"),
        "Calibration": QT_TRANSLATE_NOOP("DatabaseTerms", "Calibration"),
    }
    
    @staticmethod
    def _normalize_token(value: str | None) -> str:
        if value is None:
            return ""
        text = str(value).strip().lower()
        if not text:
            return ""
        text = text.translate(str.maketrans("₀₁₂₃₄₅₆₇₈₉", "0123456789"))
        text = text.replace("&", "and")
        text = re.sub(r"[^\w\s-]", "", text)
        text = re.sub(r"[\s_-]+", "", text)
        return text

    @classmethod
    def _build_lookup(cls, display_map: dict[str, str]) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for canonical, display in display_map.items():
            candidates = {
                canonical,
                display,
                canonical.replace("_", " "),
                canonical.replace("_", "-"),
                display.replace(" ", "_"),
            }
            candidates.update(cls.TERM_ALIASES.get(canonical, ()))
            for candidate in candidates:
                norm = cls._normalize_token(candidate)
                if norm:
                    lookup[norm] = canonical
        return lookup

    @staticmethod
    def _fallback_display(value: str | None) -> str:
        if value is None:
            return ""
        return str(value).replace("_", " ").strip()

    @staticmethod
    def _fallback_canonical(value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return re.sub(r"\s+", "_", text)

    @classmethod
    def canonicalize_contrast(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lookup = cls._build_lookup(cls.CONTRAST_DISPLAY)
        canonical = lookup.get(cls._normalize_token(value))
        return canonical or cls._fallback_canonical(value)

    @classmethod
    def canonicalize_mount(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lookup = cls._build_lookup(cls.MOUNT_DISPLAY)
        canonical = lookup.get(cls._normalize_token(value))
        return canonical or cls._fallback_canonical(value)

    @classmethod
    def canonicalize_sample(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lookup = cls._build_lookup(cls.SAMPLE_DISPLAY)
        canonical = lookup.get(cls._normalize_token(value))
        return canonical or cls._fallback_canonical(value)

    @classmethod
    def canonicalize_stain(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lookup = cls._build_lookup(cls.STAIN_DISPLAY)
        canonical = lookup.get(cls._normalize_token(value))
        return canonical or cls._fallback_canonical(value)

    @classmethod
    def canonicalize_measure(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lookup = cls._build_lookup(cls.MEASURE_DISPLAY)
        canonical = lookup.get(cls._normalize_token(value))
        return canonical or cls._fallback_canonical(value)

    @classmethod
    def canonicalize(cls, category: str, value: str | None) -> str | None:
        if category == "contrast":
            return cls.canonicalize_contrast(value)
        if category == "mount":
            return cls.canonicalize_mount(value)
        if category == "stain":
            return cls.canonicalize_stain(value)
        if category == "sample":
            return cls.canonicalize_sample(value)
        if category == "measure":
            return cls.canonicalize_measure(value)
        return cls._fallback_canonical(value)

    @classmethod
    def custom_to_canonical(cls, value: str | None) -> str | None:
        return cls._fallback_canonical(value)

    @classmethod
    def translate_contrast(cls, canonical_name: str | None) -> str:
        """Get translated display name for contrast method."""
        canonical = cls.canonicalize_contrast(canonical_name)
        display = cls.CONTRAST_DISPLAY.get(canonical or "", cls._fallback_display(canonical_name))
        return cls.tr(display)
    
    @classmethod
    def translate_mount(cls, canonical_name: str | None) -> str:
        """Get translated display name for mount medium."""
        canonical = cls.canonicalize_mount(canonical_name)
        display = cls.MOUNT_DISPLAY.get(canonical or "", cls._fallback_display(canonical_name))
        return cls.tr(display)

    @classmethod
    def translate_stain(cls, canonical_name: str | None) -> str:
        """Get translated display name for stain type."""
        canonical = cls.canonicalize_stain(canonical_name)
        display = cls.STAIN_DISPLAY.get(canonical or "", cls._fallback_display(canonical_name))
        return cls.tr(display)
    
    @classmethod
    def translate_sample(cls, canonical_name: str | None) -> str:
        """Get translated display name for sample type."""
        canonical = cls.canonicalize_sample(canonical_name)
        display = cls.SAMPLE_DISPLAY.get(canonical or "", cls._fallback_display(canonical_name))
        return cls.tr(display)
    
    @classmethod
    def translate_measure(cls, canonical_name: str | None) -> str:
        """Get translated display name for measure category."""
        canonical = cls.canonicalize_measure(canonical_name)
        display = cls.MEASURE_DISPLAY.get(canonical or "", cls._fallback_display(canonical_name))
        return cls.tr(display)

    @classmethod
    def translate(cls, category: str, canonical_name: str | None) -> str:
        if category == "contrast":
            return cls.translate_contrast(canonical_name)
        if category == "mount":
            return cls.translate_mount(canonical_name)
        if category == "stain":
            return cls.translate_stain(canonical_name)
        if category == "sample":
            return cls.translate_sample(canonical_name)
        if category == "measure":
            return cls.translate_measure(canonical_name)
        return cls.tr(cls._fallback_display(canonical_name))

    @classmethod
    def default_values(cls, category: str) -> list[str]:
        if category == "contrast":
            return list(cls.CONTRAST_METHODS)
        if category == "mount":
            return list(cls.MOUNT_MEDIA)
        if category == "stain":
            return list(cls.STAIN_TYPES)
        if category == "sample":
            return list(cls.SAMPLE_TYPES)
        if category == "measure":
            return list(cls.MEASURE_CATEGORIES)
        return []

    @classmethod
    def setting_key(cls, category: str) -> str:
        mapping = {
            "contrast": "contrast_options",
            "mount": "mount_options",
            "stain": "stain_options",
            "sample": "sample_options",
            "measure": "measure_categories",
        }
        return mapping.get(category, "")

    @classmethod
    def last_used_key(cls, category: str) -> str:
        mapping = {
            "contrast": "last_used_contrast",
            "mount": "last_used_mount",
            "stain": "last_used_stain",
            "sample": "last_used_sample",
            "measure": "last_used_measure",
        }
        return mapping.get(category, "")

    @classmethod
    def canonicalize_list(cls, category: str, values: list[str] | None) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for value in values or []:
            canonical = cls.canonicalize(category, value)
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            cleaned.append(canonical)
        defaults = cls.default_values(category)
        if not cleaned:
            return defaults
        return cleaned
