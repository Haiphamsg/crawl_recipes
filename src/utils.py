from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Optional


RE_RECIPE_PATH = re.compile(r"^/vn/cong-thuc/(\d+)$")
RE_RECIPE_URL = re.compile(r"^https://cookpad\.com/vn/cong-thuc/(\d+)$")


def signature_of_ids(ids: list[int]) -> str:
    joined = ",".join(str(x) for x in ids)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def normalize_text_for_hash(text: str) -> str:
    return " ".join((text or "").strip().split()).lower()


def sha256_text(text: str) -> str:
    return hashlib.sha256(normalize_text_for_hash(text).encode("utf-8")).hexdigest()


def parse_datetime_maybe(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    v = value.strip()
    try:
        # Handles: 2025-01-01, 2025-01-01T10:20:30Z, 2025-01-01T10:20:30+07:00
        if v.endswith("Z"):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None

