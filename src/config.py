from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta


TIER_1_SEEDS = ["a", "e", "i", "o", "u", "n", "m", "t", "c", "b"]
TIER_2_SEEDS = ["h", "g", "r", "s", "l", "p", "d", "k", "v", "y", "1", "2", "3"]


@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_service_role_key: str
    source: str = "cookpad"
    locale: str = "vn"
    cutoff_days: int = 30
    max_pages_per_keyword: int = 30

    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    @property
    def cutoff_date(self) -> date:
        return date.today() - timedelta(days=self.cutoff_days)


def load_settings() -> Settings:
    supabase_url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    supabase_service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not supabase_url or not supabase_service_role_key:
        raise RuntimeError("Missing env: SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY")

    source = os.environ.get("SOURCE", "cookpad").strip()
    locale = os.environ.get("LOCALE", "vn").strip()
    cutoff_days = int(os.environ.get("CUTOFF_DAYS", "30"))
    max_pages = int(os.environ.get("MAX_PAGES_PER_KEYWORD", "30"))
    return Settings(
        supabase_url=supabase_url,
        supabase_service_role_key=supabase_service_role_key,
        source=source,
        locale=locale,
        cutoff_days=cutoff_days,
        max_pages_per_keyword=max_pages,
    )

