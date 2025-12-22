from __future__ import annotations

from datetime import date

from .config import load_settings
from .supabase_rest import SupabaseRest


def main() -> None:
    settings = load_settings()
    sb = SupabaseRest(settings.supabase_url, settings.supabase_service_role_key)
    cutoff: date = settings.cutoff_date

    sb.rpc("promote_recent_recipes", {"p_cutoff_date": cutoff.isoformat(), "p_limit": 2000})
    # Best-effort prune to keep product within cutoff.
    sb.rpc("prune_product_older_than", {"p_cutoff_date": cutoff.isoformat()})


if __name__ == "__main__":
    main()
