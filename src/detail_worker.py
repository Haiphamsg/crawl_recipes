from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from typing import Any, Dict, Optional

import requests

from .config import load_settings
from .jsonld_recipe import ParsedRecipe, parse_jsonld_recipe
from .supabase_rest import SupabaseRest
from .utils import RE_RECIPE_URL


def _short_repr(value: Any, limit: int = 200) -> str:
    s = repr(value)
    if len(s) > limit:
        return s[: max(0, limit - 3)] + "..."
    return s


def _invalid_reason(
    code: str,
    *,
    job_id: Any,
    recipe_id: Any,
    requested_url: Any,
    parsed_recipe_id: Any = None,
) -> str:
    parts = [
        code,
        f"job_id={job_id}",
        f"recipe_id={recipe_id}",
        f"requested_url={_short_repr(requested_url)}",
    ]
    if parsed_recipe_id is not None:
        parts.append(f"parsed_recipe_id={parsed_recipe_id}")
    return "|".join(parts)


def _job_recipe_id_from_url(url: Optional[str]) -> Optional[int]:
    if not isinstance(url, str) or not url.strip():
        return None
    m = RE_RECIPE_URL.match(url.strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def write_staging(sb: SupabaseRest, job: Dict[str, Any], parsed: ParsedRecipe) -> None:
    recipe_id = parsed.recipe_id

    sb.upsert(
        "stg_recipes",
        [
            {
                "recipe_id": recipe_id,
                "source": job["source"],
                "locale": job["locale"],
                "url": parsed.url,
                "name": parsed.name,
                "description": parsed.description,
                "hero_image": parsed.hero_image,
                "date_published": parsed.date_published.isoformat() if parsed.date_published else None,
                "date_modified": parsed.date_modified.isoformat() if parsed.date_modified else None,
                "cuisine": parsed.cuisine,
                "author_name": parsed.author_name,
                "author_url": parsed.author_url,
                "keywords_raw": parsed.keywords_raw,
                "bookmark_count": parsed.bookmark_count,
                "like_count": parsed.like_count,
                "comment_count": parsed.comment_count,
                "job_id": job["id"],
                "keyword": job["keyword"],
                "page": job["page"],
            }
        ],
        on_conflict="recipe_id",
    )

    # Replace-by-recipe_id for relational child tables
    sb.delete_where("stg_recipe_keywords", f"recipe_id=eq.{recipe_id}")
    sb.delete_where("stg_recipe_ingredients", f"recipe_id=eq.{recipe_id}")
    sb.delete_where("stg_recipe_steps", f"recipe_id=eq.{recipe_id}")
    sb.delete_where("stg_recipe_comments", f"recipe_id=eq.{recipe_id}")

    sb.upsert(
        "stg_recipe_keywords",
        [{"recipe_id": recipe_id, "keyword": k} for k in parsed.keywords],
        on_conflict="recipe_id,keyword",
    )
    sb.upsert(
        "stg_recipe_ingredients",
        [
            {"recipe_id": recipe_id, "ingredient_index": i, "ingredient_text": txt}
            for i, txt in enumerate(parsed.ingredients)
        ],
        on_conflict="recipe_id,ingredient_index",
    )
    sb.upsert(
        "stg_recipe_steps",
        [
            {
                "recipe_id": recipe_id,
                "step_index": i,
                "step_text": s.get("text"),
                "step_image": s.get("image"),
            }
            for i, s in enumerate(parsed.steps)
        ],
        on_conflict="recipe_id,step_index",
    )
    sb.upsert(
        "stg_recipe_comments",
        [
            {
                "recipe_id": recipe_id,
                "text_hash": c["text_hash"],
                "author_name": c.get("author_name"),
                "author_url": c.get("author_url"),
                "comment_url": c.get("url"),
                "date_published": c["date_published"].isoformat() if c.get("date_published") else None,
                "comment_text": c["text"],
            }
            for c in parsed.comments
        ],
        on_conflict="recipe_id,text_hash",
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker-id", required=True)
    args = ap.parse_args()

    settings = load_settings()
    cutoff: date = settings.cutoff_date
    sb = SupabaseRest(settings.supabase_url, settings.supabase_service_role_key)

    session = requests.Session()
    session.headers.update({"User-Agent": settings.user_agent, "Accept": "text/html"})

    while True:
        job = sb.rpc("claim_next_crawl_job", {"p_worker_id": args.worker_id})
        if isinstance(job, list):
            job = job[0] if job else None
        if not job:
            print(f"[detail_worker][{args.worker_id}] idle: no job", flush=True)
            time.sleep(5)
            continue
        if isinstance(job, dict) and not job.get("id"):
            # Some PostgREST setups can serialize a NULL composite return as an object with NULL fields.
            # Treat that as "no job" to avoid crashing / marking invalid with a NULL job_id.
            print(
                f"[detail_worker] claim_next_crawl_job returned empty payload: {_short_repr(job, limit=500)}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(5)
            continue

        job_id = job["id"]
        requested_url = job.get("requested_url")
        print(
            f"[detail_worker][{args.worker_id}] claimed job_id={job_id} recipe_id={job.get('recipe_id')} "
            f"keyword={job.get('keyword')} page={job.get('page')}",
            flush=True,
        )
        if not isinstance(requested_url, str) or not requested_url.strip():
            reason = _invalid_reason(
                "missing_requested_url",
                job_id=job_id,
                recipe_id=job.get("recipe_id"),
                requested_url=requested_url,
            )
            print(f"[detail_worker] invalid job: {reason}", file=sys.stderr, flush=True)
            sb.rpc(
                "mark_crawl_job_invalid",
                {"p_job_id": job_id, "p_reason": reason, "p_http_status": None},
            )
            print(f"[detail_worker][{args.worker_id}] invalid: {reason}", flush=True)
            continue

        recipe_id = _job_recipe_id_from_url(requested_url)
        if recipe_id is None or int(job["recipe_id"]) != int(recipe_id):
            reason = _invalid_reason(
                "bad_requested_url",
                job_id=job_id,
                recipe_id=job.get("recipe_id"),
                requested_url=requested_url,
                parsed_recipe_id=recipe_id,
            )
            print(f"[detail_worker] invalid job: {reason}", file=sys.stderr, flush=True)
            sb.rpc(
                "mark_crawl_job_invalid",
                {"p_job_id": job_id, "p_reason": reason, "p_http_status": None},
            )
            print(f"[detail_worker][{args.worker_id}] invalid: {reason}", flush=True)
            continue

        try:
            resp = session.get(
                requested_url,
                timeout=20,
                allow_redirects=False,
            )
        except requests.RequestException as e:
            sb.rpc(
                "mark_crawl_job_failed",
                {"p_job_id": job_id, "p_error": f"request_error:{type(e).__name__}", "p_http_status": None},
            )
            print(
                f"[detail_worker][{args.worker_id}] failed: request_error:{type(e).__name__} job_id={job_id}",
                flush=True,
            )
            continue

        if resp.status_code in (301, 302):
            sb.rpc(
                "mark_crawl_job_invalid",
                {"p_job_id": job_id, "p_reason": "redirect", "p_http_status": resp.status_code},
            )
            print(
                f"[detail_worker][{args.worker_id}] invalid: redirect job_id={job_id} http={resp.status_code}",
                flush=True,
            )
            continue
        if resp.url != requested_url:
            sb.rpc(
                "mark_crawl_job_invalid",
                {"p_job_id": job_id, "p_reason": "url_mismatch", "p_http_status": resp.status_code},
            )
            print(
                f"[detail_worker][{args.worker_id}] invalid: url_mismatch job_id={job_id} http={resp.status_code}",
                flush=True,
            )
            continue
        if resp.status_code in (404, 410):
            sb.rpc(
                "mark_crawl_job_invalid",
                {"p_job_id": job_id, "p_reason": "notfound", "p_http_status": resp.status_code},
            )
            print(
                f"[detail_worker][{args.worker_id}] invalid: notfound job_id={job_id} http={resp.status_code}",
                flush=True,
            )
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            sb.rpc(
                "mark_crawl_job_failed",
                {"p_job_id": job_id, "p_error": f"http_{resp.status_code}", "p_http_status": resp.status_code},
            )
            print(
                f"[detail_worker][{args.worker_id}] failed: http_{resp.status_code} job_id={job_id}",
                flush=True,
            )
            time.sleep(1)
            continue
        if resp.status_code != 200:
            sb.rpc(
                "mark_crawl_job_failed",
                {"p_job_id": job_id, "p_error": f"http_{resp.status_code}", "p_http_status": resp.status_code},
            )
            print(
                f"[detail_worker][{args.worker_id}] failed: http_{resp.status_code} job_id={job_id}",
                flush=True,
            )
            continue

        parsed = parse_jsonld_recipe(resp.text, requested_url=requested_url, recipe_id=recipe_id)
        if not parsed:
            sb.rpc(
                "mark_crawl_job_invalid",
                {"p_job_id": job_id, "p_reason": "no_recipe_jsonld", "p_http_status": 200},
            )
            print(
                f"[detail_worker][{args.worker_id}] invalid: no_recipe_jsonld job_id={job_id}",
                flush=True,
            )
            continue

        try:
            write_staging(sb, job, parsed)
        except Exception as e:
            sb.rpc(
                "mark_crawl_job_failed",
                {"p_job_id": job_id, "p_error": f"staging_write:{type(e).__name__}", "p_http_status": 200},
            )
            print(
                f"[detail_worker][{args.worker_id}] failed: staging_write:{type(e).__name__} job_id={job_id}",
                flush=True,
            )
            continue

        if parsed.date_published and parsed.date_published.date() < cutoff:
            sb.rpc(
                "update_keyword_feedback",
                {
                    "p_keyword": job["keyword"],
                    "p_page": int(job["page"]),
                    "p_date_published": parsed.date_published.date().isoformat(),
                },
            )
        else:
            sb.rpc(
                "promote_recipe_if_recent",
                {"p_recipe_id": recipe_id, "p_cutoff_date": cutoff.isoformat()},
            )

        sb.rpc("mark_crawl_job_done", {"p_job_id": job_id})
        print(
            f"[detail_worker][{args.worker_id}] done job_id={job_id} recipe_id={recipe_id}",
            flush=True,
        )


if __name__ == "__main__":
    main()
