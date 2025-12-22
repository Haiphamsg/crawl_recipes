from __future__ import annotations

import argparse
import asyncio
import re
import time
from typing import Iterable, List, Optional, Tuple

import httpx
from selectolax.parser import HTMLParser

from .config import TIER_1_SEEDS, TIER_2_SEEDS, load_settings
from .supabase_rest import SupabaseRest
from .utils import signature_of_ids


SEARCH_URL_TEMPLATE = "https://cookpad.com/vn/tim-kiem/{keyword}?page={page}"
_RE_RECIPE_REL = re.compile(r"^/vn/cong-thuc/(\d+)(?:[/?#].*)?$")
_RE_RECIPE_ABS = re.compile(r"^https://cookpad\.com/vn/cong-thuc/(\d+)(?:[/?#].*)?$")


def extract_recipe_ids_from_listing(html: str) -> List[int]:
    ids: List[int] = []
    tree = HTMLParser(html)
    for a in tree.css("a"):
        href = a.attributes.get("href")
        if not href:
            continue
        href = href.strip()
        m = _RE_RECIPE_REL.match(href)
        if not m:
            m = _RE_RECIPE_ABS.match(href)
        if not m:
            continue
        try:
            ids.append(int(m.group(1)))
        except ValueError:
            continue
    # Keep page-order but de-dupe within page
    seen = set()
    out: List[int] = []
    for rid in ids:
        if rid not in seen:
            out.append(rid)
            seen.add(rid)
    return out


def iter_seed_tiers() -> Iterable[Tuple[int, List[str]]]:
    yield 1, TIER_1_SEEDS
    yield 2, TIER_2_SEEDS


def _fetch_listing_sync(client: httpx.Client, url: str) -> Optional[str]:
    backoffs_s = [1.0, 3.0, 7.0]
    for attempt in range(1, 4):
        try:
            resp = client.get(url, follow_redirects=False)
        except httpx.RequestError:
            resp = None

        if resp is not None and resp.status_code == 200:
            return resp.text

        status = resp.status_code if resp is not None else None
        if status in (429,) or (status is not None and status >= 500) or status is None:
            if attempt < 3:
                time.sleep(backoffs_s[attempt - 1])
                continue
        return None
    return None


async def _fetch_listing_async(client: httpx.AsyncClient, url: str) -> Optional[str]:
    backoffs_s = [1.0, 3.0, 7.0]
    for attempt in range(1, 4):
        try:
            resp = await client.get(url, follow_redirects=False)
        except httpx.RequestError:
            resp = None

        if resp is not None and resp.status_code == 200:
            return resp.text

        status = resp.status_code if resp is not None else None
        if status in (429,) or (status is not None and status >= 500) or status is None:
            if attempt < 3:
                await asyncio.sleep(backoffs_s[attempt - 1])
                continue
        return None
    return None


async def _harvest_keyword_async(
    sb: SupabaseRest,
    client: httpx.AsyncClient,
    *,
    source: str,
    locale: str,
    keyword: str,
    tier: int,
    max_pages: int,
    batch_size: int,
    verbose: bool,
) -> None:
    consecutive_zero_new = 0
    prev_signature = None
    total_found = 0
    total_inserted = 0
    pages_ok = 0

    page = 1
    while page <= max_pages:
        batch_pages = list(range(page, min(page + batch_size, max_pages + 1)))
        urls = [SEARCH_URL_TEMPLATE.format(keyword=keyword, page=p) for p in batch_pages]
        htmls = await asyncio.gather(*[_fetch_listing_async(client, u) for u in urls])

        for p, html in zip(batch_pages, htmls):
            if not html:
                if verbose:
                    print(f"[harvest][tier={tier}][kw={keyword}] stop: fetch_failed page={p}")
                return

            recipe_ids = extract_recipe_ids_from_listing(html)
            if not recipe_ids:  # S1
                if verbose:
                    print(f"[harvest][tier={tier}][kw={keyword}] stop: empty_page page={p}")
                return

            sig = signature_of_ids(recipe_ids)
            if prev_signature is not None and sig == prev_signature:  # S3
                if verbose:
                    print(f"[harvest][tier={tier}][kw={keyword}] stop: loop_signature page={p}")
                return
            prev_signature = sig

            result = sb.rpc(
                "enqueue_crawl_jobs",
                {
                    "p_source": source,
                    "p_locale": locale,
                    "p_keyword": keyword,
                    "p_tier": tier,
                    "p_page": p,
                    "p_recipe_ids": recipe_ids,
                },
            )
            inserted = int(result[0]["inserted_count"]) if result else 0
            skipped = int(result[0]["skipped_count"]) if result else 0
            pages_ok += 1
            total_found += len(recipe_ids)
            total_inserted += inserted

            if verbose:
                print(
                    f"[harvest][tier={tier}][kw={keyword}] page={p} found={len(recipe_ids)} "
                    f"inserted={inserted} skipped={skipped}"
                )

            if inserted == 0:
                consecutive_zero_new += 1
            else:
                consecutive_zero_new = 0

            if consecutive_zero_new >= 5:  # S2
                if verbose:
                    print(
                        f"[harvest][tier={tier}][kw={keyword}] stop: no_new_jobs_5_pages page={p} "
                        f"summary pages_ok={pages_ok} found={total_found} inserted={total_inserted}"
                    )
                return

            await asyncio.sleep(0.2)

        page += batch_size
    if verbose:
        print(
            f"[harvest][tier={tier}][kw={keyword}] done: reached_max_pages={max_pages} "
            f"summary pages_ok={pages_ok} found={total_found} inserted={total_inserted}"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--async", dest="use_async", action="store_true")
    ap.add_argument("--keyword-concurrency", type=int, default=5)
    ap.add_argument("--batch-size", type=int, default=3)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    settings = load_settings()
    sb = SupabaseRest(settings.supabase_url, settings.supabase_service_role_key)

    headers = {"User-Agent": settings.user_agent, "Accept": "text/html"}
    total_keywords = 0
    total_inserted = 0
    total_found = 0

    if args.use_async:
        async def _run() -> None:
            limits = httpx.Limits(max_connections=max(args.keyword_concurrency * 2, 10))
            timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=10.0)
            sem = asyncio.Semaphore(max(1, args.keyword_concurrency))

            async with httpx.AsyncClient(
                http2=True, headers=headers, timeout=timeout, limits=limits
            ) as client:
                tasks = []
                for tier, seeds in iter_seed_tiers():
                    for keyword in seeds:
                        total_keywords += 1
                        feedback = sb.select_one(
                            "keyword_feedback", f"keyword=eq.{keyword}&select=is_stale,stale_page"
                        )
                        is_stale = bool(feedback and feedback.get("is_stale"))
                        max_pages = 2 if is_stale else settings.max_pages_per_keyword

                        async def _bounded(keyword: str = keyword, tier: int = tier, max_pages: int = max_pages) -> None:
                            async with sem:
                                await _harvest_keyword_async(
                                    sb,
                                    client,
                                    source=settings.source,
                                    locale=settings.locale,
                                    keyword=keyword,
                                    tier=tier,
                                    max_pages=max_pages,
                                    batch_size=max(1, args.batch_size),
                                    verbose=args.verbose,
                                )

                        tasks.append(asyncio.create_task(_bounded()))

                await asyncio.gather(*tasks)

        asyncio.run(_run())
        print(f"[harvest] complete (async) keywords={total_keywords}")
        return

    timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=10.0)
    with httpx.Client(http2=True, timeout=timeout, headers=headers) as client:
        for tier, seeds in iter_seed_tiers():
            for keyword in seeds:
                total_keywords += 1
                feedback = sb.select_one(
                    "keyword_feedback", f"keyword=eq.{keyword}&select=is_stale,stale_page"
                )
                is_stale = bool(feedback and feedback.get("is_stale"))
                max_pages = 2 if is_stale else settings.max_pages_per_keyword

                consecutive_zero_new = 0
                prev_signature = None
                kw_found = 0
                kw_inserted = 0
                stop_reason = "max_pages"

                for page in range(1, max_pages + 1):
                    url = SEARCH_URL_TEMPLATE.format(keyword=keyword, page=page)
                    html = _fetch_listing_sync(client, url)
                    if not html:
                        stop_reason = f"fetch_failed page={page}"
                        break

                    recipe_ids = extract_recipe_ids_from_listing(html)
                    found_count = len(recipe_ids)
                    if found_count == 0:  # S1
                        stop_reason = f"empty_page page={page}"
                        break

                    sig = signature_of_ids(recipe_ids)
                    if prev_signature is not None and sig == prev_signature:  # S3
                        stop_reason = f"loop_signature page={page}"
                        break
                    prev_signature = sig

                    result = sb.rpc(
                        "enqueue_crawl_jobs",
                        {
                            "p_source": settings.source,
                            "p_locale": settings.locale,
                            "p_keyword": keyword,
                            "p_tier": tier,
                            "p_page": page,
                            "p_recipe_ids": recipe_ids,
                        },
                    )
                    inserted = int(result[0]["inserted_count"]) if result else 0
                    skipped = int(result[0]["skipped_count"]) if result else 0
                    kw_found += found_count
                    kw_inserted += inserted

                    if args.verbose:
                        print(
                            f"[harvest][tier={tier}][kw={keyword}] page={page} found={found_count} "
                            f"inserted={inserted} skipped={skipped}"
                        )

                    if inserted == 0:
                        consecutive_zero_new += 1
                    else:
                        consecutive_zero_new = 0

                    if consecutive_zero_new >= 5:  # S2
                        stop_reason = f"no_new_jobs_5_pages page={page}"
                        break

                    time.sleep(0.2)  # politeness delay

                total_found += kw_found
                total_inserted += kw_inserted
                print(
                    f"[harvest][tier={tier}][kw={keyword}] done: {stop_reason} "
                    f"found={kw_found} inserted={kw_inserted}"
                )

    print(
        f"[harvest] complete (sync) keywords={total_keywords} found={total_found} inserted={total_inserted}"
    )


if __name__ == "__main__":
    main()
