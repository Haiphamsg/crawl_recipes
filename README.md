# Cookpad VN → Supabase (30-day recent recipes)

End-to-end pipeline to harvest newest Cookpad Vietnam recipes (last 30 days), queue via Supabase Postgres, crawl details with 3 parallel workers, store relational staging (no JSONB), then promote recent recipes into product tables.

## Architecture (required flow)

```
HARVEST (listing search pages)
  -> extract recipe_id URLs
  -> enqueue (dedupe)
    ↓
QUEUE (Supabase: crawl_jobs)
    ↓ (3 workers in parallel, FOR UPDATE SKIP LOCKED)
DETAIL CRAWL (recipe pages)
  -> parse only JSON-LD Recipe
  -> write STAGING tables (relational)
    ↓
TRANSFORM (promote only recipes within cutoff)
    ↓
PRODUCT tables (relational)
```

## Keyword seeds (optimized for “newest”)

- Tier 1 (run first): `a,e,i,o,u,n,m,t,c,b`
- Tier 2: `h,g,r,s,l,p,d,k,v,y,1,2,3`

Priority formula: `priority = tier_weight * 1000 + page` (Tier1=1, Tier2=2; page starts at 1). Lower `priority` crawls first.

## What is (not) stored

- No `raw_html` is stored anywhere.
- Detail parsing reads only `<script type="application/ld+json">` and keeps the object where `"@type" == "Recipe"`.
- Staging/product tables are relational (no JSONB columns).

## Setup

1) Apply schema to your Supabase database:

- Run `sql/001_init.sql` in Supabase SQL editor.

Included objects:

- Queue: `crawl_jobs`
- Feedback: `keyword_feedback`
- Staging: `stg_recipes`, `stg_recipe_keywords`, `stg_recipe_ingredients`, `stg_recipe_steps`, `stg_recipe_comments`
- Product: `recipes`, `recipe_keywords`, `recipe_ingredients`, `recipe_steps`
- RPCs: `enqueue_crawl_jobs`, `claim_next_crawl_job`, `mark_crawl_job_done`, `mark_crawl_job_invalid`, `mark_crawl_job_failed`, `requeue_crawl_job`, `update_keyword_feedback`, `promote_recipe_if_recent`, `promote_recent_recipes`, `reset_stuck_processing_jobs`, `revive_dead_jobs`, `prune_product_older_than`

2) Configure env vars:

- `SUPABASE_URL` (e.g. `https://xyzcompany.supabase.co`)
- `SUPABASE_SERVICE_ROLE_KEY`
- Optional: `SOURCE=cookpad`, `LOCALE=vn`

3) Python deps:

- `pip install -r requirements.txt`

## Run

Harvest listings (push jobs into queue):

- `python -m src.harvest`
- Async (HTTP/2 + gather): `python -m src.harvest --async`

Run 3 detail workers (in 3 terminals):

- `python -m src.detail_worker --worker-id w1`
- `python -m src.detail_worker --worker-id w2`
- `python -m src.detail_worker --worker-id w3`

Optional: batch promote recent recipes (if you prefer scheduled transforms):

- `python -m src.promote_recent`

## Pseudocode (required)

### Harvest worker (listing)

```
for tier in [1,2]:
  for keyword in seeds[tier]:
    max_pages = feedback.is_stale(keyword) ? 2 : 30
    consecutive_zero_new = 0
    prev_signature = null

    for page in 1..max_pages:
      html = fetch("/vn/tim-kiem/<keyword>?page=<page>")
      ids = extract_links(regex "^/vn/cong-thuc/\\d+")
      if ids.count == 0: break  (S1)

      signature = hash(join(",", ids))
      if signature == prev_signature: break (S3)
      prev_signature = signature

      inserted = rpc_enqueue_crawl_jobs(keyword,tier,page,ids)
      if inserted == 0: consecutive_zero_new += 1 else consecutive_zero_new = 0
      if consecutive_zero_new >= 2: break (S2)
```

### Detail worker (3 parallel)

```
loop forever:
  job = rpc_claim_next_crawl_job(worker_id)  -- SKIP LOCKED
  if job is null: sleep(5); continue

  resp = GET job.requested_url (no redirects, timeout, retry)
  if 301/302: rpc_mark_job_invalid("redirect")
  elif 404/410: rpc_mark_job_invalid("notfound")
  elif 429/5xx/timeout: rpc_mark_job_failed_with_backoff(max_attempts=3)
  elif 200:
    recipe = parse_jsonld_recipe(resp.html)
    if recipe missing: rpc_mark_job_invalid("no_recipe_jsonld")
    else:
      write staging (upsert stg_recipes; replace child rows)
      if recipe.datePublished < cutoff(30d):
        rpc_update_keyword_feedback(keyword, page, recipe.datePublished)
      else:
        rpc_promote_recipe(recipe_id, cutoff_date)
      rpc_mark_job_done()
```

## JSON-LD → DB mapping (required)

Detail parser selects only JSON-LD object with `"@type": "Recipe"` (skip `WebSite`, etc).

- `Recipe.name` → `stg_recipes.name`
- `Recipe.image` (string|array|object) → `stg_recipes.hero_image` (first URL)
- `Recipe.url` → `stg_recipes.url` (fallback: requested_url)
- `Recipe.description` → `stg_recipes.description`
- `Recipe.datePublished` → `stg_recipes.date_published`
- `Recipe.dateModified` → `stg_recipes.date_modified`
- `Recipe.recipeCuisine` (string|array) → `stg_recipes.cuisine`
- `Recipe.author` (object|array): `name`, `url` → `stg_recipes.author_name`, `stg_recipes.author_url`
- `Recipe.recipeIngredient[]` → `stg_recipe_ingredients(ingredient_index, ingredient_text)`
- `Recipe.recipeInstructions[]`:
  - string → `stg_recipe_steps(step_index, step_text)`
  - `HowToStep.text`, `HowToStep.image` → `stg_recipe_steps(step_text, step_image)`
- `Recipe.keywords` (string|array) → split → `stg_recipe_keywords(keyword)`
- `Recipe.interactionStatistic[]`:
  - `interactionType` contains `BookmarkAction` → `stg_recipes.bookmark_count`
  - contains `LikeAction` → `stg_recipes.like_count`
  - contains `CommentAction` → `stg_recipes.comment_count`
- `Recipe.comment[]` (if present) → `stg_recipe_comments` with `text_hash` dedupe

## Operational checklist (required)

- Metrics (per run): pages fetched, recipe links found, jobs inserted, jobs claimed, success/invalid/dead counts, retry counts, p95 fetch time.
- Logs: per job (`job_id`, `recipe_id`, `keyword`, `page`, `status`, `http_status`, `error`, `attempts`).
- Alerts: queue backlog (`queued` older than X mins), spike in `invalid` or `dead`, repeated 429.
- Rate limiting: cap concurrent workers to 3; add jitter sleep on 429.
- Idempotency: queue unique `(source, locale, recipe_id)`; staging/product upsert by PK; child rows replace-by-recipe_id.
- Backoff: attempts 1→+60s, 2→+180s, 3→dead.
- Daily run: harvest Tier1 then Tier2; run workers continuously or on schedule; optional `rpc_prune_product(cutoff_date)` to keep only last 30d in product.






beautifulsoup4>=4.12.0
lxml>=5.0.0