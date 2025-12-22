-- Cookpad VN → Supabase schema (queue + staging + product + feedback + RPCs)
-- Apply in Supabase SQL editor.

create extension if not exists pgcrypto;

-- ===============
-- Utilities
-- ===============

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- ===============
-- Queue (crawl_jobs)
-- ===============

create table if not exists public.crawl_jobs (
  id uuid primary key default gen_random_uuid(),
  source text not null default 'cookpad',
  locale text not null default 'vn',
  recipe_id bigint not null,
  requested_url text not null,
  keyword text not null,
  tier smallint not null check (tier in (1, 2)),
  page int not null check (page > 0),
  priority int not null check (priority > 0),
  status text not null default 'queued' check (status in ('queued', 'processing', 'done', 'invalid', 'dead')),
  attempts int not null default 0 check (attempts >= 0),
  max_attempts int not null default 3 check (max_attempts > 0),
  next_attempt_at timestamptz not null default now(),
  claimed_by text,
  claimed_at timestamptz,
  invalid_reason text,
  http_status int,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint crawl_jobs_dedupe unique (source, locale, recipe_id)
);

create index if not exists crawl_jobs_claim_idx
  on public.crawl_jobs (status, next_attempt_at, priority, created_at);

create index if not exists crawl_jobs_recipe_id_idx
  on public.crawl_jobs (recipe_id);

drop trigger if exists crawl_jobs_set_updated_at on public.crawl_jobs;
create trigger crawl_jobs_set_updated_at
before update on public.crawl_jobs
for each row execute function public.set_updated_at();

-- ===============
-- Feedback (stop harvest early)
-- ===============

create table if not exists public.keyword_feedback (
  keyword text primary key,
  is_stale boolean not null default false,
  stale_page int,
  oldest_published_seen date,
  updated_at timestamptz not null default now()
);

drop trigger if exists keyword_feedback_set_updated_at on public.keyword_feedback;
create trigger keyword_feedback_set_updated_at
before update on public.keyword_feedback
for each row execute function public.set_updated_at();

-- ===============
-- Staging (relational, no JSONB)
-- ===============

create table if not exists public.stg_recipes (
  recipe_id bigint primary key,
  source text not null default 'cookpad',
  locale text not null default 'vn',
  url text not null,
  name text,
  description text,
  hero_image text,
  date_published timestamptz,
  date_modified timestamptz,
  cuisine text,
  author_name text,
  author_url text,
  keywords_raw text,
  bookmark_count int,
  like_count int,
  comment_count int,
  job_id uuid references public.crawl_jobs(id) on delete set null,
  keyword text,
  page int,
  harvested_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists stg_recipes_set_updated_at on public.stg_recipes;
create trigger stg_recipes_set_updated_at
before update on public.stg_recipes
for each row execute function public.set_updated_at();

create table if not exists public.stg_recipe_keywords (
  recipe_id bigint not null references public.stg_recipes(recipe_id) on delete cascade,
  keyword text not null,
  created_at timestamptz not null default now(),
  primary key (recipe_id, keyword)
);

create table if not exists public.stg_recipe_ingredients (
  recipe_id bigint not null references public.stg_recipes(recipe_id) on delete cascade,
  ingredient_index int not null check (ingredient_index >= 0),
  ingredient_text text not null,
  created_at timestamptz not null default now(),
  primary key (recipe_id, ingredient_index)
);

create table if not exists public.stg_recipe_steps (
  recipe_id bigint not null references public.stg_recipes(recipe_id) on delete cascade,
  step_index int not null check (step_index >= 0),
  step_text text,
  step_image text,
  created_at timestamptz not null default now(),
  primary key (recipe_id, step_index)
);

create table if not exists public.stg_recipe_comments (
  recipe_id bigint not null references public.stg_recipes(recipe_id) on delete cascade,
  text_hash text not null,
  author_name text,
  author_url text,
  comment_url text,
  date_published timestamptz,
  comment_text text not null,
  created_at timestamptz not null default now(),
  primary key (recipe_id, text_hash)
);

-- ===============
-- Product (relational)
-- ===============

create table if not exists public.recipes (
  recipe_id bigint primary key,
  source text not null default 'cookpad',
  locale text not null default 'vn',
  url text not null,
  name text,
  description text,
  hero_image text,
  date_published timestamptz,
  date_modified timestamptz,
  cuisine text,
  author_name text,
  author_url text,
  bookmark_count int,
  like_count int,
  comment_count int,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists recipes_set_updated_at on public.recipes;
create trigger recipes_set_updated_at
before update on public.recipes
for each row execute function public.set_updated_at();

create table if not exists public.recipe_keywords (
  recipe_id bigint not null references public.recipes(recipe_id) on delete cascade,
  keyword text not null,
  created_at timestamptz not null default now(),
  primary key (recipe_id, keyword)
);

create table if not exists public.recipe_ingredients (
  recipe_id bigint not null references public.recipes(recipe_id) on delete cascade,
  ingredient_index int not null check (ingredient_index >= 0),
  ingredient_text text not null,
  created_at timestamptz not null default now(),
  primary key (recipe_id, ingredient_index)
);

create table if not exists public.recipe_steps (
  recipe_id bigint not null references public.recipes(recipe_id) on delete cascade,
  step_index int not null check (step_index >= 0),
  step_text text,
  step_image text,
  created_at timestamptz not null default now(),
  primary key (recipe_id, step_index)
);

create index if not exists recipes_date_published_idx on public.recipes (date_published desc);

-- ===============
-- RPC: enqueue crawl jobs (listing -> queue)
-- ===============

create or replace function public.enqueue_crawl_jobs(
  p_source text,
  p_locale text,
  p_keyword text,
  p_tier smallint,
  p_page int,
  p_recipe_ids bigint[]
)
returns table (inserted_count int, skipped_count int)
language plpgsql
security definer
as $$
declare
  v_inserted int := 0;
  v_total int := 0;
begin
  if p_recipe_ids is null or array_length(p_recipe_ids, 1) is null then
    return query select 0, 0;
    return;
  end if;

  v_total := array_length(p_recipe_ids, 1);

  with src as (
    select distinct unnest(p_recipe_ids) as recipe_id
  ),
  ins as (
    insert into public.crawl_jobs (source, locale, recipe_id, requested_url, keyword, tier, page, priority, status, next_attempt_at)
    select
      p_source,
      p_locale,
      s.recipe_id,
      'https://cookpad.com/' || p_locale || '/cong-thuc/' || s.recipe_id,
      p_keyword,
      p_tier,
      p_page,
      (p_tier * 1000 + p_page),
      'queued',
      now()
    from src s
    on conflict (source, locale, recipe_id) do nothing
    returning 1
  )
  select count(*) into v_inserted from ins;

  return query select v_inserted, (v_total - v_inserted);
end;
$$;

-- ===============
-- RPC: claim next crawl job (3 workers, SKIP LOCKED)
-- ===============

create or replace function public.claim_next_crawl_job(p_worker_id text)
returns public.crawl_jobs
language plpgsql
security definer
as $$
declare
  v_job public.crawl_jobs;
begin
  with candidate as (
    select id
    from public.crawl_jobs
    where status = 'queued'
      and next_attempt_at <= now()
      and attempts < max_attempts
    order by priority asc, created_at asc
    limit 1
    for update skip locked
  ),
  upd as (
    update public.crawl_jobs j
    set
      status = 'processing',
      attempts = j.attempts + 1,
      claimed_by = p_worker_id,
      claimed_at = now(),
      last_error = null,
      http_status = null,
      invalid_reason = null
    where j.id in (select id from candidate)
    returning j.*
  )
  select * into v_job from upd;

  return v_job;
end;
$$;

-- ===============
-- RPC: mark done / invalid / failed(backoff) / requeue
-- ===============

create or replace function public.mark_crawl_job_done(p_job_id uuid)
returns void
language sql
security definer
as $$
  update public.crawl_jobs
  set status = 'done', next_attempt_at = now()
  where id = p_job_id;
$$;

create or replace function public.mark_crawl_job_invalid(
  p_job_id uuid,
  p_reason text,
  p_http_status int default null
)
returns void
language sql
security definer
as $$
  update public.crawl_jobs
  set status = 'invalid',
      invalid_reason = p_reason,
      http_status = p_http_status,
      next_attempt_at = now()
  where id = p_job_id;
$$;

create or replace function public.mark_crawl_job_failed(
  p_job_id uuid,
  p_error text,
  p_http_status int default null
)
returns void
language plpgsql
security definer
as $$
declare
  v_attempts int;
  v_max int;
  v_backoff interval;
begin
  select attempts, max_attempts into v_attempts, v_max
  from public.crawl_jobs
  where id = p_job_id;

  if not found then
    return;
  end if;

  if v_attempts >= v_max then
    update public.crawl_jobs
    set status = 'dead',
        last_error = p_error,
        http_status = p_http_status,
        next_attempt_at = now()
    where id = p_job_id;
    return;
  end if;

  v_backoff :=
    case v_attempts
      when 1 then interval '60 seconds'
      when 2 then interval '180 seconds'
      else interval '180 seconds'
    end;

  update public.crawl_jobs
  set status = 'queued',
      last_error = p_error,
      http_status = p_http_status,
      next_attempt_at = now() + v_backoff
  where id = p_job_id;
end;
$$;

create or replace function public.requeue_crawl_job(
  p_job_id uuid,
  p_delay_seconds int default 0
)
returns void
language sql
security definer
as $$
  update public.crawl_jobs
  set status = 'queued',
      next_attempt_at = now() + make_interval(secs => greatest(p_delay_seconds, 0)),
      invalid_reason = null,
      last_error = null,
      http_status = null
  where id = p_job_id;
$$;

-- ===============
-- RPC: update keyword feedback (stale detection)
-- ===============

create or replace function public.update_keyword_feedback(
  p_keyword text,
  p_page int,
  p_date_published date
)
returns void
language plpgsql
security definer
as $$
begin
  insert into public.keyword_feedback(keyword, is_stale, stale_page, oldest_published_seen)
  values (p_keyword, true, p_page, p_date_published)
  on conflict (keyword) do update
  set
    is_stale = public.keyword_feedback.is_stale or true,
    stale_page = least(coalesce(public.keyword_feedback.stale_page, p_page), p_page),
    oldest_published_seen = least(coalesce(public.keyword_feedback.oldest_published_seen, p_date_published), p_date_published),
    updated_at = now();
end;
$$;

-- ===============
-- RPC: promote one recipe from staging -> product (only if within cutoff)
-- ===============

create or replace function public.promote_recipe_if_recent(
  p_recipe_id bigint,
  p_cutoff_date date
)
returns boolean
language plpgsql
security definer
as $$
declare
  r public.stg_recipes;
begin
  select * into r from public.stg_recipes where recipe_id = p_recipe_id;
  if not found then
    return false;
  end if;

  if r.date_published is null then
    return false;
  end if;

  if (r.date_published::date) < p_cutoff_date then
    return false;
  end if;

  insert into public.recipes (
    recipe_id, source, locale, url, name, description, hero_image,
    date_published, date_modified, cuisine, author_name, author_url,
    bookmark_count, like_count, comment_count
  )
  values (
    r.recipe_id, r.source, r.locale, r.url, r.name, r.description, r.hero_image,
    r.date_published, r.date_modified, r.cuisine, r.author_name, r.author_url,
    r.bookmark_count, r.like_count, r.comment_count
  )
  on conflict (recipe_id) do update set
    url = excluded.url,
    name = excluded.name,
    description = excluded.description,
    hero_image = excluded.hero_image,
    date_published = excluded.date_published,
    date_modified = excluded.date_modified,
    cuisine = excluded.cuisine,
    author_name = excluded.author_name,
    author_url = excluded.author_url,
    bookmark_count = excluded.bookmark_count,
    like_count = excluded.like_count,
    comment_count = excluded.comment_count,
    updated_at = now();

  delete from public.recipe_keywords where recipe_id = p_recipe_id;
  insert into public.recipe_keywords (recipe_id, keyword)
  select recipe_id, keyword from public.stg_recipe_keywords where recipe_id = p_recipe_id;

  delete from public.recipe_ingredients where recipe_id = p_recipe_id;
  insert into public.recipe_ingredients (recipe_id, ingredient_index, ingredient_text)
  select recipe_id, ingredient_index, ingredient_text
  from public.stg_recipe_ingredients
  where recipe_id = p_recipe_id
  order by ingredient_index asc;

  delete from public.recipe_steps where recipe_id = p_recipe_id;
  insert into public.recipe_steps (recipe_id, step_index, step_text, step_image)
  select recipe_id, step_index, step_text, step_image
  from public.stg_recipe_steps
  where recipe_id = p_recipe_id
  order by step_index asc;

  return true;
end;
$$;

-- ===============
-- RPC: batch promote from staging -> product
-- ===============

create or replace function public.promote_recent_recipes(
  p_cutoff_date date,
  p_limit int default 500
)
returns int
language plpgsql
security definer
as $$
declare
  v_count int := 0;
  v_id bigint;
begin
  for v_id in
    select recipe_id
    from public.stg_recipes
    where date_published is not null
      and date_published::date >= p_cutoff_date
    order by date_published desc nulls last
    limit greatest(p_limit, 0)
  loop
    if public.promote_recipe_if_recent(v_id, p_cutoff_date) then
      v_count := v_count + 1;
    end if;
  end loop;
  return v_count;
end;
$$;

-- ===============
-- RPC: revive dead or stuck jobs (ops)
-- ===============

create or replace function public.revive_dead_jobs(p_limit int default 1000)
returns int
language sql
security definer
as $$
  with upd as (
    update public.crawl_jobs
    set status = 'queued',
        attempts = 0,
        next_attempt_at = now(),
        claimed_by = null,
        claimed_at = null,
        invalid_reason = null,
        last_error = null,
        http_status = null,
        updated_at = now()
    where id in (
      select id from public.crawl_jobs
      where status = 'dead'
      order by updated_at desc
      limit greatest(p_limit, 0)
    )
    returning 1
  )
  select count(*) from upd;
$$;

create or replace function public.reset_stuck_processing_jobs(p_stuck_minutes int default 30)
returns int
language sql
security definer
as $$
  with upd as (
    update public.crawl_jobs
    set status = 'queued',
        next_attempt_at = now(),
        claimed_by = null,
        claimed_at = null,
        updated_at = now()
    where status = 'processing'
      and claimed_at is not null
      and claimed_at < (now() - make_interval(mins => greatest(p_stuck_minutes, 1)))
    returning 1
  )
  select count(*) from upd;
$$;

-- ===============
-- RPC: prune product (keep only last 30 days)
-- ===============

create or replace function public.prune_product_older_than(p_cutoff_date date)
returns int
language sql
security definer
as $$
  with del as (
    delete from public.recipes
    where date_published is not null
      and date_published::date < p_cutoff_date
    returning 1
  )
  select count(*) from del;
$$;
