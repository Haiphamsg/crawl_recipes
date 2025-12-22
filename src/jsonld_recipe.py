from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from selectolax.parser import HTMLParser

from .utils import parse_datetime_maybe, sha256_text


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _first_url_from_image(image: Any) -> Optional[str]:
    if not image:
        return None
    if isinstance(image, str):
        return image.strip() or None
    if isinstance(image, list):
        for item in image:
            u = _first_url_from_image(item)
            if u:
                return u
        return None
    if isinstance(image, dict):
        if isinstance(image.get("url"), str) and image["url"].strip():
            return image["url"].strip()
        if isinstance(image.get("@id"), str) and image["@id"].strip():
            return image["@id"].strip()
    return None


def _extract_counts(interaction_statistic: Any) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    bookmark = like = comment = None
    for stat in _as_list(interaction_statistic):
        if not isinstance(stat, dict):
            continue
        it = stat.get("interactionType")
        count = stat.get("userInteractionCount")
        try:
            count_int = int(count) if count is not None else None
        except (ValueError, TypeError):
            count_int = None

        it_str = None
        if isinstance(it, dict):
            it_str = it.get("@type") or it.get("name")
        elif isinstance(it, str):
            it_str = it

        if not it_str or count_int is None:
            continue
        if "BookmarkAction" in it_str:
            bookmark = count_int
        elif "LikeAction" in it_str:
            like = count_int
        elif "CommentAction" in it_str:
            comment = count_int
    return bookmark, like, comment


def _extract_author(author: Any) -> Tuple[Optional[str], Optional[str]]:
    for a in _as_list(author):
        if isinstance(a, dict):
            name = a.get("name")
            url = a.get("url") or a.get("@id")
            return (name if isinstance(name, str) else None, url if isinstance(url, str) else None)
        if isinstance(a, str):
            return (a, None)
    return (None, None)


def _extract_keywords(keywords: Any) -> Tuple[Optional[str], List[str]]:
    if keywords is None:
        return (None, [])
    if isinstance(keywords, list):
        cleaned = [k.strip() for k in keywords if isinstance(k, str) and k.strip()]
        return (", ".join(cleaned) if cleaned else None, cleaned)
    if isinstance(keywords, str):
        raw = keywords.strip()
        parts = [p.strip() for p in raw.replace(";", ",").split(",")]
        parts = [p for p in parts if p]
        return (raw or None, parts)
    return (None, [])


def _extract_instructions(recipe_instructions: Any) -> List[Dict[str, Optional[str]]]:
    steps: List[Dict[str, Optional[str]]] = []
    for item in _as_list(recipe_instructions):
        if isinstance(item, str):
            text = item.strip()
            if text:
                steps.append({"text": text, "image": None})
            continue
        if isinstance(item, dict):
            text = item.get("text") or item.get("name")
            if isinstance(text, str):
                text = text.strip()
            else:
                text = None
            image = _first_url_from_image(item.get("image"))
            if text or image:
                steps.append({"text": text, "image": image})
    return steps


def _extract_comments(comments: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in _as_list(comments):
        if not isinstance(c, dict):
            continue
        text = c.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        author_name, author_url = _extract_author(c.get("author"))
        out.append(
            {
                "author_name": author_name,
                "author_url": author_url,
                "url": c.get("url") if isinstance(c.get("url"), str) else None,
                "date_published": parse_datetime_maybe(c.get("datePublished") if isinstance(c.get("datePublished"), str) else None),
                "text": text.strip(),
                "text_hash": sha256_text(text),
            }
        )
    return out


@dataclass(frozen=True)
class ParsedRecipe:
    recipe_id: int
    url: str
    name: Optional[str]
    description: Optional[str]
    hero_image: Optional[str]
    date_published: Any
    date_modified: Any
    cuisine: Optional[str]
    author_name: Optional[str]
    author_url: Optional[str]
    keywords_raw: Optional[str]
    keywords: List[str]
    ingredients: List[str]
    steps: List[Dict[str, Optional[str]]]
    bookmark_count: Optional[int]
    like_count: Optional[int]
    comment_count: Optional[int]
    comments: List[Dict[str, Any]]


def parse_jsonld_recipe(html: str, requested_url: str, recipe_id: int) -> Optional[ParsedRecipe]:
    candidates: List[Dict[str, Any]] = []

    tree = HTMLParser(html)
    for s in tree.css('script[type="application/ld+json"]'):
        raw = (s.text() or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for obj in _as_list(parsed):
            if isinstance(obj, dict):
                if isinstance(obj.get("@graph"), list):
                    for g in obj["@graph"]:
                        if isinstance(g, dict):
                            candidates.append(g)
                candidates.append(obj)

    recipe_obj: Optional[Dict[str, Any]] = None
    for obj in candidates:
        t = obj.get("@type")
        if t == "Recipe" or (isinstance(t, list) and "Recipe" in t):
            recipe_obj = obj
            break
    if recipe_obj is None:
        return None

    name = recipe_obj.get("name") if isinstance(recipe_obj.get("name"), str) else None
    description = (
        recipe_obj.get("description") if isinstance(recipe_obj.get("description"), str) else None
    )
    url = recipe_obj.get("url") if isinstance(recipe_obj.get("url"), str) else requested_url
    hero_image = _first_url_from_image(recipe_obj.get("image"))
    cuisine_val = recipe_obj.get("recipeCuisine")
    cuisine = None
    if isinstance(cuisine_val, str):
        cuisine = cuisine_val.strip() or None
    elif isinstance(cuisine_val, list):
        cuisine = ", ".join([c.strip() for c in cuisine_val if isinstance(c, str) and c.strip()]) or None

    author_name, author_url = _extract_author(recipe_obj.get("author"))
    keywords_raw, keywords = _extract_keywords(recipe_obj.get("keywords"))
    ingredients = [i.strip() for i in _as_list(recipe_obj.get("recipeIngredient")) if isinstance(i, str) and i.strip()]

    steps = _extract_instructions(recipe_obj.get("recipeInstructions"))
    bookmark_count, like_count, comment_count = _extract_counts(recipe_obj.get("interactionStatistic"))
    comments = _extract_comments(recipe_obj.get("comment"))

    date_published = parse_datetime_maybe(
        recipe_obj.get("datePublished") if isinstance(recipe_obj.get("datePublished"), str) else None
    )
    date_modified = parse_datetime_maybe(
        recipe_obj.get("dateModified") if isinstance(recipe_obj.get("dateModified"), str) else None
    )

    return ParsedRecipe(
        recipe_id=recipe_id,
        url=url,
        name=name,
        description=description,
        hero_image=hero_image,
        date_published=date_published,
        date_modified=date_modified,
        cuisine=cuisine,
        author_name=author_name,
        author_url=author_url,
        keywords_raw=keywords_raw,
        keywords=keywords,
        ingredients=ingredients,
        steps=steps,
        bookmark_count=bookmark_count,
        like_count=like_count,
        comment_count=comment_count,
        comments=comments,
    )
