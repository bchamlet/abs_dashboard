"""
Natural language dataset search.

Primary: Claude API (claude-haiku-4-5) matches user query to ABS dataflow catalogue.
Fallback: simple fuzzy keyword matching.
"""
import json
import re
from difflib import SequenceMatcher

import anthropic

from config import ANTHROPIC_API_KEY
from modules.metadata import get_all_dataflow_summaries

_SYSTEM_PROMPT = """You are an assistant that helps users find Australian Bureau of Statistics (ABS) datasets.
Given a list of ABS dataflows and a user query, identify the top matching datasets.
Return ONLY a valid JSON array (no markdown, no explanation) of objects with keys: id, name, reason.
Order by relevance, most relevant first. Limit to the requested number."""


def find_matching_datasets(user_query: str, top_n: int = 5) -> list[dict]:
    """
    Return up to top_n matching datasets for the given plain-English query.
    Each result: {id, name, description, reason}
    """
    summaries = get_all_dataflow_summaries()
    if not summaries:
        return []

    if ANTHROPIC_API_KEY:
        try:
            return _claude_search(user_query, summaries, top_n)
        except Exception:
            pass

    return _fuzzy_search(user_query, summaries, top_n)


def _claude_search(query: str, summaries: list[dict], top_n: int) -> list[dict]:
    catalogue_json = json.dumps(
        [{"id": s["id"], "name": s["name"], "description": s["description"]} for s in summaries],
        indent=2,
    )
    user_message = (
        f"User query: \"{query}\"\n\n"
        f"Return the top {top_n} matching datasets from this catalogue:\n{catalogue_json}"
    )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    raw = message.content[0].text.strip()

    # Strip any accidental markdown fences
    raw = re.sub(r"^```[a-z]*\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw)

    results = json.loads(raw)

    # Enrich with description from our local summaries
    summary_map = {s["id"]: s for s in summaries}
    enriched = []
    for r in results[:top_n]:
        meta = summary_map.get(r.get("id", ""), {})
        enriched.append({
            "id": r.get("id", ""),
            "name": r.get("name", meta.get("name", "")),
            "description": meta.get("description", ""),
            "reason": r.get("reason", ""),
        })
    return enriched


def _fuzzy_search(query: str, summaries: list[dict], top_n: int) -> list[dict]:
    """Score each dataset by keyword overlap with the query."""
    query_lower = query.lower()
    scored = []
    for s in summaries:
        haystack = f"{s['name']} {s['description']}".lower()
        score = SequenceMatcher(None, query_lower, haystack).ratio()
        # Boost if any query word appears verbatim
        for word in query_lower.split():
            if len(word) > 2 and word in haystack:
                score += 0.2
        scored.append((score, s))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [
        {**s, "reason": "Keyword match"}
        for _, s in scored[:top_n]
        if scored[0][0] > 0
    ]
