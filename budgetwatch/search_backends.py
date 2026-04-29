"""
BudgetWatch — Search backends.

In production this is Meilisearch (BM25) + pgvector (cosine similarity on
multilingual-e5-base embeddings). For dev / prototype, this in-memory
backend uses simple token overlap so the matcher works end-to-end.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal

from matching import MarketplaceSample
from models import LineItem


_TOKEN = re.compile(r"\w+", re.UNICODE)


def _tokenize(s: str) -> set[str]:
    return {t.lower() for t in _TOKEN.findall(s or "") if len(t) > 2}


class InMemorySearchBackend:
    """Trivial Jaccard-similarity backend over LineItems with marketplace
    samples attached — used for prototype only. Replace with Meilisearch
    in production."""

    def __init__(self, indexed: list[LineItem]):
        self._index = [(it, _tokenize(it.description)) for it in indexed]

    def hybrid(self, query: str, k: int = 10) -> list[MarketplaceSample]:
        q = _tokenize(query)
        if not q:
            return []
        scored = []
        for it, toks in self._index:
            if not toks:
                continue
            jacc = len(q & toks) / max(len(q | toks), 1)
            if jacc > 0:
                scored.append((jacc, it))
        scored.sort(reverse=True)

        out: list[MarketplaceSample] = []
        for _, it in scored[:k]:
            out.append(MarketplaceSample(
                vendor="EKATALOG",
                title=it.description,
                price=Decimal(str(it.unit_price)),
                url=it.source_url,
                captured_at=datetime.now(timezone.utc).isoformat(),
                spec_match_score=1.0,
            ))
        return out
