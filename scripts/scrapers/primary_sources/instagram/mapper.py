"""Instagram posts are classified by ig-gateway (local model, no LLM) and posted by S5's runner, so this source has no mapper of its own."""

from __future__ import annotations


def map_events(_raw, _meta) -> list[dict]:
    return []
