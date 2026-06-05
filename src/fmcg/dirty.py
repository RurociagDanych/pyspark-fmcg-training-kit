"""Helpers that inject realistic data-quality problems.

These are used by the generator so the data-quality notebook has real targets to
find and fix. Everything is driven by a seeded ``random.Random`` for
reproducibility.
"""

from __future__ import annotations

import random


def messy_casing(value: str, rng: random.Random, rate: float) -> str:
    """With probability ``rate`` return a casing/whitespace-corrupted variant.

    Realistic dimension data arrives from several source systems, so the same
    category or region shows up as ``"Beverages"``, ``"beverages"``,
    ``"BEVERAGES "`` and ``" Beverages"``. The canonical form is title-case with
    no surrounding whitespace.
    """
    if rng.random() >= rate:
        return value
    variant = rng.choice(
        [
            value.lower(),
            value.upper(),
            f" {value}",
            f"{value} ",
            f"  {value.lower()} ",
        ]
    )
    return variant


def maybe(rng: random.Random, rate: float) -> bool:
    """True with probability ``rate``."""
    return rng.random() < rate
