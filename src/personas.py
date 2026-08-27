"""F1-112: deterministic prediction personas for synthetic replay users.

Each persona implements a deterministic strategy that maps a race entry list to
P1/P2/P3 driver picks. Personas are referenced by slug in ``users.persona`` and
are exercised by ``scripts/replay_season.py``.

Archetypes (slugs) implemented here:

1. random_picker      - Chaos picker: shuffles the grid and picks the first three.
2. front_runner       - Always picks the three lowest driver numbers (champions first).
3. midfield_oracle    - Targets the middle of the pack by driver number.
4. contrarian         - Picks the three highest driver numbers (against the favorite).
5. max_verstappen_fanboy - Picks Max Verstappen for P1 whenever he enters.
6. statistics_junkie  - Picks drivers whose numbers are closest to the round number.
7. home_track_hero    - Picks drivers sharing the race country's nationality.

These slugs are also the canonical AC traceability list for BUD-134.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class RaceContext:
    """Everything a persona strategy needs to make a pick."""

    race_id: int
    race_name: str
    round: int
    date: str
    drivers: tuple[dict, ...]

    @property
    def driver_ids(self) -> tuple[int, ...]:
        return tuple(d["id"] for d in self.drivers)


def _race_country(race_name: str) -> str | None:
    """Naive country lookup used by the home_track_hero persona.

    Only common F1 venues are mapped; unknown races fall back to the persona's
    default strategy so the pick stays deterministic.
    """
    name = race_name.lower()
    # Map from race name fragments to ISO-like country codes used in driver nationalities.
    mapping = {
        "bahrain": "Bahraini",
        "saudi": "Saudi",
        "australian": "Australian",
        "japanese": "Japanese",
        "chinese": "Chinese",
        "miami": "American",
        "emilia": "Italian",
        "monaco": "Monegasque",
        "canadian": "Canadian",
        "spanish": "Spanish",
        "austrian": "Austrian",
        "british": "British",
        "hungarian": "Hungarian",
        "belgian": "Belgian",
        "dutch": "Dutch",
        "italian": "Italian",
        "azerbaijan": "Azerbaijani",
        "singapore": "Singaporean",
        "united states": "American",
        "las vegas": "American",
        "mexican": "Mexican",
        "brazilian": "Brazilian",
        "qatar": "Qatari",
        "abu dhabi": "Emirati",
        "french": "French",
        "german": "German",
    }
    for fragment, country in mapping.items():
        if fragment in name:
            return country
    return None


def _shuffled_sample(
    driver_ids: list[int], rng: random.Random, k: int = 3
) -> list[int]:
    """Deterministic ``random.sample`` wrapper that always returns ``k`` items."""
    if len(driver_ids) <= k:
        return driver_ids
    return rng.sample(driver_ids, k)


def _make_rng(seed: int, user_id: str, race_id: int, persona_slug: str) -> random.Random:
    """Build a deterministic RNG for one (user, race, persona) pick."""
    h = hash((seed, user_id, race_id, persona_slug))
    # Use the absolute value; hash() can be negative on some platforms.
    return random.Random(abs(h))


class Persona:
    """Base class for a synthetic prediction persona."""

    slug: str = ""
    name: str = ""
    description: str = ""

    def pick(self, context: RaceContext, rng: random.Random) -> tuple[int, int, int]:
        raise NotImplementedError


class RandomPicker(Persona):
    """Chaos picker: shuffles the grid and takes the first three."""

    slug = "random_picker"
    name = "Chaos Picker"
    description = "Picks three random drivers from the entry list."

    def pick(self, context: RaceContext, rng: random.Random) -> tuple[int, int, int]:
        p1, p2, p3 = _shuffled_sample(list(context.driver_ids), rng, 3)
        return p1, p2, p3


class FrontRunner(Persona):
    """Front runner: always backs the lowest driver numbers."""

    slug = "front_runner"
    name = "Front Runner"
    description = "Picks the three drivers with the lowest car numbers."

    def pick(self, context: RaceContext, rng: random.Random) -> tuple[int, int, int]:
        ordered = sorted(context.drivers, key=lambda d: d["number"])
        return tuple(d["id"] for d in ordered[:3])


class MidfieldOracle(Persona):
    """Midfield oracle: targets the middle of the grid by car number."""

    slug = "midfield_oracle"
    name = "Midfield Oracle"
    description = "Picks three drivers from the middle of the number order."

    def pick(self, context: RaceContext, rng: random.Random) -> tuple[int, int, int]:
        ordered = sorted(context.drivers, key=lambda d: d["number"])
        n = len(ordered)
        if n <= 3:
            return tuple(d["id"] for d in ordered)
        # Start near the middle and take the next three.
        start = n // 2
        if start + 3 > n:
            start = n - 3
        return tuple(d["id"] for d in ordered[start : start + 3])


class Contrarian(Persona):
    """Contrarian: bets against the favorite with the highest numbers."""

    slug = "contrarian"
    name = "Contrarian"
    description = "Picks the three drivers with the highest car numbers."

    def pick(self, context: RaceContext, rng: random.Random) -> tuple[int, int, int]:
        ordered = sorted(context.drivers, key=lambda d: d["number"], reverse=True)
        return tuple(d["id"] for d in ordered[:3])


class MaxVerstappenFanboy(Persona):
    """Max Verstappen fanboy: Max for P1, random for P2/P3."""

    slug = "max_verstappen_fanboy"
    name = "Max Verstappen Fanboy"
    description = "Picks Max Verstappen for P1 whenever he is on the entry list."

    def pick(self, context: RaceContext, rng: random.Random) -> tuple[int, int, int]:
        max_id = None
        for d in context.drivers:
            name = (d.get("name") or "").lower()
            code = (d.get("code") or "").lower()
            if "verstappen" in name or code == "ver":
                max_id = d["id"]
                break
        if max_id is None:
            # Max isn't racing; behave like a chaos picker.
            p1, p2, p3 = _shuffled_sample(list(context.driver_ids), rng, 3)
            return p1, p2, p3
        rest = [d_id for d_id in context.driver_ids if d_id != max_id]
        p2, p3 = _shuffled_sample(rest, rng, 2)
        return max_id, p2, p3


class StatisticsJunkie(Persona):
    """Statistics junkie: picks numbers closest to the round number."""

    slug = "statistics_junkie"
    name = "Statistics Junkie"
    description = "Picks the three drivers whose car numbers are closest to the round number."

    def pick(self, context: RaceContext, rng: random.Random) -> tuple[int, int, int]:
        target = context.round
        ordered = sorted(context.drivers, key=lambda d: abs(d["number"] - target))
        return tuple(d["id"] for d in ordered[:3])


class HomeTrackHero(Persona):
    """Home track hero: backs drivers from the race's country."""

    slug = "home_track_hero"
    name = "Home Track Hero"
    description = "Picks drivers whose nationality matches the race country."

    def pick(self, context: RaceContext, rng: random.Random) -> tuple[int, int, int]:
        country = _race_country(context.race_name)
        locals_ = [
            d["id"]
            for d in context.drivers
            if country and (d.get("nationality") or "").lower() == country.lower()
        ]
        if len(locals_) >= 3:
            return tuple(locals_[:3])
        # Fill missing slots deterministically from the remaining drivers.
        remaining = [d_id for d_id in context.driver_ids if d_id not in locals_]
        fill = _shuffled_sample(remaining, rng, 3 - len(locals_))
        return tuple((locals_ + fill)[:3])


# Registry used by the replay harness and tests.
PERSONA_CLASSES: list[type[Persona]] = [
    RandomPicker,
    FrontRunner,
    MidfieldOracle,
    Contrarian,
    MaxVerstappenFanboy,
    StatisticsJunkie,
    HomeTrackHero,
]

PERSONAS_BY_SLUG: dict[str, Persona] = {cls.slug: cls() for cls in PERSONA_CLASSES}

DEFAULT_PERSONA_SLUGS = [cls.slug for cls in PERSONA_CLASSES]


def get_persona(slug: str) -> Persona:
    """Return the persona instance for ``slug``.

    Raises ``KeyError`` for unknown slugs so typos fail fast.
    """
    return PERSONAS_BY_SLUG[slug]


def list_persona_slugs() -> list[str]:
    """Return all canonical persona slugs in AC order."""
    return DEFAULT_PERSONA_SLUGS.copy()


def assign_personas(user_count: int, mixed: bool = False, persona_slug: str | None = None) -> list[str]:
    """Assign a persona to each synthetic user.

    When ``mixed`` is True, personas are cycled through the canonical list.
    When ``persona_slug`` is set, every user gets that persona.
    Otherwise every user gets the default ``random_picker`` persona.

    The assignment order is deterministic for a given ``user_count``.
    """
    if mixed:
        slugs = list_persona_slugs()
        return [slugs[i % len(slugs)] for i in range(user_count)]
    if persona_slug:
        return [persona_slug] * user_count
    return [RandomPicker.slug] * user_count


def generate_persona_prediction(
    seed: int,
    user_id: str,
    persona_slug: str,
    context: RaceContext,
) -> tuple[int, int, int]:
    """Generate a deterministic P1/P2/P3 pick for a user+race+persona."""
    persona = get_persona(persona_slug)
    rng = _make_rng(seed, user_id, context.race_id, persona_slug)
    return persona.pick(context, rng)
