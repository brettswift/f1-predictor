"""Safe readiness checks for permitted sentiment sources.

This module deliberately validates configuration and records sanitized access
evidence only. It never accepts a configured variable name as proof that an
authenticated source read succeeded.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Mapping


class AccessState(StrEnum):
    READY = "ready"
    BLOCKED = "blocked"
    AUTH_FAILURE = "auth_failure"


REDDIT_REQUIRED_SECRET_REFS = (
    "REDDIT_CLIENT_ID",
    "REDDIT_CLIENT_SECRET",
    "REDDIT_REFRESH_TOKEN",
)


@dataclass(frozen=True)
class SourceReadiness:
    source: str
    kind: str
    state: AccessState
    authenticated_read_proven: bool
    reason: str
    missing_secret_refs: tuple[str, ...] = ()

    def sanitized(self) -> dict[str, object]:
        """Return reportable metadata; values and author data never enter it."""
        return asdict(self)


def reddit_readiness(env: Mapping[str, str]) -> SourceReadiness:
    """Check protected Reddit setup without reading values.

    Presence means only that a later authenticated probe may be attempted. A
    successful ``authenticated_fetch_evidence`` call is the sole way to mark
    Reddit access as READY.
    """
    missing = tuple(name for name in REDDIT_REQUIRED_SECRET_REFS if not env.get(name))
    if missing:
        return SourceReadiness(
            source="reddit", kind="fan_sentiment", state=AccessState.BLOCKED,
            authenticated_read_proven=False,
            reason="protected credential setup is incomplete", missing_secret_refs=missing,
        )
    return SourceReadiness(
        source="reddit", kind="fan_sentiment", state=AccessState.BLOCKED,
        authenticated_read_proven=False,
        reason="credentials configured; authenticated read has not been proven",
    )


def authenticated_fetch_evidence(*, status_code: int, stable_id: str | None,
                                 permalink: str | None,
                                 source_created_at: str | None) -> SourceReadiness:
    """Classify an authenticated API probe using token-free response metadata."""
    if status_code in {401, 403}:
        return SourceReadiness(
            source="reddit", kind="fan_sentiment", state=AccessState.AUTH_FAILURE,
            authenticated_read_proven=False,
            reason=f"authenticated API probe returned {status_code}; renew or replace the protected token",
        )
    if 200 <= status_code < 300 and stable_id and permalink and source_created_at:
        return SourceReadiness(
            source="reddit", kind="fan_sentiment", state=AccessState.READY,
            authenticated_read_proven=True,
            reason="authenticated API probe returned stable, attributable source metadata",
        )
    return SourceReadiness(
        source="reddit", kind="fan_sentiment", state=AccessState.BLOCKED,
        authenticated_read_proven=False,
        reason="authenticated API probe did not return complete stable source metadata",
    )
