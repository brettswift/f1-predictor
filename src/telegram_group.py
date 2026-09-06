"""F1-115: Telegram persona group chat helper.

The replay harness (``scripts/replay_season.py``) calls this module to emit
synthetic-persona predictions into a Telegram group chat where Brett is a
member. Telegram bots cannot create groups or add members, so the group is
set up manually once and its chat id is supplied via environment variable.

This module is intentionally thin and has no Flask dependency so it can be
used from the replay harness, cron jobs, and tests.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Iterable

TELEGRAM_API = "https://api.telegram.org"


class TelegramError(Exception):
    """Raised when a Telegram API call fails."""


@dataclass(frozen=True)
class TelegramConfig:
    """Runtime configuration for Telegram notifications."""

    bot_token: str
    chat_id: str

    @classmethod
    def from_env(cls) -> "TelegramConfig":
        """Load config from environment.

        Raises:
            TelegramError: if either token or chat id is missing.
        """
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.environ.get("F1_PERSONA_CHAT_ID", "").strip()
        if not token:
            raise TelegramError("TELEGRAM_BOT_TOKEN is not set")
        if not chat_id:
            raise TelegramError("F1_PERSONA_CHAT_ID is not set")
        return cls(token, chat_id)


def _api_url(token: str, method: str) -> str:
    """Build a Telegram Bot API URL for ``method``."""
    return f"{TELEGRAM_API}/bot{urllib.parse.quote(token, safe=':/')}/{method}"


def _post_json(url: str, payload: dict) -> dict:
    """POST JSON to a Telegram endpoint and return the parsed response."""
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        raise TelegramError(f"Telegram HTTP {e.code}: {err}") from e
    except OSError as e:
        raise TelegramError(f"Telegram request failed: {e}") from e

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise TelegramError(f"Telegram returned invalid JSON: {raw!r}") from e

    if not data.get("ok"):
        raise TelegramError(f"Telegram error: {data!r}")
    return data


def send_message(config: TelegramConfig, text: str) -> dict:
    """Send a plain-text message to the configured chat."""
    url = _api_url(config.bot_token, "sendMessage")
    return _post_json(url, {"chat_id": config.chat_id, "text": text})


def format_prediction_message(
    persona_name: str,
    username: str,
    race_name: str,
    p1: str,
    p2: str,
    p3: str,
) -> str:
    """Format a single persona prediction for Telegram."""
    return (
        f"🏁 <b>{persona_name}</b> (@{username})\n"
        f"<i>{race_name}</i>\n"
        f"P1: {p1}\nP2: {p2}\nP3: {p3}"
    )


def send_persona_predictions(
    config: TelegramConfig,
    race_name: str,
    predictions: Iterable[tuple[str, str, str, str, str]],
) -> list[dict]:
    """Send one message per persona prediction.

    Args:
        config: Telegram configuration.
        race_name: Name of the race being predicted.
        predictions: Iterable of ``(persona_name, username, p1, p2, p3)``
            tuples, one per persona.

    Returns:
        List of Telegram API responses (one per sent message).
    """
    responses: list[dict] = []
    for persona_name, username, p1, p2, p3 in predictions:
        text = format_prediction_message(
            persona_name=persona_name,
            username=username,
            race_name=race_name,
            p1=p1,
            p2=p2,
            p3=p3,
        )
        responses.append(send_message(config, text))
    return responses
