#!/usr/bin/env python3
"""
Minimal Telegram alerting helper for F1 cron jobs.

Uses the Telegram Bot API directly (urllib) so no extra dependencies are
required beyond the Python stdlib.  Credentials are read from the environment:

    TELEGRAM_BOT_TOKEN    Bot token from BotFather
    TELEGRAM_NOTIFY_CHAT_ID  Destination chat id (default: Brett's DM)

This is intentionally small and stateless; deduplication lives in
alerting.py so multiple callers share one alert history.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

DEFAULT_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
DEFAULT_CHAT_ID = os.environ.get("TELEGRAM_NOTIFY_CHAT_ID", "")
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramAlertError(RuntimeError):
    """Failed to deliver a Telegram alert."""


def send_telegram_alert(
    message: str,
    *,
    bot_token: str | None = None,
    chat_id: str | None = None,
) -> dict:
    """
    Send a plain-text Telegram message.

    Returns the parsed Telegram API response.  Raises TelegramAlertError on
    missing credentials or delivery failure.
    """
    bot_token = (bot_token or DEFAULT_BOT_TOKEN or "").strip()
    chat_id = (chat_id or DEFAULT_CHAT_ID or "").strip()

    if not bot_token:
        raise TelegramAlertError("TELEGRAM_BOT_TOKEN is not set")
    if not chat_id:
        raise TelegramAlertError("TELEGRAM_NOTIFY_CHAT_ID is not set")

    url = TELEGRAM_API_URL.format(token=bot_token)
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise TelegramAlertError(f"Telegram HTTP {e.code}: {detail}") from e
    except Exception as exc:
        raise TelegramAlertError(f"Telegram send failed: {exc}") from exc

    if not body.get("ok"):
        raise TelegramAlertError(f"Telegram API error: {body}")

    logger.info("Telegram alert sent (message_id=%s)", body.get("result", {}).get("message_id"))
    return body


def alert_enabled() -> bool:
    """True when both required environment variables are present."""
    return bool(DEFAULT_BOT_TOKEN.strip() and DEFAULT_CHAT_ID.strip())
