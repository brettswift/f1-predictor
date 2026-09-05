"""Unit tests for F1-115 Telegram persona group chat helper."""

from __future__ import annotations

import json
import os
import sys
from http import HTTPStatus
from urllib.error import HTTPError

# Set test environment BEFORE any imports
os.environ["DATABASE_PATH"] = ":memory:"
os.environ["F1_SEASON"] = "2026"
os.environ["OPENF1_OFFLINE"] = "true"
os.environ["TESTING"] = "true"

_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "src"))
sys.path.insert(0, os.path.join(_root, "scripts"))

import pytest

from telegram_group import (
    TelegramConfig,
    TelegramError,
    format_prediction_message,
    send_message,
    send_persona_predictions,
)


class TestTelegramConfig:
    """F1-115 AC: Telegram configuration is loaded from environment."""

    def test_from_env_requires_bot_token(self, monkeypatch):
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
        monkeypatch.delenv("F1_PERSONA_CHAT_ID", raising=False)
        with pytest.raises(TelegramError):
            TelegramConfig.from_env()

    def test_from_env_requires_chat_id(self, monkeypatch):
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.setenv("F1_PERSONA_CHAT_ID", "-12345")
        with pytest.raises(TelegramError):
            TelegramConfig.from_env()

    def test_from_env_returns_config(self, monkeypatch):
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
        monkeypatch.setenv("F1_PERSONA_CHAT_ID", "-12345")
        config = TelegramConfig.from_env()
        assert config.bot_token == "test-token"
        assert config.chat_id == "-12345"


class TestFormatPredictionMessage:
    """F1-115 AC: predictions appear as labeled persona messages."""

    def test_includes_persona_name_username_and_picks(self):
        text = format_prediction_message(
            persona_name="Front Runner",
            username="synthetic_42_001",
            race_name="Bahrain Grand Prix",
            p1="Max Verstappen",
            p2="Lando Norris",
            p3="Charles Leclerc",
        )
        assert "Front Runner" in text
        assert "synthetic_42_001" in text
        assert "Bahrain Grand Prix" in text
        assert "P1: Max Verstappen" in text
        assert "P2: Lando Norris" in text
        assert "P3: Charles Leclerc" in text


class TestSendMessage:
    """F1-115 AC: messages are delivered via Telegram Bot API."""

    def test_success(self, monkeypatch):
        config = TelegramConfig("test-token", "-12345")
        calls = []

        def fake_urlopen(req, timeout=None):
            payload = json.loads(req.data)
            calls.append(payload)
            response_data = {"ok": True, "result": {"message_id": 1}}
            class FakeResponse:
                def read(self):
                    return json.dumps(response_data).encode("utf-8")
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    return False
            return FakeResponse()

        monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
        result = send_message(config, "Hello group")
        assert result["ok"] is True
        assert len(calls) == 1
        assert calls[0]["chat_id"] == "-12345"
        assert calls[0]["text"] == "Hello group"

    def test_http_error_raises_telegram_error(self, monkeypatch):
        config = TelegramConfig("test-token", "-12345")

        def fake_urlopen(req, timeout=None):
            raise HTTPError(
                url=req.full_url,
                code=HTTPStatus.BAD_REQUEST,
                msg="Bad Request",
                hdrs=None,
                fp=None,
            )

        monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
        with pytest.raises(TelegramError):
            send_message(config, "Hello group")

    def test_telegram_api_error_raises(self, monkeypatch):
        config = TelegramConfig("test-token", "-12345")

        def fake_urlopen(req, timeout=None):
            response_data = {"ok": False, "description": "Chat not found"}
            class FakeResponse:
                def read(self):
                    return json.dumps(response_data).encode("utf-8")
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    return False
            return FakeResponse()

        monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
        with pytest.raises(TelegramError):
            send_message(config, "Hello group")


class TestSendPersonaPredictions:
    """F1-115 AC: each persona prediction is a separate message."""

    def test_sends_one_message_per_prediction(self, monkeypatch):
        config = TelegramConfig("test-token", "-12345")
        calls = []

        def fake_urlopen(req, timeout=None):
            payload = json.loads(req.data)
            calls.append(payload)
            response_data = {"ok": True, "result": {"message_id": len(calls)}}
            class FakeResponse:
                def read(self):
                    return json.dumps(response_data).encode("utf-8")
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    return False
            return FakeResponse()

        monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
        predictions = [
            ("Front Runner", "synthetic_42_001", "A", "B", "C"),
            ("Contrarian", "synthetic_42_002", "D", "E", "F"),
        ]
        result = send_persona_predictions(config, "Bahrain Grand Prix", predictions)
        assert len(result) == 2
        assert len(calls) == 2
        assert "Front Runner" in calls[0]["text"]
        assert "Contrarian" in calls[1]["text"]
