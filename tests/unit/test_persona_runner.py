"""Unit tests for the persona runner (BUD-171 / F1-113).

Tests verify:
- Persona runner module imports correctly
- Magic-link token reading from DB works
- Driver option parsing from HTML works
- Module-level helpers work correctly

Full end-to-end runs require a running app instance (integration test).
"""

import pytest
import requests
import sqlite3


class TestGetLoginToken:
    """Tests for _get_login_token which reads tokens from the app's DB."""

    def test_returns_unused_token(self, tmp_path):
        from persona_runner import _get_login_token

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS login_tokens (id INTEGER PRIMARY KEY, email TEXT, token TEXT, used INTEGER DEFAULT 0)"
        )
        conn.execute(
            "INSERT INTO login_tokens (email, token, used) VALUES ('test@ex.com', 'abc123', 0)"
        )
        conn.commit()
        conn.close()

        token = _get_login_token(str(db_path), "test@ex.com")
        assert token == "abc123"

    def test_prefers_most_recent_unused(self, tmp_path):
        from persona_runner import _get_login_token

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS login_tokens (id INTEGER PRIMARY KEY, email TEXT, token TEXT, used INTEGER DEFAULT 0)"
        )
        conn.execute(
            "INSERT INTO login_tokens (email, token, used) VALUES ('test@ex.com', 'first', 0)"
        )
        conn.execute(
            "INSERT INTO login_tokens (email, token, used) VALUES ('test@ex.com', 'second', 0)"
        )
        conn.commit()
        conn.close()

        token = _get_login_token(str(db_path), "test@ex.com")
        assert token == "second"

    def test_raises_when_no_unused_token(self, tmp_path):
        from persona_runner import _get_login_token, PersonaRunnerError

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS login_tokens (id INTEGER PRIMARY KEY, email TEXT, token TEXT, used INTEGER DEFAULT 0)"
        )
        conn.commit()
        conn.close()

        with pytest.raises(PersonaRunnerError):
            _get_login_token(str(db_path), "nobody@ex.com")


class TestParseDriverOptions:
    """Tests for HTML driver option parsing."""

    def test_parses_driver_options(self):
        from persona_runner import _parse_driver_options

        html = """
        <select name="p1">
            <option value="1">Max Verstappen</option>
            <option value="2">Lewis Hamilton</option>
            <option value="3">Charles Leclerc</option>
            <option value="4" selected>Lando Norris</option>
        </select>
        """
        ids = _parse_driver_options(html)
        assert ids == [1, 2, 3, 4]

    def test_parses_multiple_selects(self):
        from persona_runner import _parse_driver_options

        html = """
        <select name="p1">
            <option value="1">Driver A</option>
            <option value="2">Driver B</option>
        </select>
        <select name="p2">
            <option value="1">Driver A</option>
            <option value="3">Driver C</option>
        </select>
        """
        ids = _parse_driver_options(html)
        assert ids == [1, 2, 3]

    def test_raises_on_no_options(self):
        from persona_runner import _parse_driver_options, PersonaRunnerError

        with pytest.raises(PersonaRunnerError):
            _parse_driver_options("<html><body>No form here</body></html>")


class TestManager:
    """Verify the module can be imported and its public API is intact."""

    def test_module_imports(self):
        from persona_runner import (
            create_persona_account,
            join_league,
            submit_prediction,
            run_persona,
            PersonaRunnerError,
        )
        assert callable(create_persona_account)
        assert callable(join_league)
        assert callable(submit_prediction)
        assert callable(run_persona)
        assert issubclass(PersonaRunnerError, Exception)

    def test_cli_entry_point(self):
        """main() should accept --help gracefully."""
        import sys
        from persona_runner import main

        sys.argv = ["persona_runner.py", "--help"]
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 0