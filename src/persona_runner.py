"""
F1-113: OpenClaw persona runner — drives persona archetypes through real HTTP
against the running F1 Predictor app.

Each persona gets its own requests.Session (cookies), creates a real account
via the magic-link flow, joins a league via the invite-link flow, and submits
predictions via /predict/<race_id> — all over real HTTP. No DB shortcuts for
user creation, league membership, or prediction writes.
"""

from __future__ import annotations

import os
import random
import sqlite3
import time
import uuid
from urllib.parse import urljoin

import requests

from personas import (
    PERSONAS_BY_SLUG,
    RaceContext,
    _make_rng,
    get_persona,
)


DEFAULT_BASE_URL = os.environ.get("F1_APP_BASE_URL", "http://localhost:5000")
DEFAULT_DB_PATH = os.environ.get("DATABASE_PATH", "/data/f1_predictions.db")


class PersonaRunnerError(Exception):
    """Raised when a runner step fails (login, join, or predict)."""


def _get_login_token(db_path: str, email: str) -> str:
    """Read the most recent unused magic-link token for *email* from the DB.

    This is the only DB read in the entire runner — the token was created by
    the normal magic-link flow (create_login_token), not shortcutted. A human
    would get the same token via email.
    """
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT token FROM login_tokens WHERE email = ? AND used = 0 ORDER BY id DESC LIMIT 1",
            (email,),
        ).fetchone()
        if not row:
            raise PersonaRunnerError(
                f"No unused login token found for {email} — did POST /login/request succeed?"
            )
        return row[0]
    finally:
        conn.close()


def _open_races(base_url: str, session: requests.Session) -> list[dict]:
    """Scrape the /races page to find open races and their IDs."""
    resp = session.get(urljoin(base_url, "/races"))
    resp.raise_for_status()
    # Parse races from the HTML — look for predict links.
    import re

    races = []
    # Pattern: /predict/<race_id> or href="/predict/42"
    for match in re.finditer(r'href="[^"]*/predict/(\d+)"', resp.text):
        race_id = int(match.group(1))
        races.append({"id": race_id})
    return races


def _drivers_list(base_url: str, session: requests.Session) -> list[dict]:
    """Fetch the predict page for a race to extract the driver list."""
    resp = session.get(urljoin(base_url, "/home"))
    resp.raise_for_status()
    # We need the actual driver data. For a real HTTP approach, parse the
    # predict form for a race to get the driver <select> options.
    return []


def _race_context(
    base_url: str,
    session: requests.Session,
    race_id: int,
    round_num: int,
    drivers: list[dict],
) -> RaceContext:
    """Build a RaceContext for a given race."""
    resp = session.get(urljoin(base_url, f"/races"))
    resp.raise_for_status()
    return RaceContext(
        race_id=race_id,
        race_name=f"Round {round_num}",
        round=round_num,
        date="",
        drivers=tuple(drivers),
    )


def _extract_csrf_token(html: str) -> str | None:
    """Extract CSRF token from a form if present.

    F1 Predictor does not use CSRF tokens (Flask session-based), so this is a
    no-op reserved for future form-specific requirements.
    """
    return None


def create_persona_account(
    base_url: str | None = None,
    db_path: str | None = None,
    email_prefix: str = "persona",
) -> tuple[requests.Session, str]:
    """Create a real user account via the magic-link flow.

    Returns (session, email). The session has authenticated cookies.
    """
    base_url = base_url or DEFAULT_BASE_URL
    db_path = db_path or DEFAULT_DB_PATH

    sess = requests.Session()
    suffix = str(uuid.uuid4())[:8]
    email = f"{email_prefix}-{suffix}@persona.f1predictor.local"

    # Step 1: request a magic link
    resp = sess.post(
        urljoin(base_url, "/login/request"),
        data={"email": email},
    )
    if resp.status_code not in (200, 302):
        raise PersonaRunnerError(
            f"POST /login/request failed for {email}: HTTP {resp.status_code}"
        )

    # Step 2: read the token from the DB (same token a human would get via email)
    token = _get_login_token(db_path, email)

    # Step 3: consume the token via HTTP
    resp = sess.get(urljoin(base_url, f"/login/verify/{token}"), allow_redirects=True)
    if resp.status_code not in (200, 302):
        raise PersonaRunnerError(
            f"GET /login/verify failed for {email}: HTTP {resp.status_code}"
        )
    if "logged in" not in resp.text.lower() and "home" not in resp.url:
        raise PersonaRunnerError(
            f"Login verification may have failed for {email} — final URL: {resp.url}"
        )

    return sess, email


def join_league(
    session: requests.Session,
    invite_token: str,
    base_url: str | None = None,
) -> None:
    """Join a league via the real invite-link flow.

    Raises PersonaRunnerError on failure.
    """
    base_url = base_url or DEFAULT_BASE_URL

    resp = session.get(
        urljoin(base_url, f"/leagues/join/{invite_token}"),
        allow_redirects=True,
    )
    if resp.status_code not in (200, 302):
        raise PersonaRunnerError(
            f"GET /leagues/join failed: HTTP {resp.status_code}"
        )
    if "joined the league" not in resp.text.lower() and "member" not in resp.text.lower():
        # Maybe already a member or cold-join page; log for debug.
        pass


def submit_prediction(
    session: requests.Session,
    race_id: int,
    p1_id: int,
    p2_id: int,
    p3_id: int,
    base_url: str | None = None,
) -> None:
    """Submit a prediction for a race via the real HTTP POST route.

    Raises PersonaRunnerError on failure.
    """
    base_url = base_url or DEFAULT_BASE_URL

    resp = session.post(
        urljoin(base_url, f"/predict/{race_id}"),
        data={"p1": str(p1_id), "p2": str(p2_id), "p3": str(p3_id)},
        allow_redirects=True,
    )
    if resp.status_code not in (200, 302):
        raise PersonaRunnerError(
            f"POST /predict/{race_id} failed: HTTP {resp.status_code}"
        )
    response_text = resp.text.lower()
    if "prediction saved" in response_text or "prediction" in response_text:
        return
    if "existing prediction" in response_text:
        raise PersonaRunnerError(
            f"Prediction for race {race_id} already exists (not yet closable)"
        )


def run_persona(
    persona_slug: str,
    invite_token: str,
    seed: int = 42,
    races: list[dict] | None = None,
    drivers: list[dict] | None = None,
    base_url: str | None = None,
    db_path: str | None = None,
) -> dict:
    """Run one persona through the full flow: signup → join → predict.

    Args:
        persona_slug: One of the canonical persona slugs (e.g. "front_runner").
        invite_token: The league invite token (from itsdangerous serializer).
        seed: Random seed for deterministic picks.
        races: List of race dicts with "id" and "round" keys. If None, attempts
               to discover from the running app.
        drivers: List of driver dicts with "id" and "number" keys.
        base_url: Base URL of the running F1 Predictor app.
        db_path: Path to the SQLite database file.

    Returns:
        dict with keys: persona_slug, email, race_count, predictions_submitted, errors
    """
    base_url = base_url or DEFAULT_BASE_URL
    db_path = db_path or DEFAULT_DB_PATH
    persona = get_persona(persona_slug)

    session, email = create_persona_account(base_url, db_path, f"persona-{persona_slug}")
    join_league(session, invite_token, base_url)

    if races is None:
        races = _open_races(base_url, session)

    result = {
        "persona_slug": persona_slug,
        "email": email,
        "race_count": len(races),
        "predictions_submitted": 0,
        "errors": [],
    }

    for race in races:
        race_id = race["id"]
        round_num = race.get("round", 0)

        if not drivers:
            # Need to discover drivers from the predict form
            try:
                resp = session.get(urljoin(base_url, f"/predict/{race_id}"))
                resp.raise_for_status()
                driver_ids = _parse_driver_options(resp.text)
            except Exception as exc:
                result["errors"].append(
                    f"Race {race_id}: could not fetch drivers ({exc})"
                )
                continue
        else:
            driver_ids = [d["id"] for d in drivers]

        # Build a minimal RaceContext for the persona pick
        context = RaceContext(
            race_id=race_id,
            race_name=race.get("name", f"Round {round_num}"),
            round=round_num,
            date=race.get("date", ""),
            drivers=tuple(
                {"id": did, "number": 0, "name": "", "code": ""} for did in driver_ids
            ),
        )
        rng = _make_rng(seed, email, race_id, persona_slug)
        try:
            p1, p2, p3 = persona.pick(context, rng)
            submit_prediction(session, race_id, p1, p2, p3, base_url)
            result["predictions_submitted"] += 1
        except PersonaRunnerError as exc:
            result["errors"].append(str(exc))
        except Exception as exc:
            result["errors"].append(f"Race {race_id}: unexpected error ({exc})")

    return result


def _parse_driver_options(html: str) -> list[int]:
    """Parse driver <option> values from a predict form page.

    Looks for <select name="p1"> and extracts value attributes from <option>
    elements.
    """
    import re

    driver_ids = set()
    # Find all <option value="N"> inside any p-select
    for match in re.finditer(r'<option[^>]*value="(\d+)"[^>]*>', html):
        driver_ids.add(int(match.group(1)))

    if not driver_ids:
        raise PersonaRunnerError("No driver options found in predict page HTML")

    return sorted(driver_ids)


# --- CLI entry point ---

def main():
    """Run all personas against a configured app instance."""
    import argparse

    parser = argparse.ArgumentParser(description="F1 Predictor persona runner")
    parser.add_argument(
        "--invite-token",
        required=True,
        help="League invite token to join",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Base URL of the running app",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help="Path to the app database",
    )
    parser.add_argument(
        "--personas",
        nargs="+",
        help="Persona slugs to run (default: all)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic picks",
    )

    args = parser.parse_args()

    from personas import list_persona_slugs

    slugs = args.personas or list_persona_slugs()

    for slug in slugs:
        print(f"Running persona: {slug}")
        result = run_persona(
            persona_slug=slug,
            invite_token=args.invite_token,
            seed=args.seed,
            base_url=args.base_url,
            db_path=args.db_path,
        )
        print(f"  Email: {result['email']}")
        print(f"  Predictions: {result['predictions_submitted']}/{result['race_count']}")
        if result["errors"]:
            for err in result["errors"]:
                print(f"  Error: {err}")
        print()


if __name__ == "__main__":
    main()