"""F1-114 (BUD-172): race-calendar trigger for the persona runner.

Builds a season's trigger calendar straight from OpenF1 session data (via
`openf1.get_race_sessions`) — nothing here is a hardcoded list of races or
dates. For each race it computes a trigger time ~5 minutes before the race
locks (lock = race start, same definition `cron/lock_races.py` uses), and
runs a plain in-process, timestamp-comparison loop that calls back into the
persona runner (BUD-171) when a trigger is due.

Two run modes:
  * wall-clock (default) — sleep-then-check loop, for the live deployment
    where triggers should fire close to their real times.
  * fast-forward (BUD-135) — used during simulated season replays; every
    still-pending trigger fires back-to-back with no sleeping, so a whole
    season plays out in seconds.

No cron, no systemd — this is owned and driven in-process by whatever calls
`run()` / `run_fast_forward()`, for the lifetime of that process.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Optional

import openf1

logger = logging.getLogger(__name__)

# How long before a race locks that its trigger should fire.
DEFAULT_LEAD_TIME_SECONDS = 5 * 60

# Wall-clock mode: how often to check whether the next trigger is due.
DEFAULT_POLL_INTERVAL_SECONDS = 15


@dataclass(frozen=True)
class RaceTrigger:
    """One race's fire time, derived from OpenF1 session data."""

    session_key: int
    round: int
    name: str
    lock_at: datetime      # race start — races lock at this instant
    trigger_at: datetime   # lock_at minus the lead time

    def is_past_lock(self, now: datetime) -> bool:
        return now >= self.lock_at


def _parse_date(date_start: Optional[str]) -> Optional[datetime]:
    if not date_start:
        return None
    try:
        dt = datetime.fromisoformat(date_start.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def build_calendar(
    season: Optional[int] = None,
    db=None,
    lead_time_seconds: int = DEFAULT_LEAD_TIME_SECONDS,
) -> list[RaceTrigger]:
    """
    Build the season's trigger calendar from OpenF1 session data.

    `openf1.get_race_sessions` already excludes cancelled sessions and
    assigns `round`; sessions without a parseable `date_start` are skipped
    since there is nothing to schedule a trigger against.
    """
    sessions = openf1.get_race_sessions(season=season, db=db).data
    triggers = []
    for s in sessions:
        lock_at = _parse_date(s.get("date_start"))
        if lock_at is None:
            logger.warning(
                "race_calendar_trigger: skipping session %s (round %s) — no parseable date_start",
                s.get("session_key"), s.get("round"),
            )
            continue
        triggers.append(
            RaceTrigger(
                session_key=s["session_key"],
                round=s["round"],
                name=s.get("circuit_short_name") or s.get("country_name") or f"Round {s['round']}",
                lock_at=lock_at,
                trigger_at=lock_at - timedelta(seconds=lead_time_seconds),
            )
        )
    triggers.sort(key=lambda t: t.trigger_at)
    return triggers


class RaceCalendarTrigger:
    """
    Simple in-process scheduler: fires `on_trigger(race)` ~5 minutes before
    each race locks.

    Callable by the persona runner (BUD-171) as:

        trigger = RaceCalendarTrigger(on_trigger=persona_runner.run_for_race)
        trigger.run(fast_forward=is_replay)
    """

    def __init__(
        self,
        on_trigger: Callable[[RaceTrigger], None],
        *,
        season: Optional[int] = None,
        db=None,
        lead_time_seconds: int = DEFAULT_LEAD_TIME_SECONDS,
        calendar: Optional[Iterable[RaceTrigger]] = None,
    ):
        self.on_trigger = on_trigger
        self.lead_time_seconds = lead_time_seconds
        self.calendar: list[RaceTrigger] = (
            list(calendar) if calendar is not None
            else build_calendar(season=season, db=db, lead_time_seconds=lead_time_seconds)
        )
        self._fired: set[int] = set()

    def pending(self, now: Optional[datetime] = None) -> list[RaceTrigger]:
        """Triggers that are due (trigger_at <= now) and have not fired yet."""
        now = now or datetime.now(timezone.utc)
        return [
            t for t in self.calendar
            if t.session_key not in self._fired and t.trigger_at <= now
        ]

    def skip_past_lock(self, now: Optional[datetime] = None) -> list[RaceTrigger]:
        """
        Mark races whose lock has already passed as fired without calling
        `on_trigger`, so a runner that starts late (or resumes mid-season)
        doesn't try to submit picks for a race that can no longer accept
        them. Returns what was skipped.
        """
        now = now or datetime.now(timezone.utc)
        skipped = []
        for t in self.calendar:
            if t.session_key in self._fired:
                continue
            if t.is_past_lock(now):
                logger.info(
                    "race_calendar_trigger: skipping round %d (%s) — already past lock at %s",
                    t.round, t.name, t.lock_at.isoformat(),
                )
                self._fired.add(t.session_key)
                skipped.append(t)
        return skipped

    def _fire(self, trigger: RaceTrigger) -> None:
        self._fired.add(trigger.session_key)
        logger.info(
            "race_calendar_trigger: firing round %d (%s), lock at %s",
            trigger.round, trigger.name, trigger.lock_at.isoformat(),
        )
        self.on_trigger(trigger)

    def run_fast_forward(self) -> int:
        """
        BUD-135 fast-forward mode: fire every still-pending trigger back to
        back in calendar order, skipping races already past lock, with no
        wall-clock waiting. Returns the number of triggers fired.
        """
        self.skip_past_lock(datetime.now(timezone.utc))
        fired = 0
        for t in self.calendar:
            if t.session_key in self._fired:
                continue
            self._fire(t)
            fired += 1
        return fired

    def run(
        self,
        *,
        fast_forward: bool = False,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
        max_iterations: Optional[int] = None,
    ) -> int:
        """
        Run the scheduler until every trigger in the calendar has fired.

        `fast_forward=True` fires everything immediately (BUD-135 replay
        mode). Otherwise this is a plain sleep-then-check loop against the
        wall clock — a timestamp-comparison loop, not cron/systemd.

        `max_iterations` bounds the wall-clock loop for tests; production
        callers leave it unset and let it run for the process lifetime.
        """
        if fast_forward:
            return self.run_fast_forward()

        fired = 0
        iterations = 0
        while len(self._fired) < len(self.calendar):
            now = datetime.now(timezone.utc)
            self.skip_past_lock(now)
            for t in self.pending(now):
                self._fire(t)
                fired += 1
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                break
            if len(self._fired) < len(self.calendar):
                time.sleep(poll_interval_seconds)
        return fired
