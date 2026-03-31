"""Time control utilities for testing time-sensitive features.

This module provides the TimeController class for mocking time in tests.
It supports freezing, advancing, and resetting time to test race conditions
and time-based logic.

Usage:
    # Using the fixture (recommended)
    def test_race_locking(app, time_controller):
        time_controller.freeze(datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc))
        # ... test code ...
        time_controller.unfreeze()

    # Using context manager (recommended for automatic cleanup)
    def test_race_locking(app, time_controller):
        with time_controller.frozen(datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)):
            # ... test code ...
        # Automatic unfreeze

    # Advance time
    time_controller.freeze(datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc))
    time_controller.advance(minutes=10)
    time_controller.advance(hours=1)
    time_controller.advance(timedelta(days=1))
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, Union
from unittest.mock import patch

if TYPE_CHECKING:
    pass

# Module path to patch for time access
_TIME_TARGET = "app._now_utc"


class TimeController:
    """Control time for testing time-sensitive features.

    This class provides a simple way to freeze, advance, and reset time
    for testing race conditions and time-based logic. It works with the
    time_controller fixture in conftest.py.

    Attributes:
        _frozen_time: The currently frozen datetime, or None if not frozen.
        _initial_frozen_time: The initial freeze time for reset(), or None.
        _patcher: The active mock patcher, or None if not patching.

    Example:
        controller = TimeController()
        controller.freeze(datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc))
        controller.advance(minutes=10)
        controller.unfreeze()
    """

    def __init__(self, target: str = _TIME_TARGET):
        """Initialize the TimeController.

        Args:
            target: The module path to patch for time access.
                   Defaults to "app._now_utc".
        """
        self._frozen_time: datetime | None = None
        self._initial_frozen_time: datetime | None = None
        self._patcher: patch | None = None
        self._target = target

    @property
    def is_frozen(self) -> bool:
        """Return True if time is currently frozen."""
        return self._frozen_time is not None

    @property
    def frozen_time(self) -> datetime | None:
        """Return the currently frozen time, or None if not frozen."""
        return self._frozen_time

    def freeze(self, dt: datetime) -> None:
        """Freeze time at the specified datetime.

        Args:
            dt: The datetime to freeze time at. Should be timezone-aware
                for consistent behavior.

        Raises:
            ValueError: If dt is None.
        """
        if dt is None:
            raise ValueError("Cannot freeze time with None datetime")

        self._frozen_time = dt
        self._initial_frozen_time = dt  # Store initial time for reset()
        self._stop_any_patcher()
        self._patcher = patch(self._target, return_value=dt)
        self._patcher.start()

    def advance(
        self,
        minutes: int = 0,
        hours: int = 0,
        days: int = 0,
        timedelta_arg: timedelta | None = None,
    ) -> None:
        """Advance the frozen time by the specified amount.

        Can be called with any combination of minutes, hours, days, or
        a single timedelta. All arguments are cumulative.

        Args:
            minutes: Number of minutes to advance (default 0).
            hours: Number of hours to advance (default 0).
            days: Number of days to advance (default 0).
            timedelta_arg: A timedelta to advance by. If provided,
                           it's added to any other time components.

        Raises:
            RuntimeError: If time is not currently frozen.

        Example:
            controller.advance(minutes=10)
            controller.advance(hours=1, minutes=30)
            controller.advance(timedelta(days=1, hours=2))
        """
        if not self.is_frozen:
            raise RuntimeError(
                "Cannot advance time - time is not frozen. Call freeze() first."
            )

        delta = timedelta(minutes=minutes, hours=hours, days=days)
        if timedelta_arg:
            delta += timedelta_arg

        self._frozen_time = self._frozen_time + delta  # type: ignore
        self._stop_any_patcher()
        self._patcher = patch(self._target, return_value=self._frozen_time)
        self._patcher.start()

    def jump_to(self, dt: datetime) -> None:
        """Jump to a specific datetime while frozen.

        Unlike advance() which moves relative to the current frozen time,
        this method sets an absolute target time.

        Args:
            dt: The datetime to jump to.

        Raises:
            RuntimeError: If time is not currently frozen.
            ValueError: If dt is None.
        """
        if not self.is_frozen:
            raise RuntimeError(
                "Cannot jump_to time - time is not frozen. Call freeze() first."
            )
        if dt is None:
            raise ValueError("Cannot jump to None datetime")

        self._frozen_time = dt
        self._stop_any_patcher()
        self._patcher = patch(self._target, return_value=dt)
        self._patcher.start()

    def reset(self) -> None:
        """Reset frozen time to the initial frozen value.

        This is useful when you want to restart a test scenario
        from the beginning without having to re-freeze.

        Raises:
            RuntimeError: If time is not currently frozen.
        """
        if not self.is_frozen:
            raise RuntimeError(
                "Cannot reset time - time is not frozen. Call freeze() first."
            )
        if self._initial_frozen_time is None:
            raise RuntimeError(
                "Cannot reset time - no initial time stored. Call freeze() first."
            )
        # Restore to initial frozen time
        self._frozen_time = self._initial_frozen_time
        self._stop_any_patcher()
        self._patcher = patch(self._target, return_value=self._frozen_time)
        self._patcher.start()

    def unfreeze(self) -> None:
        """Restore normal time flow.

        This method stops patching and allows time to flow normally again.
        It's safe to call even if time is not currently frozen.
        """
        self._stop_any_patcher()
        self._frozen_time = None
        self._initial_frozen_time = None

    def now(self) -> datetime:
        """Return the current frozen time or the real current time.

        Returns:
            The frozen datetime if time is frozen, otherwise the real
            current UTC time.

        Raises:
            RuntimeError: If time is not currently frozen.
        """
        if not self.is_frozen:
            raise RuntimeError(
                "Cannot call now() - time is not frozen. Call freeze() first."
            )
        return self._frozen_time  # type: ignore

    @contextmanager
    def frozen(self, dt: datetime):
        """Context manager to freeze time for a block of code.

        This is the recommended way to use TimeController as it ensures
        automatic cleanup even if an exception is raised.

        Args:
            dt: The datetime to freeze time at.

        Yields:
            The TimeController instance.

        Example:
            with time_controller.frozen(some_datetime):
                # time is frozen here
                do_something()
            # time is unfrozen here automatically

        Equivalent to:
            time_controller.freeze(dt)
            try:
                yield time_controller
            finally:
                time_controller.unfreeze()
        """
        self.freeze(dt)
        try:
            yield self
        finally:
            self.unfreeze()

    def _stop_any_patcher(self) -> None:
        """Stop any active patcher safely."""
        if self._patcher:
            try:
                self._patcher.stop()
            except Exception:
                pass  # Ignore errors during cleanup
            self._patcher = None

    def __repr__(self) -> str:
        status = (
            f"frozen at {self._frozen_time}"
            if self._frozen_time
            else "not frozen"
        )
        return f"TimeController({status})"

    def __del__(self) -> None:
        """Ensure patcher is stopped on cleanup."""
        self._stop_any_patcher()


class MockDateTime:
    """Alternative datetime mock for more complex scenarios.

    This class provides a callable mock that returns a fixed datetime,
    useful for patching datetime.datetime.now() directly.

    Example:
        frozen_now = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        with patch.object(datetime, 'now', return_value=frozen_now):
            # datetime.now() returns frozen_now
            pass
    """

    def __init__(self, frozen_dt: datetime):
        """Initialize with a frozen datetime.

        Args:
            frozen_dt: The datetime to return from now().
        """
        self._frozen_dt = frozen_dt

    def __call__(self, tz: timezone | None = None) -> datetime:
        """Return the frozen datetime.

        Args:
            tz: If provided and different from the frozen datetime's timezone,
                convert accordingly. Otherwise return the frozen datetime as-is.

        Returns:
            The frozen datetime.
        """
        if tz is not None and self._frozen_dt.tzinfo != tz:
            return self._frozen_dt.astimezone(tz)
        return self._frozen_dt
