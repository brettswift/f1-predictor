"""Tests for time control utilities.

These tests verify the TimeController class works correctly for:
- Freezing time at a specific datetime
- Advancing frozen time
- Jumping to specific times
- Resetting to initial frozen time
- Context manager usage
- Automatic cleanup with unfreeze()
"""

import pytest
from datetime import datetime, timezone, timedelta

from tests.utils.time_control import TimeController


class TestTimeController:
    """Test cases for TimeController."""

    def test_freeze_sets_time(self):
        """Test that freeze() sets the frozen time."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)

        assert controller.is_frozen is True
        assert controller.frozen_time == freeze_time

    def test_freeze_replaces_previous_freeze(self):
        """Test that calling freeze() twice replaces the previous freeze."""
        controller = TimeController()
        time1 = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        time2 = datetime(2026, 3, 24, 10, 0, 0, tzinfo=timezone.utc)

        controller.freeze(time1)
        controller.freeze(time2)

        assert controller.frozen_time == time2

    def test_freeze_none_raises_error(self):
        """Test that freeze() raises ValueError for None."""
        controller = TimeController()

        with pytest.raises(ValueError, match="Cannot freeze time with None"):
            controller.freeze(None)

    def test_advance_minutes(self):
        """Test advancing time by minutes."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)
        controller.advance(minutes=30)

        expected = freeze_time + timedelta(minutes=30)
        assert controller.frozen_time == expected

    def test_advance_hours(self):
        """Test advancing time by hours."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)
        controller.advance(hours=2)

        expected = freeze_time + timedelta(hours=2)
        assert controller.frozen_time == expected

    def test_advance_days(self):
        """Test advancing time by days."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)
        controller.advance(days=5)

        expected = freeze_time + timedelta(days=5)
        assert controller.frozen_time == expected

    def test_advance_combined(self):
        """Test advancing time with multiple parameters."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)
        controller.advance(days=1, hours=3, minutes=45)

        expected = freeze_time + timedelta(days=1, hours=3, minutes=45)
        assert controller.frozen_time == expected

    def test_advance_with_timedelta(self):
        """Test advancing time with a timedelta."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)
        controller.advance(timedelta_arg=timedelta(hours=5, minutes=30))

        expected = freeze_time + timedelta(hours=5, minutes=30)
        assert controller.frozen_time == expected

    def test_advance_cumulative_with_timedelta(self):
        """Test that timedelta_arg is cumulative with other params."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)
        controller.advance(minutes=30, timedelta_arg=timedelta(hours=1))

        expected = freeze_time + timedelta(hours=1, minutes=30)
        assert controller.frozen_time == expected

    def test_advance_when_not_frozen_raises_error(self):
        """Test that advance() raises RuntimeError when not frozen."""
        controller = TimeController()

        with pytest.raises(RuntimeError, match="Cannot advance time"):
            controller.advance(minutes=10)

    def test_jump_to(self):
        """Test jumping to a specific time."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        jump_time = datetime(2026, 4, 1, 8, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)
        controller.advance(days=1)  # Now at April 24
        controller.jump_to(jump_time)  # Jump to April 1

        assert controller.frozen_time == jump_time

    def test_jump_to_when_not_frozen_raises_error(self):
        """Test that jump_to() raises RuntimeError when not frozen."""
        controller = TimeController()

        with pytest.raises(RuntimeError, match="Cannot jump_to time"):
            controller.jump_to(datetime.now(timezone.utc))

    def test_jump_to_none_raises_error(self):
        """Test that jump_to() raises ValueError for None."""
        controller = TimeController()
        controller.freeze(datetime.now(timezone.utc))

        with pytest.raises(ValueError, match="Cannot jump to None"):
            controller.jump_to(None)

    def test_reset(self):
        """Test resetting to initial frozen time."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)
        controller.advance(hours=5)
        controller.advance(days=2)  # Now far from initial
        controller.reset()  # Should go back to initial freeze_time

        assert controller.frozen_time == freeze_time

    def test_reset_when_not_frozen_raises_error(self):
        """Test that reset() raises RuntimeError when not frozen."""
        controller = TimeController()

        with pytest.raises(RuntimeError, match="Cannot reset time"):
            controller.reset()

    def test_unfreeze(self):
        """Test that unfreeze() stops patching."""
        controller = TimeController()
        controller.freeze(datetime.now(timezone.utc))

        assert controller.is_frozen is True

        controller.unfreeze()

        assert controller.is_frozen is False
        assert controller.frozen_time is None

    def test_unfreeze_when_not_frozen_is_safe(self):
        """Test that unfreeze() doesn't raise when not frozen."""
        controller = TimeController()

        # Should not raise
        controller.unfreeze()

        assert controller.is_frozen is False

    def test_unfreeze_called_multiple_times_is_safe(self):
        """Test that multiple unfreeze() calls don't raise."""
        controller = TimeController()
        controller.freeze(datetime.now(timezone.utc))
        controller.unfreeze()

        # Should not raise
        controller.unfreeze()

        assert controller.is_frozen is False

    def test_now_returns_frozen_time(self):
        """Test that now() returns the frozen time."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)

        assert controller.now() == freeze_time

    def test_now_after_advance_returns_advanced_time(self):
        """Test that now() returns advanced time after advance()."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        controller.freeze(freeze_time)
        controller.advance(hours=1)

        assert controller.now() == freeze_time + timedelta(hours=1)

    def test_now_when_not_frozen_raises_error(self):
        """Test that now() raises RuntimeError when not frozen."""
        controller = TimeController()

        with pytest.raises(RuntimeError, match="Cannot call now"):
            controller.now()


class TestTimeControllerContextManager:
    """Test cases for TimeController context manager usage."""

    def test_context_manager_freezes_time(self):
        """Test that context manager freezes time."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        with controller.frozen(freeze_time):
            assert controller.is_frozen is True
            assert controller.frozen_time == freeze_time

    def test_context_manager_unfreezes_after_block(self):
        """Test that context manager unfreezes after block exits."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        with controller.frozen(freeze_time):
            pass

        assert controller.is_frozen is False

    def test_context_manager_unfreezes_on_exception(self):
        """Test that context manager unfreezes even on exception."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)

        with pytest.raises(ValueError):
            with controller.frozen(freeze_time):
                raise ValueError("Test exception")

        assert controller.is_frozen is False

    def test_nested_freeze_and_context_manager(self):
        """Test nesting freeze() call with context manager usage."""
        controller = TimeController()
        time1 = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        time2 = datetime(2026, 3, 24, 10, 0, 0, tzinfo=timezone.utc)

        controller.freeze(time1)
        with controller.frozen(time2):
            # Context manager should replace the freeze
            assert controller.frozen_time == time2
        # After context manager, should be unfrozen
        assert controller.is_frozen is False


class TestTimeControllerRepr:
    """Test cases for TimeController string representation."""

    def test_repr_when_frozen(self):
        """Test __repr__ when frozen."""
        controller = TimeController()
        freeze_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        controller.freeze(freeze_time)

        repr_str = repr(controller)

        assert "TimeController" in repr_str
        assert "frozen" in repr_str

    def test_repr_when_not_frozen(self):
        """Test __repr__ when not frozen."""
        controller = TimeController()

        repr_str = repr(controller)

        assert "TimeController" in repr_str
        assert "not frozen" in repr_str


class TestMockDateTime:
    """Test cases for MockDateTime helper class."""

    def test_returns_frozen_datetime(self):
        """Test that MockDateTime returns the frozen datetime."""
        from tests.utils.time_control import MockDateTime

        frozen = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        mock = MockDateTime(frozen)

        assert mock() == frozen

    def test_timezone_conversion(self):
        """Test timezone conversion when tz differs."""
        from tests.utils.time_control import MockDateTime

        utc_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        mock = MockDateTime(utc_time)

        # Without tz argument, returns as-is
        assert mock() == utc_time
