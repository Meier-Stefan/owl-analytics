import time as time_module
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from tests.helpers import TimeTravel
from part1_build_dataset import RateLimiter


class TestRateLimiterAcquire:

    def test_acquire_returns_zero_when_under_limit(self):
        limiter = RateLimiter(max_per_minute=3, window=60.0)
        assert limiter.acquire() == 0.0
        assert limiter.acquire() == 0.0
        assert limiter.acquire() == 0.0

    def test_acquire_does_not_increment_wait_count_when_under_limit(self):
        limiter = RateLimiter(max_per_minute=3, window=60.0)
        limiter.acquire()
        limiter.acquire()
        limiter.acquire()
        assert limiter.wait_count == 0

    def test_acquire_does_not_sleep_when_under_limit(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=3, window=60.0)
            result = limiter.acquire()
        assert result == 0.0

    def test_acquire_returns_positive_when_limit_exceeded(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=3, window=60.0)
            clock._now = 0.0
            limiter.acquire()
            clock._now = 1.0
            limiter.acquire()
            clock._now = 2.0
            limiter.acquire()
            clock._now = 3.0
            result = limiter.acquire()
        assert result > 0.0

    def test_acquire_increments_wait_count_when_limit_exceeded(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=3, window=60.0)
            clock._now = 0.0
            limiter.acquire()
            clock._now = 1.0
            limiter.acquire()
            clock._now = 2.0
            limiter.acquire()
            clock._now = 3.0
            limiter.acquire()
        assert limiter.wait_count == 1

    def test_acquire_sleeps_for_correct_duration_when_limited(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=3, window=60.0)
            clock._now = 0.0
            limiter.acquire()
            clock._now = 1.0
            limiter.acquire()
            clock._now = 2.0
            limiter.acquire()
            clock._now = 3.0
            limiter.acquire()
        expected_sleep = 57.0
        assert clock._now == 3.0 + expected_sleep

    def test_acquire_records_timestamp_after_sleep(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=1, window=10.0)
            clock._now = 0.0
            limiter.acquire()
            clock._now = 5.0
            limiter.acquire()
        assert len(limiter._timestamps) == 2

    def test_acquire_allows_new_request_after_window_expires(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=3, window=60.0)
            clock._now = 0.0
            limiter.acquire()
            clock._now = 1.0
            limiter.acquire()
            clock._now = 2.0
            limiter.acquire()
            clock._now = 62.0
            result = limiter.acquire()
        assert result == 0.0

    def test_acquire_does_not_block_when_old_timestamps_expired(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=3, window=60.0)
            clock._now = 0.0
            limiter.acquire()
            clock._now = 1.0
            limiter.acquire()
            clock._now = 2.0
            limiter.acquire()
            clock._now = 62.0
            limiter.acquire()
        assert limiter.wait_count == 0

    def test_acquire_cleans_expired_timestamps_from_list(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=10, window=60.0)
            clock._now = 0.0
            limiter.acquire()
            clock._now = 61.0
            limiter.acquire()
        assert len(limiter._timestamps) == 1

    def test_acquire_does_not_raise_when_burst_equals_limit(self):
        limiter = RateLimiter(max_per_minute=100, window=60.0)
        for _ in range(100):
            limiter.acquire()
        assert limiter.wait_count == 0

    def test_acquire_accepts_window_from_config(self):
        limiter = RateLimiter(max_per_minute=5, window=30.0)
        assert limiter._window == 30.0

    def test_thread_safety_with_concurrent_requests(self):
        limiter = RateLimiter(max_per_minute=500, window=60.0)
        errors = []

        def worker():
            try:
                for _ in range(10):
                    limiter.acquire()
            except Exception as exc:
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(worker) for _ in range(8)]
            for f in futures:
                f.result()
        assert len(errors) == 0

    def test_thread_safety_wait_count_is_accurate_after_concurrent_use(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=1, window=60.0)
            errors = []

            def worker():
                try:
                    limiter.acquire()
                except Exception as exc:
                    errors.append(exc)

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(worker) for _ in range(4)]
                for f in futures:
                    f.result()
        assert len(errors) == 0
        assert limiter.wait_count == 3

    def test_thread_safety_no_exceptions_with_high_limit(self):
        limiter = RateLimiter(max_per_minute=1000, window=60.0)
        errors = []

        def worker():
            try:
                for _ in range(50):
                    limiter.acquire()
            except Exception as exc:
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(worker) for _ in range(4)]
            for f in futures:
                f.result()
        assert len(errors) == 0

    def test_acquire_releases_lock_during_sleep_allowing_another_thread(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=1, window=60.0)
            clock._now = 0.0
            limiter.acquire()

            blocked_result = []
            def blocked_request():
                clock._now = 5.0
                blocked_result.append(limiter.acquire())

            blocker = ThreadPoolExecutor(max_workers=1)
            future_1 = blocker.submit(blocked_request)
            future_1.result()
        assert blocked_result[0] > 0.0
        assert limiter.wait_count == 1

    def test_acquire_logs_message_when_log_lock_provided_and_blocked(self):
        from threading import Lock
        clock = TimeTravel()
        captured = []
        def fake_log(lock, msg):
            captured.append(msg)
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            import part1_build_dataset as mod
            original = mod.log_message
            mod.log_message = fake_log
            try:
                limiter = RateLimiter(max_per_minute=1, window=60.0, log_lock=Lock())
                clock._now = 0.0
                limiter.acquire()
                clock._now = 5.0
                limiter.acquire()
            finally:
                mod.log_message = original
        assert any("RATE_LIMIT" in msg for msg in captured)

    def test_acquire_handles_max_per_minute_of_one(self):
        clock = TimeTravel()
        with (
            patch.object(time_module, "monotonic", clock.monotonic),
            patch.object(time_module, "sleep", clock.sleep),
        ):
            limiter = RateLimiter(max_per_minute=1, window=60.0)
            clock._now = 0.0
            result_1 = limiter.acquire()
            clock._now = 1.0
            result_2 = limiter.acquire()
        assert result_1 == 0.0
        assert result_2 > 0.0
