"""Count-based circuit breaker (same model as Resilience4j's COUNT_BASED window).

    CLOSED    --(>= failure_threshold failures in the last window_size calls)--> OPEN
    OPEN      --(open_wait seconds have passed)------------------------------> HALF_OPEN
    HALF_OPEN --(trial call succeeds)-----------------------------------------> CLOSED
    HALF_OPEN --(trial call fails)--------------------------------------------> OPEN

CLOSED forwards every call. OPEN rejects every call without running it (fail fast).
HALF_OPEN lets exactly one trial call through and rejects the rest until it finishes.
"""
import time
from collections import deque
from enum import Enum
from typing import Awaitable, Callable, TypeVar

from .config import log

T = TypeVar("T")


class State(Enum):
    CLOSED = 0
    OPEN = 1
    HALF_OPEN = 2


class CircuitOpenError(Exception):
    """Raised instead of making the call when the breaker won't allow it."""

    def __init__(self, name: str, state: State, retry_after: float):
        super().__init__(f"Circuit '{name}' is {state.name} - call rejected")
        self.state = state
        self.retry_after = retry_after


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        window_size: int = 10,
        failure_threshold: int = 5,
        open_wait: float = 10.0,
        is_failure: Callable[[BaseException], bool] = lambda exc: True,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.open_wait = open_wait
        self.is_failure = is_failure
        self._window: deque[bool] = deque(maxlen=window_size)  # True = call failed
        self._state = State.CLOSED
        self._opened_at = 0.0
        self._trial_in_flight = False
        # Bumped on every state change. A call only counts if the breaker is still in
        # the state that admitted it - e.g. a slow call that started while CLOSED but
        # finishes after the circuit opened must not count again or be taken as the trial.
        self._generation = 0

    @property
    def state(self) -> State:
        # OPEN -> HALF_OPEN happens on its own once the wait is over, so anyone
        # reading the state (a request, /circuit, the metrics scrape) sees it.
        if self._state is State.OPEN and time.monotonic() - self._opened_at >= self.open_wait:
            self._transition(State.HALF_OPEN, f"{self.open_wait:.0f}s wait elapsed")
        return self._state

    @property
    def failures_in_window(self) -> int:
        return sum(self._window)

    async def call(self, fn: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        self._acquire_permission()
        generation = self._generation
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            if generation == self._generation:
                # Errors that don't mean "the service is unhealthy" (e.g. 401, 404) still
                # prove it answered, so they count as successes for the breaker.
                if self.is_failure(exc):
                    self._record_failure(exc)
                else:
                    self._record_success()
            raise
        except BaseException:
            # Cancelled mid-call: neither outcome, but free the trial slot.
            if generation == self._generation:
                self._trial_in_flight = False
            raise
        if generation == self._generation:
            self._record_success()
        return result

    # --- internals -----------------------------------------------------------
    def _acquire_permission(self) -> None:
        state = self.state
        if state is State.OPEN:
            remaining = self.open_wait - (time.monotonic() - self._opened_at)
            log.info("Circuit '%s' OPEN - rejecting call (half-open in %.1fs)", self.name, remaining)
            raise CircuitOpenError(self.name, state, remaining)
        if state is State.HALF_OPEN:
            if self._trial_in_flight:
                log.info("Circuit '%s' HALF_OPEN - trial already in flight, rejecting call", self.name)
                raise CircuitOpenError(self.name, state, 1.0)
            self._trial_in_flight = True
            log.info("Circuit '%s' HALF_OPEN - letting trial call through", self.name)

    def _record_success(self) -> None:
        if self._state is State.HALF_OPEN:
            self._transition(State.CLOSED, "trial call succeeded")
        else:
            self._window.append(False)

    def _record_failure(self, exc: BaseException) -> None:
        if self._state is State.HALF_OPEN:
            self._transition(State.OPEN, f"trial call failed ({type(exc).__name__})")
            return
        self._window.append(True)
        log.info(
            "Circuit '%s' CLOSED - failure recorded (%d/%d failures in last %d calls)",
            self.name, self.failures_in_window, self.failure_threshold, len(self._window),
        )
        if self.failures_in_window >= self.failure_threshold:
            self._transition(
                State.OPEN,
                f"{self.failures_in_window} failures in last {len(self._window)} calls",
            )

    def _transition(self, new: State, reason: str) -> None:
        log.warning("Circuit '%s' STATE CHANGE: %s -> %s (%s)", self.name, self._state.name, new.name, reason)
        self._state = new
        self._generation += 1
        self._trial_in_flight = False
        self._window.clear()
        if new is State.OPEN:
            self._opened_at = time.monotonic()
