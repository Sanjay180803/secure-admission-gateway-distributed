"""
Two ways to decide "what budget should we allow right now":

1. ReactiveController -- same control law as the live system's
   queue_monitor.py/ledger.py: watches MEASURED delay, shrinks the budget
   after delay rises, grows it back after delay falls. Only ever responds
   AFTER a problem is already visible.

2. SeasonalPredictor -- learns a time-of-day profile from historical data
   (average request rate at each time bucket across several past days),
   and sets the budget based on the PREDICTED load for the upcoming
   window, before delay has a chance to rise. This is a "seasonal naive"
   forecast -- one of the simplest legitimate forecasting methods, used
   for real capacity planning when a metric has a strong daily/weekly
   cycle (this traffic pattern does, by construction).

Neither is dressed up as more sophisticated than it is. The predictor
will look great on the recurring daily pattern (that's what it learned)
and will NOT anticipate a genuine anomaly -- that's demonstrated
explicitly in simulate.py rather than hidden.
"""
import statistics

BACKEND_CAPACITY_RPS = 90.0            # the backend's real, FIXED processing capacity --
                                        # independent of budget. Budget only controls how
                                        # much of the incoming traffic gets ADMITTED to
                                        # compete for this fixed capacity.
BACKEND_SERVICE_TIME_S = 1.0 / BACKEND_CAPACITY_RPS  # for the M/M/1 delay formula
ADMISSION_SERVICE_TIME_S = 0.5          # how a "budget" unit (concurrency) converts to an
                                        # admitted-rate limit via Little's Law -- a separate
                                        # knob from the backend's own service time.
MIN_BUDGET = 3
MAX_BUDGET = 100                       # budget/ADMISSION_SERVICE_TIME_S can reach ~200 rps,
                                        # comfortably above backend capacity -- so an
                                        # over-large budget CAN cause real overload, which is
                                        # the point: the controller has to find the right
                                        # level, not just "more is better."
TARGET_DELAY_S = 3 * BACKEND_SERVICE_TIME_S
SAFETY_MARGIN = 1.15        # predictive budget aims 15% above predicted need, as headroom


def budget_for_target_rate(rate_rps: float) -> int:
    """Convert a target admitted-rate into a concurrency budget via Little's Law:
    concurrency = throughput * avg_service_time."""
    budget = rate_rps * ADMISSION_SERVICE_TIME_S
    return int(max(MIN_BUDGET, min(MAX_BUDGET, budget)))


class ReactiveController:
    """Mirrors the live system's actual control law, for a fair comparison."""

    def __init__(self):
        self.ewma_delay = 0.0
        self.budget = MIN_BUDGET  # deliberately conservative cold start -- a real
                                   # system doesn't know the "right" budget on boot
                                   # any more than a fixed-budget system does; the
                                   # only difference is reactive can grow from here.
        self.alpha = 0.3

    def step(self, measured_delay_s: float) -> int:
        self.ewma_delay = self.alpha * measured_delay_s + (1 - self.alpha) * self.ewma_delay
        if self.ewma_delay > TARGET_DELAY_S:
            self.budget = max(MIN_BUDGET, int(self.budget * 0.7))
        else:
            self.budget = min(MAX_BUDGET, int(self.budget * 1.15) + 1)  # multiplicative growth --
            # additive-only growth (+2/tick) would take too long to reach a useful
            # budget from a conservative cold start; real systems ramp up faster
            # than they back off, but still bounded (not an instant jump).
        return self.budget


class SeasonalPredictor:
    def __init__(self, history_days: list[list[float]], bucket_minutes: int = 10):
        """history_days: list of past days' per-minute rate series."""
        self.bucket_minutes = bucket_minutes
        self.ticks_per_day = len(history_days[0])
        self.n_buckets = self.ticks_per_day // bucket_minutes
        self._profile = self._build_profile(history_days)

    def _build_profile(self, history_days):
        """Average rate per time-of-day bucket across all historical days."""
        bucket_sums = [0.0] * self.n_buckets
        bucket_counts = [0] * self.n_buckets
        for day in history_days:
            for t, rate in enumerate(day):
                b = min(t // self.bucket_minutes, self.n_buckets - 1)
                bucket_sums[b] += rate
                bucket_counts[b] += 1
        return [s / c if c else 0.0 for s, c in zip(bucket_sums, bucket_counts)]

    def predict(self, t: int, recent_actuals: list[float]) -> float:
        """
        Predicted rate for time t = seasonal profile at this time-of-day,
        blended with a short-term trend correction from recent actual
        observations (so a day that's running consistently hotter/cooler
        than history still gets partially corrected for, not just blind
        seasonal repetition).
        """
        bucket = min(t // self.bucket_minutes, self.n_buckets - 1)
        seasonal = self._profile[bucket]
        if recent_actuals:
            recent_avg = statistics.mean(recent_actuals[-10:])
            recent_seasonal_bucket = min(max(0, t - len(recent_actuals[-10:])) // self.bucket_minutes,
                                          self.n_buckets - 1)
            recent_seasonal_expected = self._profile[recent_seasonal_bucket]
            trend_correction = recent_avg - recent_seasonal_expected
        else:
            trend_correction = 0.0
        return max(0.0, seasonal + 0.5 * trend_correction)

    def budget_at(self, t: int, recent_actuals: list[float]) -> int:
        predicted_rate = self.predict(t, recent_actuals)
        target_rate = predicted_rate * SAFETY_MARGIN
        return budget_for_target_rate(target_rate)


class HybridController:
    """
    Uses the seasonal prediction as the BASELINE ("we expect roughly this
    much load right now"), then still applies a bounded reactive
    correction on top based on the actually-measured delay -- this is
    the fix for pure prediction's real weakness (shown in simulate.py):
    a prediction that's even slightly off gets committed to for the
    whole window with no self-correction. The reactive correction here
    is deliberately gentler than the standalone ReactiveController's,
    since the predictive baseline should already be close.
    """

    def __init__(self, predictor: "SeasonalPredictor"):
        self.predictor = predictor
        self.ewma_delay = 0.0
        self.alpha = 0.3
        self.correction_factor = 1.0

    def step(self, t: int, recent_actuals: list[float], measured_delay_s: float) -> int:
        self.ewma_delay = self.alpha * measured_delay_s + (1 - self.alpha) * self.ewma_delay
        if self.ewma_delay > TARGET_DELAY_S:
            self.correction_factor = max(0.5, self.correction_factor * 0.85)
        else:
            self.correction_factor = min(1.3, self.correction_factor * 1.03)

        base_budget = self.predictor.budget_at(t, recent_actuals)
        return int(max(MIN_BUDGET, min(MAX_BUDGET, base_budget * self.correction_factor)))
