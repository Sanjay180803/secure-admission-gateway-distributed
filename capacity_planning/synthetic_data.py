"""
Synthetic request-rate generator: produces a realistic daily traffic
pattern (base load + a repeating "rush hour" spike + noise), so we can
train a predictor on "history" and test it on a "new" day -- including a
day with a genuinely unexpected anomaly, to be honest about where
prediction helps and where it can't.
"""
import math
import random


def generate_day(seed: int, anomaly: bool = False, ticks_per_day: int = 1440):
    """
    Returns a list of `ticks_per_day` request-rate values (requests/sec),
    one per simulated minute. Pattern: a low overnight baseline, a morning
    ramp, a repeating midday rush-hour peak, an evening taper.
    `anomaly=True` injects a SURPRISE spike at a random time not tied to
    the usual daily pattern -- this is the case a seasonal predictor can't
    see coming, included deliberately so the demo doesn't overclaim.
    """
    rng = random.Random(seed)
    rates = []
    for t in range(ticks_per_day):
        hour = (t / ticks_per_day) * 24
        # smooth daily curve: low at night, peaks around "midday" (hour 12-14)
        daily = 20 + 60 * math.exp(-((hour - 13) ** 2) / (2 * 3.0 ** 2))
        noise = rng.gauss(0, 3)
        rate = max(2.0, daily + noise)
        rates.append(rate)

    if anomaly:
        # a surprise spike at a random point NOT tied to the daily pattern
        # (e.g. a viral post, a retry storm) -- genuinely unpredictable
        # from time-of-day alone.
        spike_start = rng.randint(100, ticks_per_day - 100)
        spike_len = rng.randint(20, 40)
        spike_height = rng.uniform(150, 250)
        for i in range(spike_start, min(spike_start + spike_len, ticks_per_day)):
            rates[i] += spike_height

    return rates


def generate_history(n_days: int = 5, start_seed: int = 1):
    """Multiple 'past' days with the same recurring pattern, no anomalies --
    this is what the predictor gets to learn from."""
    return [generate_day(seed=start_seed + i, anomaly=False) for i in range(n_days)]
