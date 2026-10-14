"""Adapted Qbot rules; only CLOSED candles may be passed by the worker.

SMA: qbot/strategies/sma_cross_strategy_bt.py (10/30 crossover).
BOLL: qbot/strategies/boll_strategy_bt.py (13 bars, two std deviations).
Short-side mirroring and contract risk management are additions, not Qbot claims.
"""
from statistics import fmean, pstdev


def crossover(closes, fast=10, slow=30):
    if len(closes) < slow + 2:
        return 0
    # Backtrader CrossOver remembers the last non-zero difference across ties.
    current = fmean(closes[-fast:]) - fmean(closes[-slow:])
    if current == 0:
        return 0
    for end in range(len(closes)-1, slow-1, -1):
        prior = fmean(closes[end-fast:end]) - fmean(closes[end-slow:end])
        if prior:
            return 1 if prior < 0 < current else -1 if prior > 0 > current else 0
    return 0


def signal(closes, amount, cfg):
    if cfg.strategy == 'sma_cross':
        cross = crossover(closes, cfg.fast, cfg.slow)
        if amount > 0 and cross < 0:
            return 'close'
        if amount < 0 and cross > 0:
            return 'close'
        if amount == 0:
            return 'long' if cross > 0 else 'short' if cross < 0 and cfg.allow_short else 'hold'
    elif len(closes) >= cfg.boll_period:
        window = closes[-cfg.boll_period:]
        mean, width = fmean(window), pstdev(window) * cfg.boll_deviation
        if not width:
            return 'hold'
        lower, upper = mean-width, mean+width
        if (amount > 0 and closes[-1] > upper) or (amount < 0 and closes[-1] < lower):
            return 'close'
        if amount == 0:
            return 'long' if closes[-1] < lower else 'short' if closes[-1] > upper and cfg.allow_short else 'hold'
    return 'hold'
