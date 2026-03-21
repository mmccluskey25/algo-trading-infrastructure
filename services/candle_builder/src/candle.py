from dataclasses import dataclass
from datetime import datetime


@dataclass
class Candle:
    """A completed OHLC candle."""

    instrument: str
    bucket: datetime  # start of the minute
    bid_open: float
    bid_high: float
    bid_low: float
    bid_close: float
    ask_open: float
    ask_high: float
    ask_low: float
    ask_close: float
    mid_open: float
    mid_high: float
    mid_low: float
    mid_close: float
    tick_count: int


@dataclass
class _BucketState:
    """Tracks OHLC values for a single in-progress minute bucket."""

    bucket: datetime
    bid_open: float = 0.0
    bid_high: float = float("-inf")
    bid_low: float = float("inf")
    bid_close: float = 0.0
    ask_open: float = 0.0
    ask_high: float = float("-inf")
    ask_low: float = float("inf")
    ask_close: float = 0.0
    mid_open: float = 0.0
    mid_high: float = float("-inf")
    mid_low: float = float("inf")
    mid_close: float = 0.0
    tick_count: int = 0

    def update(self, bid: float, ask: float, mid: float) -> None:
        if self.tick_count == 0:
            self.bid_open = bid
            self.ask_open = ask
            self.mid_open = mid

        self.bid_high = max(self.bid_high, bid)
        self.bid_low = min(self.bid_low, bid)
        self.bid_close = bid

        self.ask_high = max(self.ask_high, ask)
        self.ask_low = min(self.ask_low, ask)
        self.ask_close = ask

        self.mid_high = max(self.mid_high, mid)
        self.mid_low = min(self.mid_low, mid)
        self.mid_close = mid

        self.tick_count += 1

    def to_candle(self, instrument: str) -> Candle:
        return Candle(
            instrument=instrument,
            bucket=self.bucket,
            bid_open=self.bid_open,
            bid_high=self.bid_high,
            bid_low=self.bid_low,
            bid_close=self.bid_close,
            ask_open=self.ask_open,
            ask_high=self.ask_high,
            ask_low=self.ask_low,
            ask_close=self.ask_close,
            mid_open=self.mid_open,
            mid_high=self.mid_high,
            mid_low=self.mid_low,
            mid_close=self.mid_close,
            tick_count=self.tick_count,
        )


class CandleAccumulator:
    """Accumulates ticks into 1-minute OHLC candles, per instrument.

    Feed ticks via on_tick(). When a tick arrives for a new minute bucket,
    the previous bucket is emitted as a completed Candle.
    """

    def __init__(self):
        # instrument -> current bucket state
        self._state: dict[str, _BucketState] = {}

    @staticmethod
    def _parse_tick(tick: dict) -> tuple[str, datetime, float, float, float] | None:
        """Extract instrument, timestamp, bid, ask, mid from a raw tick dict.

        Returns None if the tick should be skipped (non-tradeable or malformed).
        """
        if tick.get("status") != "tradeable":
            return None

        try:
            instrument = tick["instrument"]
            # Oanda format: 2026-03-20T14:30:01.234567890Z
            time_str = tick["time"]

            tick_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))

            bid = float(tick["bids"][0]["price"])
            ask = float(tick["asks"][0]["price"])
            mid = (bid + ask) / 2

            return instrument, tick_time, bid, ask, mid
        except (KeyError, IndexError, ValueError):
            return None

    @staticmethod
    def _to_bucket(tick_time: datetime) -> datetime:
        """Floor a timestamp to the start of its minute."""
        return tick_time.replace(second=0, microsecond=0)

    def on_tick(self, tick: dict) -> Candle | None:
        """Process a tick. Returns a completed Candle if a bucket boundary was crossed."""
        parsed = self._parse_tick(tick)
        if parsed is None:
            return None

        instrument, tick_time, bid, ask, mid = parsed
        bucket = self._to_bucket(tick_time)

        current = self._state.get(instrument)

        completed = None
        if current is not None and current.bucket != bucket:
            # New bucket - emit the previous candle
            completed = current.to_candle(instrument)

        if current is None or current.bucket != bucket:
            # Start a fresh bucket
            self._state[instrument] = _BucketState(bucket=bucket)

        self._state[instrument].update(bid, ask, mid)

        return completed

    def current_candle(self, instrument: str) -> Candle | None:
        """Return the in-progress candle for an instrument, or None."""
        state = self._state.get(instrument)
        if state is None or state.tick_count == 0:
            return None
        return state.to_candle(instrument)
