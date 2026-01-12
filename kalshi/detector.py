"""Spike detection algorithms for market activity."""

import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

from .collector import Market
from .config import Config
from .database import MarketSnapshot


class SpikeType(Enum):
    """Types of detected spikes."""

    VOLUME = "volume_spike"
    PRICE = "price_spike"
    SPREAD_COMPRESSION = "spread_compression"


@dataclass
class SpikeEvent:
    """Represents a detected spike event."""

    spike_type: SpikeType
    ticker: str
    title: str
    subtitle: str
    timestamp: datetime
    current_value: float
    previous_value: float
    average_value: float
    threshold: float
    url: str
    extra_info: dict

    def format_message(self) -> str:
        """Format the spike event as a human-readable message."""
        lines = [
            f"{'='*60}",
            f"[{self.spike_type.value.upper().replace('_', ' ')}] {self.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"Market: {self.title}",
        ]
        if self.subtitle:
            lines.append(f"        {self.subtitle}")
        lines.append(f"Ticker: {self.ticker}")
        lines.append(f"Link: {self.url}")
        lines.append("")

        if self.spike_type == SpikeType.VOLUME:
            lines.extend([
                f"Current Volume: {int(self.current_value):,}",
                f"Previous Volume: {int(self.previous_value):,}",
                f"Volume Change: +{int(self.current_value - self.previous_value):,}",
                f"Avg Rate of Change: {self.average_value:.1f}",
                f"Threshold (std devs): {self.threshold:.1f}",
            ])
        elif self.spike_type == SpikeType.PRICE:
            lines.extend([
                f"Current Price: ${self.current_value:.2f}",
                f"Price {self.extra_info.get('window_minutes', 5)} min ago: ${self.previous_value:.2f}",
                f"Price Change: ${self.current_value - self.previous_value:+.2f}",
                f"Threshold: ${self.threshold:.2f}",
            ])
        elif self.spike_type == SpikeType.SPREAD_COMPRESSION:
            lines.extend([
                f"Current Spread: ${self.current_value:.2f}",
                f"Average Spread: ${self.average_value:.2f}",
                f"Compression: {(1 - self.current_value / self.average_value) * 100:.1f}%",
                f"Yes Bid: ${self.extra_info.get('yes_bid', 0):.2f}",
                f"Yes Ask: ${self.extra_info.get('yes_ask', 0):.2f}",
            ])

        lines.append(f"{'='*60}")
        return "\n".join(lines)


class SpikeDetector:
    """Detects volume, price, and spread spikes in market data."""

    def __init__(self, config: Config):
        self.config = config

    def detect_spikes(
        self, market: Market, history: list[MarketSnapshot]
    ) -> list[SpikeEvent]:
        """Detect all types of spikes for a market given its history."""
        events = []

        if len(history) < 2:
            return events

        # Sort history by timestamp (newest first)
        history = sorted(history, key=lambda x: x.timestamp, reverse=True)

        volume_spike = self._detect_volume_spike(market, history)
        if volume_spike:
            events.append(volume_spike)

        price_spike = self._detect_price_spike(market, history)
        if price_spike:
            events.append(price_spike)

        spread_spike = self._detect_spread_compression(market, history)
        if spread_spike:
            events.append(spread_spike)

        return events

    def _detect_volume_spike(
        self, market: Market, history: list[MarketSnapshot]
    ) -> Optional[SpikeEvent]:
        """Detect if current volume increase exceeds threshold."""
        if len(history) < 3:
            return None

        # Calculate rate of change for each interval
        rates_of_change = []
        for i in range(len(history) - 1):
            newer = history[i]
            older = history[i + 1]
            rate = newer.volume - older.volume
            if rate >= 0:  # Only consider increases
                rates_of_change.append(rate)

        if len(rates_of_change) < 2:
            return None

        # Current rate of change (newest interval)
        current_rate = market.volume - history[0].volume

        if current_rate <= 0:
            return None

        # Calculate mean and standard deviation
        mean_rate = statistics.mean(rates_of_change)
        if len(rates_of_change) >= 2:
            try:
                std_rate = statistics.stdev(rates_of_change)
            except statistics.StatisticsError:
                std_rate = 0
        else:
            std_rate = 0

        # Check if current rate exceeds threshold
        if std_rate > 0:
            z_score = (current_rate - mean_rate) / std_rate
            if z_score >= self.config.volume_std_threshold:
                return SpikeEvent(
                    spike_type=SpikeType.VOLUME,
                    ticker=market.ticker,
                    title=market.title,
                    subtitle=market.subtitle,
                    timestamp=datetime.utcnow(),
                    current_value=market.volume,
                    previous_value=history[0].volume,
                    average_value=mean_rate,
                    threshold=self.config.volume_std_threshold,
                    url=market.url,
                    extra_info={"z_score": z_score, "std_rate": std_rate},
                )

        return None

    def _detect_price_spike(
        self, market: Market, history: list[MarketSnapshot]
    ) -> Optional[SpikeEvent]:
        """Detect if price moved more than threshold in the time window."""
        if market.last_price is None:
            return None

        window_cutoff = datetime.utcnow() - timedelta(
            minutes=self.config.price_spike_window_minutes
        )

        # Find the oldest snapshot within the window
        reference_snapshot = None
        for snapshot in history:
            if snapshot.timestamp <= window_cutoff:
                reference_snapshot = snapshot
                break
            reference_snapshot = snapshot  # Keep the oldest we've seen

        if reference_snapshot is None or reference_snapshot.last_price is None:
            return None

        # Convert prices from cents to dollars for comparison
        current_price = market.last_price / 100.0
        reference_price = reference_snapshot.last_price / 100.0
        price_change = abs(current_price - reference_price)

        if price_change >= self.config.price_spike_threshold:
            return SpikeEvent(
                spike_type=SpikeType.PRICE,
                ticker=market.ticker,
                title=market.title,
                subtitle=market.subtitle,
                timestamp=datetime.utcnow(),
                current_value=current_price,
                previous_value=reference_price,
                average_value=reference_price,  # Not really an average
                threshold=self.config.price_spike_threshold,
                url=market.url,
                extra_info={
                    "window_minutes": self.config.price_spike_window_minutes,
                    "direction": "up" if current_price > reference_price else "down",
                },
            )

        return None

    def _detect_spread_compression(
        self, market: Market, history: list[MarketSnapshot]
    ) -> Optional[SpikeEvent]:
        """Detect significant narrowing of bid-ask spread."""
        if market.yes_bid is None or market.yes_ask is None:
            return None

        current_spread = (market.yes_ask - market.yes_bid) / 100.0

        if current_spread <= 0:
            return None

        # Calculate historical spreads
        historical_spreads = []
        for snapshot in history:
            if snapshot.yes_bid is not None and snapshot.yes_ask is not None:
                spread = (snapshot.yes_ask - snapshot.yes_bid) / 100.0
                if spread > 0:
                    historical_spreads.append(spread)

        if len(historical_spreads) < 3:
            return None

        avg_spread = statistics.mean(historical_spreads)

        # Check if current spread is significantly narrower
        compression_ratio = current_spread / avg_spread

        if compression_ratio <= (1 - self.config.spread_compression_threshold):
            return SpikeEvent(
                spike_type=SpikeType.SPREAD_COMPRESSION,
                ticker=market.ticker,
                title=market.title,
                subtitle=market.subtitle,
                timestamp=datetime.utcnow(),
                current_value=current_spread,
                previous_value=historical_spreads[0] if historical_spreads else 0,
                average_value=avg_spread,
                threshold=self.config.spread_compression_threshold,
                url=market.url,
                extra_info={
                    "yes_bid": market.yes_bid / 100.0,
                    "yes_ask": market.yes_ask / 100.0,
                    "compression_ratio": compression_ratio,
                },
            )

        return None
