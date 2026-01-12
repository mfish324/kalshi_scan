"""Main scanner logic that ties together all components."""

import asyncio
from datetime import datetime
from typing import Optional

import httpx

from .alerts import AlertManager
from .auth import KalshiAuth
from .collector import Market, MarketCollector
from .config import Config
from .database import Database
from .detector import SpikeDetector


class MarketScanner:
    """Main scanner that monitors markets for activity spikes."""

    def __init__(self, config: Config):
        self.config = config
        self.auth = KalshiAuth(config)
        self.database = Database(config)
        self.collector = MarketCollector(config, self.auth, self.database)
        self.detector = SpikeDetector(config)
        self.alerts = AlertManager(config)
        self._running = False

    async def start(self) -> None:
        """Start the scanner."""
        await self.database.connect()
        self._running = True

        print(f"Kalshi Market Scanner Started")
        print(f"Poll interval: {self.config.poll_interval_seconds}s")
        print(f"Volume threshold: {self.config.volume_std_threshold} std devs")
        print(f"Price threshold: ${self.config.price_spike_threshold:.2f} in {self.config.price_spike_window_minutes} min")
        print(f"Spread compression threshold: {self.config.spread_compression_threshold * 100:.0f}%")
        print(f"Discord alerts: {'Enabled' if self.config.discord_webhook_url else 'Disabled'}")
        print("-" * 60)

        async with httpx.AsyncClient(timeout=30.0) as client:
            while self._running:
                try:
                    await self._poll_cycle(client)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 401:
                        print(f"[{datetime.utcnow().isoformat()}] Auth error, clearing token...")
                        self.auth.clear_token()
                    else:
                        print(f"[{datetime.utcnow().isoformat()}] HTTP error: {e}")
                except Exception as e:
                    print(f"[{datetime.utcnow().isoformat()}] Error: {e}")

                if self._running:
                    await asyncio.sleep(self.config.poll_interval_seconds)

    async def stop(self) -> None:
        """Stop the scanner."""
        self._running = False
        await self.database.close()

    async def _poll_cycle(self, client: httpx.AsyncClient) -> None:
        """Execute a single poll cycle."""
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] Polling markets...", end=" ", flush=True)

        # Collect current market data
        markets = await self.collector.collect_and_store(client)
        print(f"Found {len(markets)} active markets")

        # Get all historical data for spike detection
        histories = await self.database.get_all_market_histories(
            self.config.max_history_points
        )

        # Check each market for spikes
        spike_count = 0
        for market in markets:
            history = histories.get(market.ticker, [])
            spikes = self.detector.detect_spikes(market, history)

            for spike in spikes:
                spike_count += 1
                await self.alerts.send_alert(spike, client)

        if spike_count > 0:
            print(f"[{timestamp}] Detected {spike_count} spike(s)")

    async def list_markets(self) -> list[Market]:
        """List all active markets."""
        await self.database.connect()

        async with httpx.AsyncClient(timeout=30.0) as client:
            markets = await self.collector.fetch_all_markets(client)

        await self.database.close()
        return markets

    async def get_market_history(self, ticker: str) -> Optional[dict]:
        """Get history for a specific market."""
        await self.database.connect()

        history = await self.database.get_history(ticker, self.config.max_history_points)
        metadata = await self.database.get_market_metadata(ticker)

        await self.database.close()

        if not history and not metadata:
            return None

        return {
            "metadata": metadata,
            "history": history,
        }
