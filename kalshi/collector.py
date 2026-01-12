"""Data collection module for fetching market data from Kalshi API."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import httpx

from .auth import KalshiAuth
from .config import Config
from .database import Database, MarketSnapshot


@dataclass
class Market:
    """Represents a Kalshi market."""

    ticker: str
    title: str
    subtitle: str
    status: str
    volume: int
    open_interest: int
    last_price: Optional[float]
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    url: str

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "Market":
        """Create a Market from API response data."""
        ticker = data.get("ticker", "")
        return cls(
            ticker=ticker,
            title=data.get("title", ""),
            subtitle=data.get("subtitle", ""),
            status=data.get("status", ""),
            volume=data.get("volume", 0) or 0,
            open_interest=data.get("open_interest", 0) or 0,
            last_price=data.get("last_price"),
            yes_bid=data.get("yes_bid"),
            yes_ask=data.get("yes_ask"),
            url=f"https://kalshi.com/markets/{ticker.lower()}" if ticker else "",
        )

    def to_snapshot(self) -> MarketSnapshot:
        """Convert to a MarketSnapshot for storage."""
        return MarketSnapshot(
            ticker=self.ticker,
            timestamp=datetime.utcnow(),
            volume=self.volume,
            last_price=self.last_price,
            yes_bid=self.yes_bid,
            yes_ask=self.yes_ask,
            open_interest=self.open_interest,
            title=self.title,
            subtitle=self.subtitle,
        )


class MarketCollector:
    """Collects market data from Kalshi API."""

    def __init__(self, config: Config, auth: KalshiAuth, database: Database):
        self.config = config
        self.auth = auth
        self.database = database

    async def fetch_all_markets(
        self, client: httpx.AsyncClient
    ) -> list[Market]:
        """Fetch all active markets from Kalshi."""
        markets = []
        cursor = None

        while True:
            headers = await self.auth.get_auth_headers(client)
            params = {"status": "open", "limit": 200}
            if cursor:
                params["cursor"] = cursor

            response = await client.get(
                f"{self.config.api_base_url}/markets",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            for market_data in data.get("markets", []):
                market = Market.from_api_response(market_data)
                markets.append(market)

            cursor = data.get("cursor")
            if not cursor:
                break

        return markets

    async def collect_and_store(
        self, client: httpx.AsyncClient
    ) -> list[Market]:
        """Fetch all markets and store snapshots in the database."""
        markets = await self.fetch_all_markets(client)

        # Create snapshots and save in batch
        snapshots = [m.to_snapshot() for m in markets]
        if snapshots:
            await self.database.save_snapshots_batch(snapshots)

        # Update metadata for each market
        for market in markets:
            await self.database.update_market_metadata(
                market.ticker, market.title, market.subtitle, market.url
            )

        # Prune old data
        await self.database.prune_old_data(self.config.max_history_points)

        return markets

    async def fetch_single_market(
        self, client: httpx.AsyncClient, ticker: str
    ) -> Optional[Market]:
        """Fetch a single market by ticker."""
        headers = await self.auth.get_auth_headers(client)

        try:
            response = await client.get(
                f"{self.config.api_base_url}/markets/{ticker}",
                headers=headers,
            )
            response.raise_for_status()
            data = response.json()
            return Market.from_api_response(data.get("market", data))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
