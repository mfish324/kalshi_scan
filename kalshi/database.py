"""Database module for persisting market data to SQLite."""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import aiosqlite

from .config import Config


@dataclass
class MarketSnapshot:
    """A point-in-time snapshot of market data."""

    ticker: str
    timestamp: datetime
    volume: int
    last_price: Optional[float]
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    open_interest: int
    title: str
    subtitle: str


class Database:
    """Handles SQLite persistence for market data."""

    def __init__(self, config: Config):
        self.config = config
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Connect to the database and create tables if needed."""
        self._db = await aiosqlite.connect(self.config.db_path)
        await self._create_tables()

    async def close(self) -> None:
        """Close the database connection."""
        if self._db:
            await self._db.close()
            self._db = None

    async def _create_tables(self) -> None:
        """Create necessary tables if they don't exist."""
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                volume INTEGER NOT NULL,
                last_price REAL,
                yes_bid REAL,
                yes_ask REAL,
                open_interest INTEGER NOT NULL,
                title TEXT NOT NULL,
                subtitle TEXT
            )
        """)
        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_ticker_timestamp
            ON market_snapshots(ticker, timestamp DESC)
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS market_metadata (
                ticker TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                subtitle TEXT,
                url TEXT,
                last_updated TEXT
            )
        """)
        await self._db.commit()

    async def save_snapshot(self, snapshot: MarketSnapshot) -> None:
        """Save a market snapshot to the database."""
        await self._db.execute(
            """
            INSERT INTO market_snapshots
            (ticker, timestamp, volume, last_price, yes_bid, yes_ask, open_interest, title, subtitle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.ticker,
                snapshot.timestamp.isoformat(),
                snapshot.volume,
                snapshot.last_price,
                snapshot.yes_bid,
                snapshot.yes_ask,
                snapshot.open_interest,
                snapshot.title,
                snapshot.subtitle,
            ),
        )
        await self._db.commit()

    async def save_snapshots_batch(self, snapshots: list[MarketSnapshot]) -> None:
        """Save multiple snapshots in a single transaction."""
        await self._db.executemany(
            """
            INSERT INTO market_snapshots
            (ticker, timestamp, volume, last_price, yes_bid, yes_ask, open_interest, title, subtitle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    s.ticker,
                    s.timestamp.isoformat(),
                    s.volume,
                    s.last_price,
                    s.yes_bid,
                    s.yes_ask,
                    s.open_interest,
                    s.title,
                    s.subtitle,
                )
                for s in snapshots
            ],
        )
        await self._db.commit()

    async def get_history(
        self, ticker: str, limit: int = 100
    ) -> list[MarketSnapshot]:
        """Get recent history for a specific market."""
        async with self._db.execute(
            """
            SELECT ticker, timestamp, volume, last_price, yes_bid, yes_ask,
                   open_interest, title, subtitle
            FROM market_snapshots
            WHERE ticker = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (ticker, limit),
        ) as cursor:
            rows = await cursor.fetchall()

        return [
            MarketSnapshot(
                ticker=row[0],
                timestamp=datetime.fromisoformat(row[1]),
                volume=row[2],
                last_price=row[3],
                yes_bid=row[4],
                yes_ask=row[5],
                open_interest=row[6],
                title=row[7],
                subtitle=row[8] or "",
            )
            for row in rows
        ]

    async def get_all_market_histories(
        self, limit_per_market: int = 100
    ) -> dict[str, list[MarketSnapshot]]:
        """Get recent history for all markets."""
        async with self._db.execute(
            """
            SELECT DISTINCT ticker FROM market_snapshots
            """
        ) as cursor:
            tickers = [row[0] for row in await cursor.fetchall()]

        histories = {}
        for ticker in tickers:
            histories[ticker] = await self.get_history(ticker, limit_per_market)
        return histories

    async def prune_old_data(self, max_points_per_market: int) -> int:
        """Remove old snapshots beyond the maximum history limit. Returns count of deleted rows."""
        # Get tickers with too many snapshots
        async with self._db.execute(
            """
            SELECT ticker, COUNT(*) as cnt
            FROM market_snapshots
            GROUP BY ticker
            HAVING cnt > ?
            """,
            (max_points_per_market,),
        ) as cursor:
            tickers_to_prune = await cursor.fetchall()

        total_deleted = 0
        for ticker, count in tickers_to_prune:
            to_delete = count - max_points_per_market
            await self._db.execute(
                """
                DELETE FROM market_snapshots
                WHERE id IN (
                    SELECT id FROM market_snapshots
                    WHERE ticker = ?
                    ORDER BY timestamp ASC
                    LIMIT ?
                )
                """,
                (ticker, to_delete),
            )
            total_deleted += to_delete

        if total_deleted > 0:
            await self._db.commit()
        return total_deleted

    async def update_market_metadata(
        self, ticker: str, title: str, subtitle: str, url: str
    ) -> None:
        """Update or insert market metadata."""
        await self._db.execute(
            """
            INSERT OR REPLACE INTO market_metadata (ticker, title, subtitle, url, last_updated)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ticker, title, subtitle, url, datetime.utcnow().isoformat()),
        )
        await self._db.commit()

    async def get_market_metadata(self, ticker: str) -> Optional[dict]:
        """Get metadata for a specific market."""
        async with self._db.execute(
            """
            SELECT ticker, title, subtitle, url FROM market_metadata WHERE ticker = ?
            """,
            (ticker,),
        ) as cursor:
            row = await cursor.fetchone()

        if row:
            return {
                "ticker": row[0],
                "title": row[1],
                "subtitle": row[2],
                "url": row[3],
            }
        return None
