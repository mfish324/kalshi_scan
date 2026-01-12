#!/usr/bin/env python3
"""
Kalshi Market Activity Scanner

Monitors Kalshi prediction markets for volume spikes, price movements,
and spread compression.

Usage:
    python scanner.py run          Start the scanner
    python scanner.py markets      List all active markets
    python scanner.py history <ticker>  Show history for a market
"""

import asyncio
import signal
import sys
from datetime import datetime

import click

from kalshi.config import load_config
from kalshi.scanner import MarketScanner


@click.group()
@click.option("--db", default="kalshi_scanner.db", help="Database file path")
@click.option("--interval", default=60, type=int, help="Poll interval in seconds")
@click.option("--volume-threshold", default=2.0, type=float, help="Volume spike threshold (std devs)")
@click.option("--price-threshold", default=0.10, type=float, help="Price spike threshold (dollars)")
@click.option("--price-window", default=5, type=int, help="Price spike window (minutes)")
@click.option("--spread-threshold", default=0.5, type=float, help="Spread compression threshold (0-1)")
@click.pass_context
def cli(ctx, db, interval, volume_threshold, price_threshold, price_window, spread_threshold):
    """Kalshi Market Activity Scanner - Monitor markets for unusual activity."""
    ctx.ensure_object(dict)
    ctx.obj["config_overrides"] = {
        "db_path": db,
        "poll_interval_seconds": interval,
        "volume_std_threshold": volume_threshold,
        "price_spike_threshold": price_threshold,
        "price_spike_window_minutes": price_window,
        "spread_compression_threshold": spread_threshold,
    }


@cli.command()
@click.pass_context
def run(ctx):
    """Start the market activity scanner."""
    config = load_config(**ctx.obj["config_overrides"])

    # Validate configuration
    errors = config.validate()
    if errors:
        for error in errors:
            click.echo(f"Error: {error}", err=True)
        click.echo("\nSet environment variables:", err=True)
        click.echo("  export KALSHI_EMAIL=your-email@example.com", err=True)
        click.echo("  export KALSHI_PASSWORD=your-password", err=True)
        click.echo("\nOptionally for Discord alerts:", err=True)
        click.echo("  export DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...", err=True)
        sys.exit(1)

    scanner = MarketScanner(config)

    # Handle graceful shutdown
    def signal_handler(sig, frame):
        click.echo("\nShutting down...")
        asyncio.create_task(scanner.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(scanner.start())
    except KeyboardInterrupt:
        click.echo("\nScanner stopped.")


@cli.command()
@click.pass_context
def markets(ctx):
    """List all active markets on Kalshi."""
    config = load_config(**ctx.obj["config_overrides"])

    errors = config.validate()
    if errors:
        for error in errors:
            click.echo(f"Error: {error}", err=True)
        sys.exit(1)

    scanner = MarketScanner(config)

    async def list_markets():
        return await scanner.list_markets()

    try:
        all_markets = asyncio.run(list_markets())
    except Exception as e:
        click.echo(f"Error fetching markets: {e}", err=True)
        sys.exit(1)

    if not all_markets:
        click.echo("No active markets found.")
        return

    click.echo(f"\nActive Markets ({len(all_markets)} total):\n")
    click.echo(f"{'Ticker':<30} {'Volume':>10} {'Last':>8} {'Bid':>8} {'Ask':>8}")
    click.echo("-" * 70)

    for market in sorted(all_markets, key=lambda m: m.volume, reverse=True):
        last = f"${market.last_price/100:.2f}" if market.last_price else "N/A"
        bid = f"${market.yes_bid/100:.2f}" if market.yes_bid else "N/A"
        ask = f"${market.yes_ask/100:.2f}" if market.yes_ask else "N/A"
        click.echo(f"{market.ticker:<30} {market.volume:>10,} {last:>8} {bid:>8} {ask:>8}")

    click.echo(f"\nTotal: {len(all_markets)} markets")


@cli.command()
@click.argument("ticker")
@click.option("--limit", default=20, type=int, help="Number of history entries to show")
@click.pass_context
def history(ctx, ticker, limit):
    """Show recent activity history for a specific market."""
    config = load_config(**ctx.obj["config_overrides"])

    scanner = MarketScanner(config)

    async def get_history():
        return await scanner.get_market_history(ticker.upper())

    try:
        result = asyncio.run(get_history())
    except Exception as e:
        click.echo(f"Error fetching history: {e}", err=True)
        sys.exit(1)

    if not result:
        click.echo(f"No history found for ticker: {ticker}")
        click.echo("Note: History is only available for markets that have been scanned.")
        return

    metadata = result.get("metadata")
    history_data = result.get("history", [])

    if metadata:
        click.echo(f"\nMarket: {metadata['title']}")
        if metadata.get("subtitle"):
            click.echo(f"        {metadata['subtitle']}")
        click.echo(f"Ticker: {metadata['ticker']}")
        click.echo(f"URL: {metadata['url']}")
        click.echo()

    if not history_data:
        click.echo("No data points recorded yet.")
        return

    click.echo(f"History (last {min(limit, len(history_data))} entries):\n")
    click.echo(f"{'Timestamp':<20} {'Volume':>10} {'Last':>8} {'Bid':>8} {'Ask':>8} {'OI':>10}")
    click.echo("-" * 76)

    for snapshot in history_data[:limit]:
        ts = snapshot.timestamp.strftime("%Y-%m-%d %H:%M")
        last = f"${snapshot.last_price/100:.2f}" if snapshot.last_price else "N/A"
        bid = f"${snapshot.yes_bid/100:.2f}" if snapshot.yes_bid else "N/A"
        ask = f"${snapshot.yes_ask/100:.2f}" if snapshot.yes_ask else "N/A"
        click.echo(f"{ts:<20} {snapshot.volume:>10,} {last:>8} {bid:>8} {ask:>8} {snapshot.open_interest:>10,}")

    click.echo(f"\nTotal data points: {len(history_data)}")

    # Calculate some basic stats
    if len(history_data) >= 2:
        newest = history_data[0]
        oldest = history_data[-1]
        volume_change = newest.volume - oldest.volume
        time_span = newest.timestamp - oldest.timestamp

        click.echo(f"\nStats over {time_span}:")
        click.echo(f"  Volume change: {volume_change:+,}")
        if newest.last_price and oldest.last_price:
            price_change = (newest.last_price - oldest.last_price) / 100
            click.echo(f"  Price change: ${price_change:+.2f}")


if __name__ == "__main__":
    cli()
