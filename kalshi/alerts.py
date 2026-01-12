"""Alert system for sending notifications."""

from typing import Optional

import httpx

from .config import Config
from .detector import SpikeEvent, SpikeType


class AlertManager:
    """Manages sending alerts to various destinations."""

    def __init__(self, config: Config):
        self.config = config

    async def send_alert(
        self, event: SpikeEvent, client: Optional[httpx.AsyncClient] = None
    ) -> None:
        """Send an alert for a spike event."""
        # Always print to console
        self._print_console_alert(event)

        # Send to Discord if configured
        if self.config.discord_webhook_url:
            await self._send_discord_alert(event, client)

    def _print_console_alert(self, event: SpikeEvent) -> None:
        """Print alert to console."""
        print(event.format_message())

    async def _send_discord_alert(
        self, event: SpikeEvent, client: Optional[httpx.AsyncClient] = None
    ) -> None:
        """Send alert to Discord webhook."""
        if not self.config.discord_webhook_url:
            return

        # Build Discord embed
        color = self._get_embed_color(event.spike_type)

        embed = {
            "title": f"{self._get_emoji(event.spike_type)} {event.spike_type.value.replace('_', ' ').title()}",
            "description": f"**{event.title}**\n{event.subtitle}" if event.subtitle else f"**{event.title}**",
            "color": color,
            "fields": self._build_embed_fields(event),
            "timestamp": event.timestamp.isoformat(),
            "footer": {"text": f"Ticker: {event.ticker}"},
        }

        payload = {
            "embeds": [embed],
            "content": f"[View Market]({event.url})",
        }

        try:
            if client:
                await client.post(self.config.discord_webhook_url, json=payload)
            else:
                async with httpx.AsyncClient() as new_client:
                    await new_client.post(self.config.discord_webhook_url, json=payload)
        except Exception as e:
            print(f"Failed to send Discord alert: {e}")

    def _get_emoji(self, spike_type: SpikeType) -> str:
        """Get emoji for spike type."""
        return {
            SpikeType.VOLUME: "📈",
            SpikeType.PRICE: "💰",
            SpikeType.SPREAD_COMPRESSION: "🎯",
        }.get(spike_type, "⚠️")

    def _get_embed_color(self, spike_type: SpikeType) -> int:
        """Get Discord embed color for spike type."""
        return {
            SpikeType.VOLUME: 0x00FF00,  # Green
            SpikeType.PRICE: 0xFFAA00,   # Orange
            SpikeType.SPREAD_COMPRESSION: 0x0099FF,  # Blue
        }.get(spike_type, 0xFFFFFF)

    def _build_embed_fields(self, event: SpikeEvent) -> list[dict]:
        """Build embed fields based on spike type."""
        fields = []

        if event.spike_type == SpikeType.VOLUME:
            fields = [
                {
                    "name": "Current Volume",
                    "value": f"{int(event.current_value):,}",
                    "inline": True,
                },
                {
                    "name": "Previous Volume",
                    "value": f"{int(event.previous_value):,}",
                    "inline": True,
                },
                {
                    "name": "Volume Change",
                    "value": f"+{int(event.current_value - event.previous_value):,}",
                    "inline": True,
                },
                {
                    "name": "Avg Rate of Change",
                    "value": f"{event.average_value:.1f}",
                    "inline": True,
                },
            ]
        elif event.spike_type == SpikeType.PRICE:
            direction = event.extra_info.get("direction", "")
            arrow = "⬆️" if direction == "up" else "⬇️"
            fields = [
                {
                    "name": "Current Price",
                    "value": f"${event.current_value:.2f}",
                    "inline": True,
                },
                {
                    "name": f"Price {event.extra_info.get('window_minutes', 5)} min ago",
                    "value": f"${event.previous_value:.2f}",
                    "inline": True,
                },
                {
                    "name": "Change",
                    "value": f"{arrow} ${abs(event.current_value - event.previous_value):.2f}",
                    "inline": True,
                },
            ]
        elif event.spike_type == SpikeType.SPREAD_COMPRESSION:
            fields = [
                {
                    "name": "Current Spread",
                    "value": f"${event.current_value:.2f}",
                    "inline": True,
                },
                {
                    "name": "Average Spread",
                    "value": f"${event.average_value:.2f}",
                    "inline": True,
                },
                {
                    "name": "Compression",
                    "value": f"{(1 - event.current_value / event.average_value) * 100:.1f}%",
                    "inline": True,
                },
                {
                    "name": "Bid/Ask",
                    "value": f"${event.extra_info.get('yes_bid', 0):.2f} / ${event.extra_info.get('yes_ask', 0):.2f}",
                    "inline": True,
                },
            ]

        return fields
