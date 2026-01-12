"""Configuration management for the Kalshi scanner."""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    """Scanner configuration with sensible defaults."""

    # Authentication
    kalshi_email: str = field(default_factory=lambda: os.getenv("KALSHI_EMAIL", ""))
    kalshi_password: str = field(default_factory=lambda: os.getenv("KALSHI_PASSWORD", ""))

    # API settings
    api_base_url: str = "https://api.elections.kalshi.com/trade-api/v2"

    # Polling settings
    poll_interval_seconds: int = 60
    max_history_points: int = 100

    # Spike detection thresholds
    volume_std_threshold: float = 2.0  # Standard deviations above average
    price_spike_threshold: float = 0.10  # 10 cents
    price_spike_window_minutes: int = 5
    spread_compression_threshold: float = 0.5  # 50% reduction in spread

    # Alerts
    discord_webhook_url: Optional[str] = field(
        default_factory=lambda: os.getenv("DISCORD_WEBHOOK_URL")
    )

    # Database
    db_path: str = "kalshi_scanner.db"

    def validate(self) -> list[str]:
        """Validate configuration and return list of errors."""
        errors = []
        if not self.kalshi_email:
            errors.append("KALSHI_EMAIL environment variable not set")
        if not self.kalshi_password:
            errors.append("KALSHI_PASSWORD environment variable not set")
        return errors


def load_config(**overrides) -> Config:
    """Load configuration from environment variables with optional overrides."""
    from dotenv import load_dotenv
    load_dotenv()
    return Config(**overrides)
