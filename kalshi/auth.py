"""Authentication module for Kalshi API."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import httpx

from .config import Config


@dataclass
class AuthToken:
    """Represents an authentication token."""

    token: str
    member_id: str
    expires_at: datetime

    def is_expired(self, buffer_seconds: int = 60) -> bool:
        """Check if token is expired or about to expire."""
        return datetime.utcnow() >= self.expires_at - timedelta(seconds=buffer_seconds)


class KalshiAuth:
    """Handles Kalshi API authentication."""

    def __init__(self, config: Config):
        self.config = config
        self._token: Optional[AuthToken] = None
        self._lock = asyncio.Lock()

    async def get_token(self, client: httpx.AsyncClient) -> str:
        """Get a valid authentication token, refreshing if necessary."""
        async with self._lock:
            if self._token is None or self._token.is_expired():
                await self._login(client)
            return self._token.token

    async def _login(self, client: httpx.AsyncClient) -> None:
        """Perform login and store the token."""
        response = await client.post(
            f"{self.config.api_base_url}/login",
            json={
                "email": self.config.kalshi_email,
                "password": self.config.kalshi_password,
            },
        )
        response.raise_for_status()
        data = response.json()

        # Token expires in 30 days, but we'll refresh more frequently
        self._token = AuthToken(
            token=data["token"],
            member_id=data["member_id"],
            expires_at=datetime.utcnow() + timedelta(hours=23),
        )

    async def get_auth_headers(self, client: httpx.AsyncClient) -> dict[str, str]:
        """Get headers with authentication token."""
        token = await self.get_token(client)
        return {"Authorization": f"Bearer {token}"}

    def clear_token(self) -> None:
        """Clear the stored token (useful for handling auth errors)."""
        self._token = None
