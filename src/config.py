"""Configuration management for the weather pipeline."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """
        Create configuration from environment variables.

        Raises:
            ValueError: If required credentials are not set
        """
        # Get credentials from environment (no defaults for security)
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        database = os.getenv("POSTGRES_DB")

        # Validate required credentials
        if not user:
            raise ValueError("POSTGRES_USER environment variable must be set")
        if not password:
            raise ValueError("POSTGRES_PASSWORD environment variable must be set")
        if not database:
            raise ValueError("POSTGRES_DB environment variable must be set")

        # Optional settings with sensible defaults
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = int(os.getenv("POSTGRES_PORT", "5432"))

        return cls(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )


@dataclass
class APIConfig:
    """BrightSky API configuration."""

    base_url: str
    timeout: int
    retry_attempts: int

    @classmethod
    def from_env(cls) -> "APIConfig":
        """Create configuration from environment variables."""
        return cls(
            base_url=os.getenv("BRIGHTSKY_API_URL", "https://api.brightsky.dev"),
            timeout=int(os.getenv("API_TIMEOUT_SECONDS", "30")),
            retry_attempts=int(os.getenv("API_RETRY_ATTEMPTS", "3")),
        )


@dataclass
class AppConfig:
    """Application configuration."""

    postal_code_filter: Optional[str]
    batch_size: int
    max_workers: int
    forecast_horizon_days: int

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Create configuration from environment variables."""
        postal_filter = os.getenv("POSTAL_CODE_FILTER", "")
        return cls(
            postal_code_filter=postal_filter if postal_filter else None,
            batch_size=int(os.getenv("BATCH_SIZE", "1000")),
            max_workers=int(os.getenv("MAX_WORKERS", "4")),
            forecast_horizon_days=int(os.getenv("FORECAST_HORIZON_DAYS", "10")),
        )


class Config:
    """Main configuration object."""

    def __init__(self) -> None:
        self.database = DatabaseConfig.from_env()
        self.api = APIConfig.from_env()
        self.app = AppConfig.from_env()


# Global config instance
config = Config()