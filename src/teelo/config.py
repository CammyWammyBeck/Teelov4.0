"""
Configuration management for Teelo.

Uses Pydantic Settings to load configuration from environment variables
with sensible defaults for development. All sensitive values (database URLs,
API keys) should be set via environment variables or .env file.

Usage:
    from teelo.config import settings
    print(settings.database_url)
"""

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Environment variables can be set directly or via a .env file
    in the project root directory.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ==========================================================================
    # Database Configuration
    # ==========================================================================

    # Primary database (self-hosted PostgreSQL on Linux server)
    database_url: str = Field(
        default="postgresql://teelo:teelo_dev@localhost:5432/teelo",
        description="PostgreSQL connection URL for the primary database",
    )

    # Heroku replica (for backup and web API hosting)
    heroku_database_url: Optional[str] = Field(
        default=None,
        description="PostgreSQL connection URL for Heroku replica (optional)",
    )

    # Database pool settings
    db_pool_size: int = Field(
        default=5,
        description="Number of connections to keep in the pool",
    )
    db_max_overflow: int = Field(
        default=10,
        description="Max additional connections beyond pool_size",
    )

    # ==========================================================================
    # Discord Configuration
    # ==========================================================================

    discord_webhook_url: Optional[str] = Field(
        default=None,
        description="Discord webhook URL for sending alerts",
    )

    # ==========================================================================
    # Scraping Configuration
    # ==========================================================================

    scrape_headless: bool = Field(
        default=True,
        description="Run browser in headless mode for scraping",
    )
    scrape_virtual_display: bool = Field(
        default=False,
        description=(
            "Use Xvfb virtual display for headed browser on headless machines. "
            "When True, starts Xvfb automatically so headless=False works "
            "without a physical display. View via noVNC on port 6080."
        ),
    )
    scrape_vnc_port: int = Field(
        default=5900,
        description="VNC server port for virtual display (x11vnc)",
    )
    scrape_novnc_port: int = Field(
        default=6080,
        description="noVNC web port - view browser at http://host:6080/vnc.html",
    )
    scrape_timeout: int = Field(
        default=45000,
        description="Default timeout for page loads in milliseconds (45s for JS-heavy sites)",
    )
    scrape_delay_min: float = Field(
        default=1.0,
        description="Minimum delay between requests (seconds)",
    )
    scrape_delay_max: float = Field(
        default=3.0,
        description="Maximum delay between requests (seconds)",
    )
    scrape_max_retries: int = Field(
        default=3,
        description="Maximum retry attempts for failed scrapes",
    )

    # ==========================================================================
    # ML Configuration
    # ==========================================================================

    model_dir: str = Field(
        default="models",
        description="Directory for storing trained models",
    )

    # Retraining thresholds (see ml/monitor.py for usage)
    ml_accuracy_threshold: float = Field(
        default=0.62,
        description="Retrain if 30-day accuracy falls below this",
    )
    ml_calibration_threshold: float = Field(
        default=0.05,
        description="Retrain if calibration error exceeds this",
    )
    ml_drift_threshold: float = Field(
        default=0.03,
        description="Retrain if accuracy drops by this amount from baseline",
    )
    ml_min_matches_retrain: int = Field(
        default=500,
        description="Minimum new matches before considering retraining",
    )

    # ==========================================================================
    # Player Matching Configuration
    # ==========================================================================

    # Fuzzy matching thresholds for player name matching
    # See players/identity.py for detailed matching logic
    player_exact_match_threshold: float = Field(
        default=0.98,
        description="Auto-match players above this similarity score",
    )
    player_suggestion_threshold: float = Field(
        default=0.85,
        description="Show suggested matches above this score in review queue",
    )

    # ==========================================================================
    # Feature Flags
    # ==========================================================================

    enable_feature_blog: bool = Field(default=True, description="Enable Blog section")
    enable_feature_matches: bool = Field(default=True, description="Enable Matches section")
    enable_feature_rankings: bool = Field(default=False, description="Enable Rankings section")
    enable_feature_players: bool = Field(default=False, description="Enable Players section")
    enable_feature_predictions: bool = Field(default=False, description="Enable Predictions/ML features")

    # ==========================================================================
    # API Configuration
    # ==========================================================================

    api_host: str = Field(
        default="0.0.0.0",
        description="Host to bind the API server to",
    )
    api_port: int = Field(
        default=8000,
        description="Port for the API server",
    )
    api_reload: bool = Field(
        default=False,
        description="Enable auto-reload for development",
    )

    # ==========================================================================
    # Logging Configuration
    # ==========================================================================

    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    log_format: str = Field(
        default="json",
        description="Log format: 'json' for production, 'console' for dev",
    )

    # ==========================================================================
    # Validators
    # ==========================================================================

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Ensure log level is valid."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper_v = v.upper()
        if upper_v not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return upper_v


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Using lru_cache ensures we only load settings once,
    which is important because loading from .env can be slow.
    """
    return Settings()


# Convenience alias for importing
settings = get_settings()
