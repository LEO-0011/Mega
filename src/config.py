"""
Configuration module for the MEGA Telegram Bot.
Loads and validates environment variables.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class Config:
    """Application configuration loaded from environment variables."""
    
    # Telegram
    bot_token: str
    
    # MEGA
    mega_email: str
    mega_password: str
    
    # Storage
    storage_path: Path
    chunk_size_bytes: int
    database_path: Path
    
    # Limits
    telegram_file_limit: int
    max_concurrent_downloads: int
    retry_attempts: int
    retry_delay_seconds: int
    
    # Optional cloud storage
    gdrive_credentials_path: Optional[Path] = None
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None
    s3_bucket: Optional[str] = None


def load_config() -> Config:
    """
    Load configuration from environment variables.
    
    Returns:
        Config: Validated configuration object.
        
    Raises:
        ValueError: If required environment variables are missing.
    """
    
    # Required variables
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise ValueError("BOT_TOKEN environment variable is required")
    
    mega_email = os.getenv("MEGA_EMAIL")
    mega_password = os.getenv("MEGA_PASSWORD")
    if not mega_email or not mega_password:
        raise ValueError("MEGA_EMAIL and MEGA_PASSWORD are required")
    
    # Storage paths
    storage_path = Path(os.getenv("STORAGE_PATH", "/app/downloads"))
    storage_path.mkdir(parents=True, exist_ok=True)
    
    database_path = Path(os.getenv("DATABASE_PATH", "/app/data/bot.db"))
    database_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Chunk size (default 4GB)
    chunk_size_gb = float(os.getenv("CHUNK_SIZE_GB", "4"))
    chunk_size_bytes = int(chunk_size_gb * 1024 * 1024 * 1024)
    
    # Telegram limit (2GB for bots)
    telegram_file_limit = int(os.getenv("TELEGRAM_FILE_LIMIT", "2147483648"))
    
    # Retry settings
    max_concurrent = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
    retry_attempts = int(os.getenv("RETRY_ATTEMPTS", "5"))
    retry_delay = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
    
    # Optional cloud storage
    gdrive_path = os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH")
    gdrive_credentials = Path(gdrive_path) if gdrive_path else None
    
    return Config(
        bot_token=bot_token,
        mega_email=mega_email,
        mega_password=mega_password,
        storage_path=storage_path,
        chunk_size_bytes=chunk_size_bytes,
        database_path=database_path,
        telegram_file_limit=telegram_file_limit,
        max_concurrent_downloads=max_concurrent,
        retry_attempts=retry_attempts,
        retry_delay_seconds=retry_delay,
        gdrive_credentials_path=gdrive_credentials,
        aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        s3_bucket=os.getenv("S3_BUCKET_NAME"),
    )


# Global config instance
config = load_config()
