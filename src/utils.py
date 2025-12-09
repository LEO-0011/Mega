"""
Utility functions for the MEGA Telegram bot.
"""

import asyncio
import logging
import humanize
from datetime import datetime, timedelta
from typing import Optional
from functools import wraps

logger = logging.getLogger(__name__)


def format_size(size_bytes: int) -> str:
    """
    Format bytes to human readable size.
    
    Args:
        size_bytes: Size in bytes.
        
    Returns:
        Formatted size string.
    """
    return humanize.naturalsize(size_bytes, binary=True)


def format_duration(seconds: float) -> str:
    """
    Format seconds to human readable duration.
    
    Args:
        seconds: Duration in seconds.
        
    Returns:
        Formatted duration string.
    """
    return humanize.naturaldelta(timedelta(seconds=seconds))


def format_progress_bar(
    current: int,
    total: int,
    length: int = 20
) -> str:
    """
    Create a text-based progress bar.
    
    Args:
        current: Current progress value.
        total: Total value.
        length: Length of the bar in characters.
        
    Returns:
        Progress bar string.
    """
    if total == 0:
        return "░" * length
    
    filled = int(length * current / total)
    bar = "█" * filled + "░" * (length - filled)
    percent = current / total * 100
    
    return f"[{bar}] {percent:.1f}%"


def create_progress_message(
    folder_name: str,
    current_file: str,
    file_progress: int,
    file_total: int,
    chunk_number: int,
    total_chunks: int,
    overall_progress: int,
    overall_total: int,
    speed_bps: float = 0
) -> str:
    """
    Create a formatted progress message for Telegram.
    
    Args:
        folder_name: Name of the MEGA folder.
        current_file: Current file being downloaded.
        file_progress: Bytes downloaded for current file.
        file_total: Total bytes for current file.
        chunk_number: Current chunk number.
        total_chunks: Total number of chunks.
        overall_progress: Total bytes downloaded.
        overall_total: Total bytes to download.
        speed_bps: Download speed in bytes per second.
        
    Returns:
        Formatted message string.
    """
    file_bar = format_progress_bar(file_progress, file_total, 15)
    overall_bar = format_progress_bar(overall_progress, overall_total, 15)
    
    eta = ""
    if speed_bps > 0:
        remaining = overall_total - overall_progress
        eta_seconds = remaining / speed_bps
        eta = f"ETA: {format_duration(eta_seconds)}"
    
    message = f"""
📥 **Downloading: {folder_name}**

📄 Current: `{current_file[:30]}...` if len(current_file) > 30 else current_file
{file_bar}
{format_size(file_progress)} / {format_size(file_total)}

📦 Chunk: {chunk_number + 1}/{total_chunks}

📊 Overall Progress:
{overall_bar}
{format_size(overall_progress)} / {format_size(overall_total)}

⚡ Speed: {format_size(int(speed_bps))}/s
{eta}
"""
    return message.strip()


def create_status_message(
    requests: list,
    user_id: int
) -> str:
    """
    Create a status message showing all active downloads.
    
    Args:
        requests: List of DownloadRequest objects.
        user_id: User ID to filter by.
        
    Returns:
        Formatted status message.
    """
    if not requests:
        return "📭 No active downloads."
    
    lines = ["📋 **Your Active Downloads:**\n"]
    
    for req in requests:
        if req.user_id != user_id:
            continue
        
        status_emoji = {
            'pending': '⏳',
            'downloading': '📥',
            'uploading': '📤',
            'completed': '✅',
            'failed': '❌',
            'paused': '⏸️'
        }.get(req.status, '❓')
        
        progress = 0
        if req.total_size > 0:
            progress = req.downloaded_bytes / req.total_size * 100
        
        lines.append(
            f"{status_emoji} **{req.folder_name}**\n"
            f"   Status: {req.status}\n"
            f"   Progress: {progress:.1f}%\n"
            f"   Chunk: {req.current_chunk + 1}/{req.total_chunks}\n"
        )
    
    return "\n".join(lines)


def validate_mega_link(link: str) -> bool:
    """
    Validate a MEGA folder link format.
    
    Args:
        link: URL to validate.
        
    Returns:
        True if valid MEGA folder link.
    """
    import re
    
    patterns = [
        r'mega\.nz/folder/[^#]+#[^/]+',
        r'mega\.nz/#F![^!]+![^/]+',
        r'mega\.co\.nz/#F![^!]+![^/]+',
    ]
    
    for pattern in patterns:
        if re.search(pattern, link):
            return True
    
    return False


async def run_with_timeout(
    coro,
    timeout: float,
    error_message: str = "Operation timed out"
):
    """
    Run a coroutine with a timeout.
    
    Args:
        coro: Coroutine to run.
        timeout: Timeout in seconds.
        error_message: Error message if timeout occurs.
        
    Returns:
        Result of the coroutine.
        
    Raises:
        asyncio.TimeoutError: If timeout is exceeded.
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(error_message)
        raise


def async_retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying async functions.
    
    Args:
        max_attempts: Maximum number of attempts.
        delay: Initial delay between attempts.
        backoff: Multiplier for delay after each attempt.
        exceptions: Tuple of exceptions to catch.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            f"Attempt {attempt + 1} failed: {e}. "
                            f"Retrying in {current_delay}s..."
                        )
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
            
            raise last_exception
        
        return wrapper
    return decorator


class DownloadSpeedTracker:
    """Track download speed over time."""
    
    def __init__(self, window_size: int = 10):
        """
        Initialize speed tracker.
        
        Args:
            window_size: Number of samples to average.
        """
        self.window_size = window_size
        self.samples = []
        self.last_bytes = 0
        self.last_time = None
    
    def update(self, total_bytes: int) -> float:
        """
        Update with new byte count and return current speed.
        
        Args:
            total_bytes: Total bytes downloaded so far.
            
        Returns:
            Current speed in bytes per second.
        """
        now = datetime.now()
        
        if self.last_time is not None:
            elapsed = (now - self.last_time).total_seconds()
            if elapsed > 0:
                bytes_delta = total_bytes - self.last_bytes
                speed = bytes_delta / elapsed
                
                self.samples.append(speed)
                if len(self.samples) > self.window_size:
                    self.samples.pop(0)
        
        self.last_bytes = total_bytes
        self.last_time = now
        
        if self.samples:
            return sum(self.samples) / len(self.samples)
        return 0
    
    def reset(self):
        """Reset the tracker."""
        self.samples = []
        self.last_bytes = 0
        self.last_time = None
