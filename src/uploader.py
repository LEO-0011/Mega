"""
Upload module for sending files to Telegram and cloud storage.
"""

import asyncio
import os
from pathlib import Path
from typing import Optional, Callable, List
import logging
import aiofiles
import aiofiles.os

from telegram import Bot, InputFile
from telegram.error import TelegramError, RetryAfter

from .config import config

logger = logging.getLogger(__name__)


class TelegramUploader:
    """
    Handles uploading files to Telegram.
    
    Features:
    - Upload documents with progress
    - Handle file size limits
    - Retry on rate limits
    - Send status messages
    """
    
    def __init__(self, bot: Bot):
        """
        Initialize uploader.
        
        Args:
            bot: Telegram bot instance.
        """
        self.bot = bot
        self.max_file_size = config.telegram_file_limit
    
    async def upload_file(
        self,
        chat_id: int,
        file_path: Path,
        caption: str = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> bool:
        """
        Upload a file to Telegram.
        
        Args:
            chat_id: Telegram chat ID.
            file_path: Path to file to upload.
            caption: Optional caption for the file.
            progress_callback: Optional callback for progress updates.
            
        Returns:
            bool: True if upload successful.
        """
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return False
        
        file_size = file_path.stat().st_size
        
        if file_size > self.max_file_size:
            logger.warning(f"File too large for Telegram: {file_path}")
            # Try to split the file
            return await self._upload_split_file(
                chat_id, file_path, caption, progress_callback
            )
        
        try:
            async with aiofiles.open(file_path, 'rb') as f:
                file_content = await f.read()
            
            await self.bot.send_document(
                chat_id=chat_id,
                document=InputFile(file_content, filename=file_path.name),
                caption=caption or f"📁 {file_path.name}",
                read_timeout=300,
                write_timeout=300,
                connect_timeout=60
            )
            
            if progress_callback:
                progress_callback(file_size, file_size)
            
            logger.info(f"Successfully uploaded {file_path.name} to chat {chat_id}")
            return True
            
        except RetryAfter as e:
            logger.warning(f"Rate limited, waiting {e.retry_after} seconds")
            await asyncio.sleep(e.retry_after)
            return await self.upload_file(
                chat_id, file_path, caption, progress_callback
            )
            
        except TelegramError as e:
            logger.error(f"Telegram upload error: {e}")
            return False
        
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False
    
    async def _upload_split_file(
        self,
        chat_id: int,
        file_path: Path,
        caption: str = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> bool:
        """
        Split and upload a file that exceeds Telegram's limit.
        
        Args:
            chat_id: Telegram chat ID.
            file_path: Path to large file.
            caption: Optional caption.
            progress_callback: Progress callback.
            
        Returns:
            bool: True if all parts uploaded successfully.
        """
        file_size = file_path.stat().st_size
        part_size = self.max_file_size - (10 * 1024 * 1024)  # Leave 10MB margin
        
        parts = []
        part_num = 0
        total_uploaded = 0
        
        try:
            # Split file into parts
            async with aiofiles.open(file_path, 'rb') as f:
                while True:
                    chunk = await f.read(part_size)
                    if not chunk:
                        break
                    
                    part_path = file_path.with_suffix(f".part{part_num:03d}")
                    async with aiofiles.open(part_path, 'wb') as part_file:
                        await part_file.write(chunk)
                    
                    parts.append(part_path)
                    part_num += 1
            
            # Upload each part
            await self.send_message(
                chat_id,
                f"📦 File too large, splitting into {len(parts)} parts..."
            )
            
            for i, part_path in enumerate(parts):
                part_caption = f"📁 {file_path.name} (Part {i+1}/{len(parts)})"
                if i == 0 and caption:
                    part_caption = f"{caption}\n\n{part_caption}"
                
                success = await self.upload_file(
                    chat_id, part_path, part_caption
                )
                
                if not success:
                    return False
                
                total_uploaded += part_path.stat().st_size
                if progress_callback:
                    progress_callback(total_uploaded, file_size)
                
                # Clean up part file
                await aiofiles.os.remove(part_path)
            
            return True
            
        except Exception as e:
            logger.error(f"Split upload failed: {e}")
            # Clean up any remaining parts
            for part_path in parts:
                if part_path.exists():
                    await aiofiles.os.remove(part_path)
            return False
    
    async def upload_chunk(
        self,
        chat_id: int,
        chunk_path: Path,
        chunk_number: int,
        total_chunks: int,
        folder_name: str,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> bool:
        """
        Upload a chunk zip file with appropriate caption.
        
        Args:
            chat_id: Telegram chat ID.
            chunk_path: Path to chunk zip file.
            chunk_number: Current chunk number.
            total_chunks: Total number of chunks.
            folder_name: Name of the MEGA folder.
            progress_callback: Progress callback.
            
        Returns:
            bool: True if upload successful.
        """
        caption = (
            f"📦 **{folder_name}**\n"
            f"Chunk {chunk_number + 1}/{total_chunks}\n"
            f"Size: {self._format_size(chunk_path.stat().st_size)}"
        )
        
        return await self.upload_file(
            chat_id, chunk_path, caption, progress_callback
        )
    
    async def send_message(
        self,
        chat_id: int,
        text: str,
        parse_mode: str = "Markdown"
    ) -> bool:
        """
        Send a text message.
        
        Args:
            chat_id: Telegram chat ID.
            text: Message text.
            parse_mode: Parse mode for formatting.
            
        Returns:
            bool: True if message sent successfully.
        """
        try:
            await self.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode
            )
            return True
        except TelegramError as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    async def send_progress_update(
        self,
        chat_id: int,
        message_id: int,
        text: str
    ) -> bool:
        """
        Edit an existing message with progress update.
        
        Args:
            chat_id: Telegram chat ID.
            message_id: Message ID to edit.
            text: New message text.
            
        Returns:
            bool: True if update successful.
        """
        try:
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode="Markdown"
            )
            return True
        except TelegramError as e:
            # Ignore "message not modified" errors
            if "not modified" not in str(e).lower():
                logger.error(f"Failed to update message: {e}")
            return False
    
    def _format_size(self, size_bytes: int) -> str:
        """Format bytes to human readable size."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"


class CloudUploader:
    """
    Fallback uploader for cloud storage (Google Drive, S3).
    Used when files exceed Telegram limits.
    """
    
    def __init__(self):
        """Initialize cloud uploader with available backends."""
        self.gdrive_available = config.gdrive_credentials_path is not None
        self.s3_available = (
            config.aws_access_key is not None and
            config.aws_secret_key is not None and
            config.s3_bucket is not None
        )
    
    async def upload_to_gdrive(
        self,
        file_path: Path,
        folder_id: str = None
    ) -> Optional[str]:
        """
        Upload file to Google Drive.
        
        Args:
            file_path: Path to file.
            folder_id: Optional destination folder ID.
            
        Returns:
            Shareable link or None if failed.
        """
        if not self.gdrive_available:
            logger.warning("Google Drive not configured")
            return None
        
        # Implementation would use google-api-python-client
        # This is a placeholder for the actual implementation
        logger.info(f"Would upload {file_path} to Google Drive")
        return None
    
    async def upload_to_s3(
        self,
        file_path: Path,
        key_prefix: str = ""
    ) -> Optional[str]:
        """
        Upload file to AWS S3.
        
        Args:
            file_path: Path to file.
            key_prefix: S3 key prefix.
            
        Returns:
            S3 URL or None if failed.
        """
        if not self.s3_available:
            logger.warning("S3 not configured")
            return None
        
        # Implementation would use boto3
        # This is a placeholder for the actual implementation
        logger.info(f"Would upload {file_path} to S3")
        return None
