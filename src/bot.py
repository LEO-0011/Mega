"""
Main Telegram bot module.
Handles bot commands and coordinates downloads.
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

from telegram import Update, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters
)
from telegram.error import TelegramError

from .config import config
from .database import db, DownloadStatus
from .mega_downloader import (
    mega_downloader,
    MegaDownloadError,
    MegaQuotaExceededError
)
from .chunk_manager import ChunkManager
from .uploader import TelegramUploader
from .utils import (
    format_size,
    format_progress_bar,
    create_progress_message,
    create_status_message,
    validate_mega_link,
    DownloadSpeedTracker
)

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class MegaBot:
    """
    Main bot class that coordinates all operations.
    
    Features:
    - Command handlers for /start, /download, /status, /cancel
    - Download queue management
    - Progress tracking and updates
    - Error handling and recovery
    """
    
    def __init__(self):
        """Initialize the bot."""
        self.application: Optional[Application] = None
        self.uploader: Optional[TelegramUploader] = None
        self.active_downloads: Dict[int, asyncio.Task] = {}
        self.progress_messages: Dict[int, int] = {}  # request_id -> message_id
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialize bot components."""
        # Connect to database
        await db.connect()
        logger.info("Database connected")
        
        # Login to MEGA
        await mega_downloader.login()
        logger.info("MEGA logged in")
        
        # Build application
        self.application = (
            Application.builder()
            .token(config.bot_token)
            .build()
        )
        
        # Initialize uploader
        self.uploader = TelegramUploader(self.application.bot)
        
        # Register handlers
        self._register_handlers()
        
        # Resume any interrupted downloads
        await self._resume_downloads()
        
        logger.info("Bot initialized successfully")
    
    def _register_handlers(self) -> None:
        """Register command and message handlers."""
        self.application.add_handler(
            CommandHandler("start", self.cmd_start)
        )
        self.application.add_handler(
            CommandHandler("download", self.cmd_download)
        )
        self.application.add_handler(
            CommandHandler("status", self.cmd_status)
        )
        self.application.add_handler(
            CommandHandler("cancel", self.cmd_cancel)
        )
        self.application.add_handler(
            CommandHandler("help", self.cmd_help)
        )
        
        # Handle direct MEGA links
        self.application.add_handler(
            MessageHandler(
                filters.TEXT & filters.Regex(r'mega\.nz'),
                self.handle_mega_link
            )
        )
        
        # Error handler
        self.application.add_error_handler(self.error_handler)
    
    async def _resume_downloads(self) -> None:
        """Resume any interrupted downloads from database."""
        active_requests = await db.get_active_requests()
        
        for request in active_requests:
            if request.status in ['downloading', 'pending']:
                logger.info(f"Resuming download {request.id}")
                # Create a new task to resume the download
                task = asyncio.create_task(
                    self._process_download(
                        request.id,
                        request.chat_id,
                        request.mega_link
                    )
                )
                self.active_downloads[request.id] = task
    
    async def run(self) -> None:
        """Run the bot."""
        await self.initialize()
        
        # Start the bot
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling(drop_pending_updates=True)
        
        logger.info("Bot is running...")
        
        # Keep running until interrupted
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the bot."""
        logger.info("Shutting down...")
        
        # Cancel active downloads
        for request_id, task in self.active_downloads.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Stop the bot
        if self.application:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
        
        # Logout from MEGA
        await mega_downloader.logout()
        
        # Close database
        await db.close()
        
        logger.info("Shutdown complete")
    
    # Command Handlers
    
    async def cmd_start(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /start command."""
        welcome_message = """
🚀 **Welcome to MEGA Folder Downloader Bot!**

I can download MEGA.nz folder links in 4GB chunks to bypass quota limits.

**Commands:**
• `/download <mega-link>` - Start downloading a MEGA folder
• `/status` - Check your active downloads
• `/cancel <id>` - Cancel a download
• `/help` - Show help message

**How it works:**
1. Send me a MEGA folder link
2. I'll analyze the folder and split it into 4GB chunks
3. Each chunk is downloaded, zipped, and sent to you
4. After upload, files are automatically cleaned up

Just paste a MEGA folder link to get started! 📁
"""
        await update.message.reply_text(
            welcome_message,
            parse_mode="Markdown"
        )
    
    async def cmd_download(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /download command."""
        if not context.args:
            await update.message.reply_text(
                "❌ Please provide a MEGA folder link.\n"
                "Usage: `/download <mega-folder-link>`",
                parse_mode="Markdown"
            )
            return
        
        mega_link = context.args[0]
        await self._start_download(update, mega_link)
    
    async def handle_mega_link(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle direct MEGA links in messages."""
        text = update.message.text
        
        # Extract MEGA link from message
        import re
        patterns = [
            r'(https?://mega\.nz/folder/[^#\s]+#[^\s]+)',
            r'(https?://mega\.nz/#F![^!\s]+![^\s]+)',
            r'(https?://mega\.co\.nz/#F![^!\s]+![^\s]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                mega_link = match.group(1)
                await self._start_download(update, mega_link)
                return
        
        await update.message.reply_text(
            "❌ Invalid MEGA folder link format.\n"
            "Please send a valid MEGA folder URL."
        )
    
    async def _start_download(
        self,
        update: Update,
        mega_link: str
    ) -> None:
        """Start a new download."""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Validate link
        if not validate_mega_link(mega_link):
            await update.message.reply_text(
                "❌ Invalid MEGA folder link format."
            )
            return
        
        # Send initial message
        status_msg = await update.message.reply_text(
            "🔍 Analyzing MEGA folder..."
        )
        
        try:
            # Get folder info
            folder_info = await mega_downloader.get_folder_info(mega_link)
            
            if folder_info.file_count == 0:
                await status_msg.edit_text(
                    "❌ Folder is empty or inaccessible."
                )
                return
            
            # Calculate chunks
            chunk_count = max(
                1,
                (folder_info.total_size // config.chunk_size_bytes) + 1
