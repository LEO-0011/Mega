"""
Main Telegram bot module.
Handles bot commands and coordinates downloads.
"""

import asyncio
import logging
import re
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
    MegaQuotaExceededError,
    MegaFile
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
        self.chunk_managers: Dict[int, ChunkManager] = {}
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
                task = asyncio.create_task(
                    self._process_download(
                        request.id,
                        request.chat_id,
                        request.mega_link,
                        resume=True
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
    
    async def cmd_help(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /help command."""
        help_message = """
📖 **MEGA Folder Downloader - Help**

**Commands:**
• `/start` - Welcome message and quick guide
• `/download <link>` - Download a MEGA folder
• `/status` - View active downloads
• `/cancel <id>` - Cancel a specific download
• `/help` - This help message

**Supported Link Formats:**
• `https://mega.nz/folder/XXXXX#YYYYY`
• `https://mega.nz/#F!XXXXX!YYYYY`

**How Chunking Works:**
1. Files are organized into ~4GB chunks
2. Each chunk is downloaded sequentially
3. After download, chunk is zipped
4. Zip is uploaded to Telegram
5. Local files are deleted automatically

**Quota Bypass:**
The bot uses authenticated MEGA downloads and chunks
to minimize quota issues. If quota is hit, the bot
will wait and retry automatically.

**Troubleshooting:**
• _Slow downloads?_ - MEGA may be throttling
• _Quota errors?_ - Bot will auto-retry after delay
• _Missing files?_ - Check /status for errors

**File Size Limits:**
• Telegram: 2GB per file (auto-split if larger)
• Chunk size: 4GB (configurable)
"""
        await update.message.reply_text(
            help_message,
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
    
    async def cmd_status(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /status command."""
        user_id = update.effective_user.id
        
        # Get user's active requests
        requests = await db.get_active_requests(user_id)
        
        if not requests:
            await update.message.reply_text(
                "📭 You have no active downloads.\n"
                "Use `/download <mega-link>` to start one!",
                parse_mode="Markdown"
            )
            return
        
        status_lines = ["📋 **Your Active Downloads:**\n"]
        
        for req in requests:
            stats = await db.get_download_stats(req.id)
            
            status_emoji = {
                'pending': '⏳',
                'downloading': '📥',
                'uploading': '📤',
                'completed': '✅',
                'failed': '❌',
                'paused': '⏸️'
            }.get(req.status, '❓')
            
            progress_pct = 0
            if stats['total_size'] > 0:
                progress_pct = stats['downloaded_bytes'] / stats['total_size'] * 100
            
            progress_bar = format_progress_bar(
                stats['downloaded_bytes'],
                stats['total_size'],
                15
            )
            
            status_lines.append(
                f"{status_emoji} **ID: {req.id}** - {req.folder_name}\n"
                f"   {progress_bar}\n"
                f"   📦 Chunk: {req.current_chunk + 1}/{req.total_chunks}\n"
                f"   📁 Files: {stats['completed_files']}/{stats['total_files']}\n"
                f"   💾 {format_size(stats['downloaded_bytes'])} / {format_size(stats['total_size'])}\n"
            )
            
            if req.error_message:
                status_lines.append(f"   ⚠️ Error: {req.error_message[:50]}...\n")
        
        await update.message.reply_text(
            "\n".join(status_lines),
            parse_mode="Markdown"
        )
    
    async def cmd_cancel(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /cancel command."""
        user_id = update.effective_user.id
        
        if not context.args:
            # Show active downloads to cancel
            requests = await db.get_active_requests(user_id)
            if not requests:
                await update.message.reply_text(
                    "📭 No active downloads to cancel."
                )
                return
            
            lines = ["📋 **Active downloads:**\n"]
            for req in requests:
                lines.append(f"• ID: `{req.id}` - {req.folder_name}")
            lines.append("\nUse `/cancel <id>` to cancel a download.")
            
            await update.message.reply_text(
                "\n".join(lines),
                parse_mode="Markdown"
            )
            return
        
        try:
            request_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ Invalid download ID.")
            return
        
        # Verify ownership
        request = await db.get_request(request_id)
        if not request or request.user_id != user_id:
            await update.message.reply_text(
                "❌ Download not found or you don't have permission."
            )
            return
        
        # Cancel the download
        if request_id in self.active_downloads:
            self.active_downloads[request_id].cancel()
            del self.active_downloads[request_id]
        
        # Update database
        await db.update_request(
            request_id,
            status=DownloadStatus.FAILED.value,
            error_message="Cancelled by user"
        )
        
        # Cleanup files
        if request_id in self.chunk_managers:
            await self.chunk_managers[request_id].cleanup_all()
            del self.chunk_managers[request_id]
        
        await update.message.reply_text(
            f"✅ Download #{request_id} has been cancelled."
        )
    
    async def handle_mega_link(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle direct MEGA links in messages."""
        text = update.message.text
        
        # Extract MEGA link from message
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
        
        # Check concurrent download limit
        user_downloads = [
            rid for rid, task in self.active_downloads.items()
            if not task.done()
        ]
        if len(user_downloads) >= config.max_concurrent_downloads:
            await update.message.reply_text(
                f"⚠️ You already have {len(user_downloads)} active download(s).\n"
                "Please wait for them to complete or cancel one."
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
                -(-folder_info.total_size // config.chunk_size_bytes)  # Ceiling division
            )
            
            # Create database entry
            folder_structure = {
                'name': folder_info.name,
                'files': [
                    {
                        'handle': f.handle,
                        'name': f.name,
                        'size': f.size,
                        'path': f.path
                    }
                    for f in folder_info.files
                ]
            }
            
            request_id = await db.create_request(
                user_id=user_id,
                chat_id=chat_id,
                mega_link=mega_link,
                folder_name=folder_info.name,
                folder_structure=folder_structure
            )
            
            # Update with file count and size
            await db.update_request(
                request_id,
                total_files=folder_info.file_count,
                total_size=folder_info.total_size,
                total_chunks=chunk_count
            )
            
            # Add files to database
            files_data = [
                {
                    'path': f.path,
                    'name': f.name,
                    'size': f.size,
                    'handle': f.handle
                }
                for f in folder_info.files
            ]
            await db.add_files(request_id, files_data)
            
            # Update status message
            await status_msg.edit_text(
                f"✅ **Folder analyzed!**\n\n"
                f"📁 **Name:** {folder_info.name}\n"
                f"📊 **Files:** {folder_info.file_count}\n"
                f"💾 **Size:** {format_size(folder_info.total_size)}\n"
                f"📦 **Chunks:** {chunk_count} (4GB each)\n\n"
                f"🚀 Starting download... (ID: {request_id})",
                parse_mode="Markdown"
            )
            
            # Store progress message ID
            self.progress_messages[request_id] = status_msg.message_id
            
            # Start download task
            task = asyncio.create_task(
                self._process_download(
                    request_id,
                    chat_id,
                    mega_link,
                    resume=False
                )
            )
            self.active_downloads[request_id] = task
            
        except MegaDownloadError as e:
            await status_msg.edit_text(
                f"❌ Failed to access MEGA folder:\n{str(e)}"
            )
        except Exception as e:
            logger.exception("Error starting download")
            await status_msg.edit_text(
                f"❌ An error occurred:\n{str(e)}"
            )
    
    async def _process_download(
        self,
        request_id: int,
        chat_id: int,
        mega_link: str,
        resume: bool = False
    ) -> None:
        """
        Process the complete download workflow.
        
        Args:
            request_id: Database request ID.
            chat_id: Telegram chat ID.
            mega_link: MEGA folder link.
            resume: Whether this is a resumed download.
        """
        chunk_manager = None
        
        try:
            # Get request info
            request = await db.get_request(request_id)
            if not request:
                logger.error(f"Request {request_id} not found")
                return
            
            # Update status
            await db.update_request(
                request_id,
                status=DownloadStatus.DOWNLOADING.value
            )
            
            # Initialize chunk manager
            chunk_manager = ChunkManager(request_id)
            await chunk_manager.initialize()
            self.chunk_managers[request_id] = chunk_manager
            
            # Get folder info
            folder_info = await mega_downloader.get_folder_info(mega_link)
            
            # Organize files into chunks
            chunks = chunk_manager.organize_files_into_chunks(folder_info.files)
            await chunk_manager.save_chunk_assignments()
            
            # Update total chunks
            await db.update_request(
                request_id,
                total_chunks=len(chunks)
            )
            
            # Get starting chunk (for resume)
            start_chunk = request.current_chunk if resume else 0
            
            # Speed tracker
            speed_tracker = DownloadSpeedTracker()
            
            # Process each chunk
            for chunk_idx in range(start_chunk, len(chunks)):
                chunk = chunks[chunk_idx]
                
                # Update current chunk
                await db.update_request(
                    request_id,
                    current_chunk=chunk_idx
                )
                
                # Send chunk start notification
                await self.uploader.send_message(
                    chat_id,
                    f"📦 Starting chunk {chunk_idx + 1}/{len(chunks)}...\n"
                    f"Files: {len(chunk.files)}\n"
                    f"Size: {format_size(chunk.total_size)}"
                )
                
                # Download chunk files
                chunk_path = await chunk_manager.get_chunk_download_path(chunk_idx)
                
                downloaded_bytes = 0
                total_chunk_bytes = chunk.total_size
                
                for file_idx, file_info in enumerate(chunk.files):
                    try:
                        # Progress callback
                        async def update_progress(
                            filename: str,
                            file_downloaded: int,
                            file_total: int
                        ):
                            nonlocal downloaded_bytes
                            current_downloaded = downloaded_bytes + file_downloaded
                            speed = speed_tracker.update(current_downloaded)
                            
                            # Update database
                            await db.update_request(
                                request_id,
                                downloaded_bytes=request.downloaded_bytes + current_downloaded
                            )
                            
                            # Update progress message (throttled)
                            if file_downloaded % (1024 * 1024) == 0:  # Every 1MB
                                progress_msg = create_progress_message(
                                    folder_name=request.folder_name,
                                    current_file=filename,
                                    file_progress=file_downloaded,
                                    file_total=file_total,
                                    chunk_number=chunk_idx,
                                    total_chunks=len(chunks),
                                    overall_progress=current_downloaded,
                                    overall_total=total_chunk_bytes,
                                    speed_bps=speed
                                )
                                
                                if request_id in self.progress_messages:
                                    await self.uploader.send_progress_update(
                                        chat_id,
                                        self.progress_messages[request_id],
                                        progress_msg
                                    )
                        
                        # Download file
                        def sync_progress(dl: int, total: int):
                            asyncio.create_task(
                                update_progress(file_info.name, dl, total)
                            )
                        
                        await mega_downloader.download_file(
                            file_info,
                            chunk_path,
                            sync_progress
                        )
                        
                        downloaded_bytes += file_info.size
                        
                        # Update file progress in database
                        pending_files = await db.get_pending_files(request_id)
                        for pf in pending_files:
                            if pf.mega_handle == file_info.handle:
                                await db.update_file_progress(
                                    pf.id,
                                    status=DownloadStatus.COMPLETED.value,
                                    downloaded_bytes=file_info.size,
                                    local_path=str(chunk_path / file_info.path)
                                )
                                break
                        
                    except MegaQuotaExceededError:
                        # Wait and retry
                        await self.uploader.send_message(
                            chat_id,
                            f"⚠️ MEGA quota reached. Waiting {config.retry_delay_seconds}s before retry..."
                        )
                        await asyncio.sleep(config.retry_delay_seconds)
                        # Retry will be handled by tenacity decorator
                        raise
                    
                    except Exception as e:
                        logger.error(f"Error downloading {file_info.name}: {e}")
                        # Continue with next file
                        continue
                
                # Chunk download complete - create zip
                await self.uploader.send_message(
                    chat_id,
                    f"📦 Chunk {chunk_idx + 1} downloaded. Creating zip..."
                )
                
                zip_path = await chunk_manager.create_chunk_zip(chunk_idx)
                
                # Upload to Telegram
                await db.update_request(
                    request_id,
                    status=DownloadStatus.UPLOADING.value
                )
                
                await self.uploader.send_message(
                    chat_id,
                    f"📤 Uploading chunk {chunk_idx + 1}/{len(chunks)}..."
                )
                
                upload_success = await self.uploader.upload_chunk(
                    chat_id=chat_id,
                    chunk_path=zip_path,
                    chunk_number=chunk_idx,
                    total_chunks=len(chunks),
                    folder_name=request.folder_name
                )
                
                if upload_success:
                    # Mark chunk complete and cleanup
                    await chunk_manager.mark_chunk_complete(chunk_idx)
                    await chunk_manager.cleanup_chunk(chunk_idx)
                    
                    await self.uploader.send_message(
                        chat_id,
                        f"✅ Chunk {chunk_idx + 1}/{len(chunks)} uploaded and cleaned up!"
                    )
                else:
                    await self.uploader.send_message(
                        chat_id,
                        f"⚠️ Failed to upload chunk {chunk_idx + 1}. Files saved locally."
                    )
                
                # Update status back to downloading for next chunk
                if chunk_idx < len(chunks) - 1:
                    await db.update_request(
                        request_id,
                        status=DownloadStatus.DOWNLOADING.value
                    )
            
            # All chunks complete!
            await db.update_request(
                request_id,
                status=DownloadStatus.COMPLETED.value
            )
            
            # Final cleanup
            await chunk_manager.cleanup_all()
            
            # Send completion message
            stats = await db.get_download_stats(request_id)
            await self.uploader.send_message(
                chat_id,
                f"🎉 **Download Complete!**\n\n"
                f"📁 **Folder:** {request.folder_name}\n"
                f"📊 **Files:** {stats['total_files']}\n"
                f"💾 **Total Size:** {format_size(stats['total_size'])}\n"
                f"📦 **Chunks:** {len(chunks)}\n\n"
                f"All files have been uploaded and cleaned up! ✨"
            )
            
        except asyncio.CancelledError:
            logger.info(f"Download {request_id} was cancelled")
            await db.update_request(
                request_id,
                status=DownloadStatus.FAILED.value,
                error_message="Download cancelled"
            )
            raise
        
        except MegaQuotaExceededError as e:
            logger.error(f"Quota exceeded for request {request_id}")
            await db.update_request(
                request_id,
                status=DownloadStatus.PAUSED.value,
                error_message=str(e)
            )
            await self.uploader.send_message(
                chat_id,
                f"⏸️ Download paused due to MEGA quota.\n"
                f"Will resume automatically or use /status to check."
            )
        
        except Exception as e:
            logger.exception(f"Error processing download {request_id}")
            await db.update_request(
                request_id,
                status=DownloadStatus.FAILED.value,
                error_message=str(e)
            )
            await self.uploader.send_message(
                chat_id,
                f"❌ Download failed:\n{str(e)}"
            )
        
        finally:
            # Cleanup
            if request_id in self.active_downloads:
                del self.active_downloads[request_id]
            if request_id in self.progress_messages:
                del self.progress_messages[request_id]
            if request_id in self.chunk_managers:
                del self.chunk_managers[request_id]
    
    async def error_handler(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle errors in the bot."""
        logger.error(f"Update {update} caused error {context.error}")
        
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ An error occurred. Please try again later."
                )
            except TelegramError:
                pass


async def main():
    """Main entry point."""
    bot = MegaBot()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
