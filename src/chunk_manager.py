"""
Chunk management module.
Handles organizing downloads into chunks and preparing them for upload.
"""

import asyncio
import os
import shutil
import zipfile
from pathlib import Path
from typing import List, Dict, Optional, Callable, AsyncIterator
from dataclasses import dataclass
import logging
import aiofiles
import aiofiles.os

from .config import config
from .database import db, DownloadStatus, FileProgress
from .mega_downloader import MegaDownloader, MegaFile, MegaFolder

logger = logging.getLogger(__name__)


@dataclass
class ChunkInfo:
    """Information about a download chunk."""
    chunk_number: int
    files: List[MegaFile]
    total_size: int
    download_path: Path
    zip_path: Optional[Path] = None
    status: str = "pending"


class ChunkManager:
    """
    Manages the chunking of downloads and preparation for upload.
    
    Features:
    - Organize files into optimal chunks
    - Track chunk progress
    - Create zip archives for upload
    - Clean up after successful upload
    """
    
    def __init__(
        self,
        request_id: int,
        storage_path: Path = None,
        chunk_size_bytes: int = None
    ):
        """
        Initialize chunk manager.
        
        Args:
            request_id: Database request ID.
            storage_path: Base storage path for downloads.
            chunk_size_bytes: Maximum size per chunk.
        """
        self.request_id = request_id
        self.storage_path = storage_path or config.storage_path
        self.chunk_size_bytes = chunk_size_bytes or config.chunk_size_bytes
        self.request_folder = self.storage_path / f"request_{request_id}"
        self.chunks: List[ChunkInfo] = []
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Create necessary directories."""
        self.request_folder.mkdir(parents=True, exist_ok=True)
    
    def organize_files_into_chunks(
        self,
        files: List[MegaFile]
    ) -> List[ChunkInfo]:
        """
        Organize files into chunks based on size limits.
        
        Uses a first-fit decreasing bin packing algorithm for optimal
        chunk utilization.
        
        Args:
            files: List of MegaFile objects.
            
        Returns:
            List of ChunkInfo objects.
        """
        # Sort files by size (descending) for better packing
        sorted_files = sorted(files, key=lambda f: f.size, reverse=True)
        
        chunks: List[ChunkInfo] = []
        
        for file in sorted_files:
            placed = False
            
            # Try to fit in existing chunk
            for chunk in chunks:
                if chunk.total_size + file.size <= self.chunk_size_bytes:
                    chunk.files.append(file)
                    chunk.total_size += file.size
                    placed = True
                    break
            
            # Create new chunk if needed
            if not placed:
                chunk_num = len(chunks)
                chunk_path = self.request_folder / f"chunk_{chunk_num}"
                
                new_chunk = ChunkInfo(
                    chunk_number=chunk_num,
                    files=[file],
                    total_size=file.size,
                    download_path=chunk_path
                )
                chunks.append(new_chunk)
        
        self.chunks = chunks
        logger.info(f"Organized {len(files)} files into {len(chunks)} chunks")
        return chunks
    
    async def save_chunk_assignments(self) -> None:
        """Save chunk assignments to database."""
        for chunk in self.chunks:
            file_ids = []
            
            for file in chunk.files:
                # Get file ID from database
                pending_files = await db.get_pending_files(self.request_id)
                for pf in pending_files:
                    if pf.mega_handle == file.handle:
                        file_ids.append(pf.id)
                        break
            
            if file_ids:
                await db.assign_files_to_chunk(
                    self.request_id,
                    file_ids,
                    chunk.chunk_number
                )
    
    async def get_chunk_download_path(self, chunk_number: int) -> Path:
        """
        Get the download path for a specific chunk.
        
        Args:
            chunk_number: The chunk number.
            
        Returns:
            Path to chunk download folder.
        """
        chunk_path = self.request_folder / f"chunk_{chunk_number}"
        chunk_path.mkdir(parents=True, exist_ok=True)
        return chunk_path
    
    async def create_chunk_zip(
        self,
        chunk_number: int,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Path:
        """
        Create a zip archive for a chunk.
        
        Args:
            chunk_number: The chunk number to zip.
            progress_callback: Optional callback for progress updates.
            
        Returns:
            Path to the created zip file.
        """
        chunk_path = self.request_folder / f"chunk_{chunk_number}"
        zip_path = self.request_folder / f"chunk_{chunk_number}.zip"
        
        if not chunk_path.exists():
            raise FileNotFoundError(f"Chunk folder not found: {chunk_path}")
        
        # Calculate total size for progress
        total_size = sum(
            f.stat().st_size 
            for f in chunk_path.rglob("*") 
            if f.is_file()
        )
        processed_size = 0
        
        # Create zip file
        loop = asyncio.get_event_loop()
        
        def create_zip():
            nonlocal processed_size
            
            with zipfile.ZipFile(
                zip_path,
                'w',
                zipfile.ZIP_DEFLATED,
                compresslevel=6
            ) as zf:
                for file_path in chunk_path.rglob("*"):
                    if file_path.is_file():
                        arcname = file_path.relative_to(chunk_path)
                        zf.write(file_path, arcname)
                        processed_size += file_path.stat().st_size
                        
                        if progress_callback:
                            progress_callback(processed_size, total_size)
        
        await loop.run_in_executor(None, create_zip)
        
        # Update database
        await db.update_chunk_status(
            self.request_id,
            chunk_number,
            "zipped",
            str(zip_path)
        )
        
        logger.info(f"Created zip for chunk {chunk_number}: {zip_path}")
        return zip_path
    
    async def split_zip_for_telegram(
        self,
        zip_path: Path,
        max_size: int = None
    ) -> List[Path]:
        """
        Split a zip file into parts if it exceeds Telegram's limit.
        
        Args:
            zip_path: Path to the zip file.
            max_size: Maximum size per part (default: Telegram limit).
            
        Returns:
            List of paths to split files.
        """
        max_size = max_size or config.telegram_file_limit
        file_size = zip_path.stat().st_size
        
        if file_size <= max_size:
            return [zip_path]
        
        # Split the file
        parts = []
        part_num = 0
        
        async with aiofiles.open(zip_path, 'rb') as f:
            while True:
                chunk = await f.read(max_size)
                if not chunk:
                    break
                
                part_path = zip_path.with_suffix(f".zip.part{part_num:03d}")
                async with aiofiles.open(part_path, 'wb') as part_file:
                    await part_file.write(chunk)
                
                parts.append(part_path)
                part_num += 1
        
        logger.info(f"Split {zip_path} into {len(parts)} parts")
        return parts
    
    async def cleanup_chunk(self, chunk_number: int) -> None:
        """
        Clean up chunk files after successful upload.
        
        Args:
            chunk_number: The chunk number to clean up.
        """
        async with self._lock:
            chunk_path = self.request_folder / f"chunk_{chunk_number}"
            zip_path = self.request_folder / f"chunk_{chunk_number}.zip"
            
            # Remove chunk folder
            if chunk_path.exists():
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    shutil.rmtree,
                    chunk_path
                )
                logger.info(f"Cleaned up chunk folder: {chunk_path}")
            
            # Remove zip file
            if zip_path.exists():
                await aiofiles.os.remove(zip_path)
                logger.info(f"Cleaned up zip file: {zip_path}")
            
            # Remove any split parts
            for part_path in self.request_folder.glob(f"chunk_{chunk_number}.zip.part*"):
                await aiofiles.os.remove(part_path)
    
    async def cleanup_all(self) -> None:
        """Clean up all files for this request."""
        async with self._lock:
            if self.request_folder.exists():
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    shutil.rmtree,
                    self.request_folder
                )
                logger.info(f"Cleaned up request folder: {self.request_folder}")
    
    async def get_progress(self) -> Dict[str, any]:
        """
        Get current progress information.
        
        Returns:
            Dictionary with progress statistics.
        """
        stats = await db.get_download_stats(self.request_id)
        
        return {
            'total_chunks': len(self.chunks),
            'completed_chunks': stats['completed_chunks'],
            'total_files': stats['total_files'],
            'completed_files': stats['completed_files'],
            'total_size': stats['total_size'],
            'downloaded_size': stats['downloaded_bytes'],
            'progress_percent': (
                (stats['downloaded_bytes'] / stats['total_size'] * 100)
                if stats['total_size'] > 0 else 0
            )
        }
    
    async def get_next_pending_chunk(self) -> Optional[ChunkInfo]:
        """
        Get the next chunk that needs to be downloaded.
        
        Returns:
            ChunkInfo or None if all chunks are complete.
        """
        for chunk in self.chunks:
            if chunk.status == "pending":
                return chunk
        return None
    
    async def mark_chunk_complete(self, chunk_number: int) -> None:
        """
        Mark a chunk as complete.
        
        Args:
            chunk_number: The chunk number.
        """
        for chunk in self.chunks:
            if chunk.chunk_number == chunk_number:
                chunk.status = "completed"
                break
        
        await db.update_chunk_status(
            self.request_id,
            chunk_number,
            "completed"
        )
