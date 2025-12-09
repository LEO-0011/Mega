"""
MEGA download handler module.
Manages connections to MEGA and downloads files with progress tracking.
"""

import asyncio
import os
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass
import logging
from concurrent.futures import ThreadPoolExecutor

from mega import Mega
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from .config import config
from .database import db, DownloadStatus, FileProgress

logger = logging.getLogger(__name__)


class MegaQuotaExceededError(Exception):
    """Raised when MEGA download quota is exceeded."""
    pass


class MegaDownloadError(Exception):
    """General MEGA download error."""
    pass


@dataclass
class MegaFile:
    """Represents a file in MEGA storage."""
    handle: str
    name: str
    size: int
    path: str
    parent_handle: str


@dataclass
class MegaFolder:
    """Represents a folder structure from MEGA."""
    name: str
    handle: str
    files: List[MegaFile]
    total_size: int
    file_count: int


class MegaDownloader:
    """
    Handles MEGA downloads with quota management and progress tracking.
    
    Features:
    - Login with credentials
    - Parse public folder links
    - Download files with progress callbacks
    - Handle quota exceeded errors gracefully
    - Chunk-based downloading
    """
    
    def __init__(self):
        """Initialize MEGA downloader."""
        self._mega: Optional[Mega] = None
        self._logged_in = False
        self._executor = ThreadPoolExecutor(max_workers=config.max_concurrent_downloads)
        self._lock = asyncio.Lock()
        self._progress_callbacks: Dict[int, Callable] = {}
    
    async def login(self) -> bool:
        """
        Login to MEGA account.
        
        Returns:
            bool: True if login successful.
        """
        async with self._lock:
            if self._logged_in:
                return True
            
            try:
                loop = asyncio.get_event_loop()
                self._mega = await loop.run_in_executor(
                    self._executor,
                    self._do_login
                )
                self._logged_in = True
                logger.info("Successfully logged into MEGA")
                return True
            except Exception as e:
                logger.error(f"MEGA login failed: {e}")
                raise MegaDownloadError(f"Login failed: {e}")
    
    def _do_login(self) -> Mega:
        """Synchronous login helper."""
        mega = Mega()
        return mega.login(config.mega_email, config.mega_password)
    
    async def logout(self) -> None:
        """Logout from MEGA."""
        async with self._lock:
            self._mega = None
            self._logged_in = False
    
    def parse_folder_link(self, link: str) -> Tuple[str, Optional[str]]:
        """
        Parse a MEGA folder link to extract folder ID and key.
        
        Args:
            link: MEGA folder URL.
            
        Returns:
            Tuple of (folder_id, folder_key).
        """
        # Handle different MEGA URL formats
        patterns = [
            r'mega\.nz/folder/([^#]+)#([^/]+)',  # New format
            r'mega\.nz/#F!([^!]+)!([^/]+)',       # Old format
            r'mega\.co\.nz/#F!([^!]+)!([^/]+)',   # Very old format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, link)
            if match:
                return match.group(1), match.group(2)
        
        raise ValueError(f"Invalid MEGA folder link format: {link}")
    
    async def get_folder_info(self, link: str) -> MegaFolder:
        """
        Get information about a MEGA folder.
        
        Args:
            link: MEGA folder URL.
            
        Returns:
            MegaFolder with file list and metadata.
        """
        await self.login()
        
        try:
            loop = asyncio.get_event_loop()
            folder_info = await loop.run_in_executor(
                self._executor,
                self._get_folder_info_sync,
                link
            )
            return folder_info
        except Exception as e:
            logger.error(f"Failed to get folder info: {e}")
            raise MegaDownloadError(f"Failed to get folder info: {e}")
    
    def _get_folder_info_sync(self, link: str) -> MegaFolder:
        """Synchronous folder info retrieval."""
        # Import folder from link
        folder_contents = self._mega.get_files_in_node(
            self._mega.import_public_url(link)
        )
        
        if not folder_contents:
            # Try getting files from public folder directly
            folder_contents = self._mega.get_public_folder_files(link)
        
        files = []
        total_size = 0
        folder_name = "MEGA_Download"
        
        for handle, item in folder_contents.items():
            if item.get('t') == 0:  # File (not folder)
                file_info = MegaFile(
                    handle=handle,
                    name=item.get('a', {}).get('n', 'unknown'),
                    size=item.get('s', 0),
                    path=self._get_file_path(folder_contents, handle),
                    parent_handle=item.get('p', '')
                )
                files.append(file_info)
                total_size += file_info.size
            elif item.get('t') == 1 and not folder_name:  # Root folder
                folder_name = item.get('a', {}).get('n', 'MEGA_Download')
        
        return MegaFolder(
            name=folder_name,
            handle=list(folder_contents.keys())[0] if folder_contents else '',
            files=files,
            total_size=total_size,
            file_count=len(files)
        )
    
    def _get_file_path(
        self,
        folder_contents: Dict,
        handle: str,
        current_path: str = ""
    ) -> str:
        """
        Build the full path for a file within the folder structure.
        
        Args:
            folder_contents: Dictionary of all items in folder.
            handle: File handle.
            current_path: Current accumulated path.
            
        Returns:
            Full path string.
        """
        item = folder_contents.get(handle, {})
        name = item.get('a', {}).get('n', 'unknown')
        parent = item.get('p')
        
        if parent and parent in folder_contents:
            parent_item = folder_contents[parent]
            if parent_item.get('t') == 1:  # Is a folder
                parent_path = self._get_file_path(folder_contents, parent)
                return f"{parent_path}/{name}" if parent_path else name
        
        return name
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=60, max=600),
        retry=retry_if_exception_type(MegaQuotaExceededError)
    )
    async def download_file(
        self,
        file_info: MegaFile,
        dest_folder: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Path:
        """
        Download a single file from MEGA.
        
        Args:
            file_info: MegaFile object with file details.
            dest_folder: Destination folder path.
            progress_callback: Optional callback for progress updates.
            
        Returns:
            Path to downloaded file.
            
        Raises:
            MegaQuotaExceededError: If quota is exceeded.
            MegaDownloadError: If download fails.
        """
        await self.login()
        
        # Create destination path maintaining folder structure
        file_path = dest_folder / file_info.path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            loop = asyncio.get_event_loop()
            result_path = await loop.run_in_executor(
                self._executor,
                self._download_file_sync,
                file_info,
                file_path,
                progress_callback
            )
            return result_path
        except Exception as e:
            error_msg = str(e).lower()
            if 'quota' in error_msg or 'bandwidth' in error_msg or 'limit' in error_msg:
                logger.warning(f"Quota exceeded for file {file_info.name}")
                raise MegaQuotaExceededError(f"Quota exceeded: {e}")
            raise MegaDownloadError(f"Download failed: {e}")
    
    def _download_file_sync(
        self,
        file_info: MegaFile,
        dest_path: Path,
        progress_callback: Optional[Callable]
    ) -> Path:
        """Synchronous file download."""
        # Download file using mega.py
        self._mega.download_url(
            url=file_info.handle,
            dest_path=str(dest_path.parent),
            dest_filename=dest_path.name
        )
        
        if progress_callback:
            # Report completion
            progress_callback(file_info.size, file_info.size)
        
        return dest_path
    
    async def download_files_chunked(
        self,
        files: List[MegaFile],
        dest_folder: Path,
        chunk_size_bytes: int,
        progress_callback: Optional[Callable[[str, int, int, int], None]] = None,
        chunk_complete_callback: Optional[Callable[[int, Path, int], None]] = None
    ) -> List[Tuple[int, Path]]:
        """
        Download files in chunks, respecting size limits.
        
        Args:
            files: List of MegaFile objects to download.
            dest_folder: Base destination folder.
            chunk_size_bytes: Maximum bytes per chunk.
            progress_callback: Callback(filename, downloaded, total, chunk_num).
            chunk_complete_callback: Callback(chunk_num, chunk_path, chunk_size).
            
        Returns:
            List of (chunk_number, chunk_path) tuples.
        """
        # Sort files by size for better chunk packing
        sorted_files = sorted(files, key=lambda f: f.size)
        
        chunks: List[Tuple[int, Path]] = []
        current_chunk = 0
        current_chunk_size = 0
        chunk_folder = dest_folder / f"chunk_{current_chunk}"
        chunk_folder.mkdir(parents=True, exist_ok=True)
        
        for file_info in sorted_files:
            # Check if we need to start a new chunk
            if current_chunk_size + file_info.size > chunk_size_bytes and current_chunk_size > 0:
                # Complete current chunk
                if chunk_complete_callback:
                    await chunk_complete_callback(
                        current_chunk,
                        chunk_folder,
                        current_chunk_size
                    )
                
                chunks.append((current_chunk, chunk_folder))
                
                # Start new chunk
                current_chunk += 1
                current_chunk_size = 0
                chunk_folder = dest_folder / f"chunk_{current_chunk}"
                chunk_folder.mkdir(parents=True, exist_ok=True)
            
            # Download file to current chunk
            try:
                def file_progress(downloaded: int, total: int):
                    if progress_callback:
                        progress_callback(
                            file_info.name,
                            downloaded,
                            total,
                            current_chunk
                        )
                
                await self.download_file(
                    file_info,
                    chunk_folder,
                    file_progress
                )
                current_chunk_size += file_info.size
                
            except MegaQuotaExceededError:
                # Wait and retry will be handled by tenacity
                raise
            except Exception as e:
                logger.error(f"Failed to download {file_info.name}: {e}")
                # Continue with next file
                continue
        
        # Handle last chunk
        if current_chunk_size > 0:
            if chunk_complete_callback:
                await chunk_complete_callback(
                    current_chunk,
                    chunk_folder,
                    current_chunk_size
                )
            chunks.append((current_chunk, chunk_folder))
        
        return chunks
    
    async def get_file_by_handle(self, handle: str) -> Optional[Dict[str, Any]]:
        """
        Get file information by its handle.
        
        Args:
            handle: MEGA file handle.
            
        Returns:
            File information dictionary or None.
        """
        await self.login()
        
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self._executor,
                lambda: self._mega.get_node_by_handle(handle)
            )
        except Exception as e:
            logger.error(f"Failed to get file by handle: {e}")
            return None


# Global downloader instance
mega_downloader = MegaDownloader()
