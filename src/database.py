"""
Database module for persistent storage using SQLite.
Tracks user requests, download progress, and error states.
"""

import aiosqlite
import json
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum

from .config import config


class DownloadStatus(Enum):
    """Enumeration of download status states."""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class DownloadRequest:
    """Represents a download request from a user."""
    id: Optional[int]
    user_id: int
    chat_id: int
    mega_link: str
    folder_name: str
    total_files: int
    total_size: int
    current_chunk: int
    total_chunks: int
    status: str
    downloaded_bytes: int
    error_message: Optional[str]
    created_at: str
    updated_at: str
    folder_structure: str  # JSON string


@dataclass
class FileProgress:
    """Tracks individual file download progress."""
    id: Optional[int]
    request_id: int
    file_path: str
    file_name: str
    file_size: int
    downloaded_bytes: int
    chunk_number: int
    status: str
    mega_handle: str
    local_path: Optional[str]
    created_at: str
    updated_at: str


class Database:
    """
    Async SQLite database handler for the MEGA bot.
    
    Provides methods for tracking download requests, file progress,
    and maintaining state for resume functionality.
    """
    
    def __init__(self, db_path: Path = None):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file.
        """
        self.db_path = db_path or config.database_path
        self._connection: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
    
    async def connect(self) -> None:
        """Establish database connection and create tables."""
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row
        await self._create_tables()
    
    async def close(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None
    
    async def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        async with self._lock:
            # Download requests table
            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS download_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    mega_link TEXT NOT NULL,
                    folder_name TEXT NOT NULL,
                    total_files INTEGER DEFAULT 0,
                    total_size INTEGER DEFAULT 0,
                    current_chunk INTEGER DEFAULT 0,
                    total_chunks INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    downloaded_bytes INTEGER DEFAULT 0,
                    error_message TEXT,
                    folder_structure TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            
            # File progress table
            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS file_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    downloaded_bytes INTEGER DEFAULT 0,
                    chunk_number INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    mega_handle TEXT NOT NULL,
                    local_path TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (request_id) REFERENCES download_requests(id)
                )
            """)
            
            # Chunk tracking table
            await self._connection.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id INTEGER NOT NULL,
                    chunk_number INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    zip_path TEXT,
                    upload_status TEXT DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (request_id) REFERENCES download_requests(id),
                    UNIQUE(request_id, chunk_number)
                )
            """)
            
            # Create indexes for faster lookups
            await self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_requests_user 
                ON download_requests(user_id)
            """)
            await self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_requests_status 
                ON download_requests(status)
            """)
            await self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_files_request 
                ON file_progress(request_id)
            """)
            
            await self._connection.commit()
    
    async def create_request(
        self,
        user_id: int,
        chat_id: int,
        mega_link: str,
        folder_name: str,
        folder_structure: Dict[str, Any]
    ) -> int:
        """
        Create a new download request.
        
        Args:
            user_id: Telegram user ID.
            chat_id: Telegram chat ID.
            mega_link: MEGA folder link.
            folder_name: Name of the MEGA folder.
            folder_structure: Dictionary of folder structure.
            
        Returns:
            int: The request ID.
        """
        now = datetime.utcnow().isoformat()
        
        async with self._lock:
            cursor = await self._connection.execute(
                """
                INSERT INTO download_requests 
                (user_id, chat_id, mega_link, folder_name, folder_structure, 
                 status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, chat_id, mega_link, folder_name,
                 json.dumps(folder_structure), DownloadStatus.PENDING.value,
                 now, now)
            )
            await self._connection.commit()
            return cursor.lastrowid
    
    async def get_request(self, request_id: int) -> Optional[DownloadRequest]:
        """
        Get a download request by ID.
        
        Args:
            request_id: The request ID.
            
        Returns:
            DownloadRequest or None if not found.
        """
        async with self._lock:
            cursor = await self._connection.execute(
                "SELECT * FROM download_requests WHERE id = ?",
                (request_id,)
            )
            row = await cursor.fetchone()
            
            if row:
                return DownloadRequest(**dict(row))
            return None
    
    async def get_active_requests(self, user_id: int = None) -> List[DownloadRequest]:
        """
        Get all active (non-completed, non-failed) requests.
        
        Args:
            user_id: Optional user ID to filter by.
            
        Returns:
            List of active DownloadRequest objects.
        """
        query = """
            SELECT * FROM download_requests 
            WHERE status NOT IN ('completed', 'failed')
        """
        params = []
        
        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)
        
        async with self._lock:
            cursor = await self._connection.execute(query, params)
            rows = await cursor.fetchall()
            return [DownloadRequest(**dict(row)) for row in rows]
    
    async def update_request(
        self,
        request_id: int,
        **kwargs
    ) -> None:
        """
        Update a download request.
        
        Args:
            request_id: The request ID.
            **kwargs: Fields to update.
        """
        if not kwargs:
            return
        
        kwargs['updated_at'] = datetime.utcnow().isoformat()
        
        set_clause = ", ".join(f"{k} = ?" for k in kwargs.keys())
        values = list(kwargs.values()) + [request_id]
        
        async with self._lock:
            await self._connection.execute(
                f"UPDATE download_requests SET {set_clause} WHERE id = ?",
                values
            )
            await self._connection.commit()
    
    async def add_files(
        self,
        request_id: int,
        files: List[Dict[str, Any]]
    ) -> None:
        """
        Add files to track for a request.
        
        Args:
            request_id: The request ID.
            files: List of file dictionaries with path, name, size, handle.
        """
        now = datetime.utcnow().isoformat()
        
        async with self._lock:
            for file_info in files:
                await self._connection.execute(
                    """
                    INSERT INTO file_progress 
                    (request_id, file_path, file_name, file_size, mega_handle,
                     status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (request_id, file_info['path'], file_info['name'],
                     file_info['size'], file_info['handle'],
                     DownloadStatus.PENDING.value, now, now)
                )
            await self._connection.commit()
    
    async def get_pending_files(
        self,
        request_id: int,
        chunk_number: int = None
    ) -> List[FileProgress]:
        """
        Get pending files for a request.
        
        Args:
            request_id: The request ID.
            chunk_number: Optional chunk number to filter by.
            
        Returns:
            List of FileProgress objects.
        """
        query = """
            SELECT * FROM file_progress 
            WHERE request_id = ? AND status = 'pending'
        """
        params = [request_id]
        
        if chunk_number is not None:
            query += " AND chunk_number = ?"
            params.append(chunk_number)
        
        query += " ORDER BY file_size ASC"
        
        async with self._lock:
            cursor = await self._connection.execute(query, params)
            rows = await cursor.fetchall()
            return [FileProgress(**dict(row)) for row in rows]
    
    async def update_file_progress(
        self,
        file_id: int,
        **kwargs
    ) -> None:
        """
        Update file progress.
        
        Args:
            file_id: The file progress ID.
            **kwargs: Fields to update.
        """
        if not kwargs:
            return
        
        kwargs['updated_at'] = datetime.utcnow().isoformat()
        
        set_clause = ", ".join(f"{k} = ?" for k in kwargs.keys())
        values = list(kwargs.values()) + [file_id]
        
        async with self._lock:
            await self._connection.execute(
                f"UPDATE file_progress SET {set_clause} WHERE id = ?",
                values
            )
            await self._connection.commit()
    
    async def assign_files_to_chunk(
        self,
        request_id: int,
        file_ids: List[int],
        chunk_number: int
    ) -> None:
        """
        Assign files to a specific chunk.
        
        Args:
            request_id: The request ID.
            file_ids: List of file IDs to assign.
            chunk_number: The chunk number.
        """
        now = datetime.utcnow().isoformat()
        
        async with self._lock:
            # Update file assignments
            placeholders = ",".join("?" * len(file_ids))
            await self._connection.execute(
                f"""
                UPDATE file_progress 
                SET chunk_number = ?, updated_at = ?
                WHERE id IN ({placeholders})
                """,
                [chunk_number, now] + file_ids
            )
            
            # Create chunk record if not exists
            await self._connection.execute(
                """
                INSERT OR IGNORE INTO chunks 
                (request_id, chunk_number, status, created_at, updated_at)
                VALUES (?, ?, 'pending', ?, ?)
                """,
                (request_id, chunk_number, now, now)
            )
            
            await self._connection.commit()
    
    async def get_chunk_files(
        self,
        request_id: int,
        chunk_number: int
    ) -> List[FileProgress]:
        """
        Get all files assigned to a chunk.
        
        Args:
            request_id: The request ID.
            chunk_number: The chunk number.
            
        Returns:
            List of FileProgress objects.
        """
        async with self._lock:
            cursor = await self._connection.execute(
                """
                SELECT * FROM file_progress 
                WHERE request_id = ? AND chunk_number = ?
                ORDER BY file_path
                """,
                (request_id, chunk_number)
            )
            rows = await cursor.fetchall()
            return [FileProgress(**dict(row)) for row in rows]
    
    async def update_chunk_status(
        self,
        request_id: int,
        chunk_number: int,
        status: str,
        zip_path: str = None
    ) -> None:
        """
        Update chunk status.
        
        Args:
            request_id: The request ID.
            chunk_number: The chunk number.
            status: New status.
            zip_path: Optional path to zip file.
        """
        now = datetime.utcnow().isoformat()
        
        async with self._lock:
            if zip_path:
                await self._connection.execute(
                    """
                    UPDATE chunks 
                    SET status = ?, zip_path = ?, updated_at = ?
                    WHERE request_id = ? AND chunk_number = ?
                    """,
                    (status, zip_path, now, request_id, chunk_number)
                )
            else:
                await self._connection.execute(
                    """
                    UPDATE chunks 
                    SET status = ?, updated_at = ?
                    WHERE request_id = ? AND chunk_number = ?
                    """,
                    (status, now, request_id, chunk_number)
                )
            await self._connection.commit()
    
    async def get_download_stats(self, request_id: int) -> Dict[str, Any]:
        """
        Get download statistics for a request.
        
        Args:
            request_id: The request ID.
            
        Returns:
            Dictionary with download statistics.
        """
        async with self._lock:
            # Get total and completed files
            cursor = await self._connection.execute(
                """
                SELECT 
                    COUNT(*) as total_files,
                    SUM(file_size) as total_size,
                    SUM(downloaded_bytes) as downloaded_bytes,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_files
                FROM file_progress
                WHERE request_id = ?
                """,
                (request_id,)
            )
            row = await cursor.fetchone()
            
            # Get chunk stats
            chunk_cursor = await self._connection.execute(
                """
                SELECT 
                    COUNT(*) as total_chunks,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_chunks
                FROM chunks
                WHERE request_id = ?
                """,
                (request_id,)
            )
            chunk_row = await chunk_cursor.fetchone()
            
            return {
                'total_files': row['total_files'] or 0,
                'total_size': row['total_size'] or 0,
                'downloaded_bytes': row['downloaded_bytes'] or 0,
                'completed_files': row['completed_files'] or 0,
                'total_chunks': chunk_row['total_chunks'] or 0,
                'completed_chunks': chunk_row['completed_chunks'] or 0,
            }
    
    async def cleanup_completed_request(self, request_id: int) -> None:
        """
        Clean up completed request data (optional, for storage management).
        
        Args:
            request_id: The request ID.
        """
        async with self._lock:
            # Keep the main request record but clean file details
            await self._connection.execute(
                "DELETE FROM file_progress WHERE request_id = ?",
                (request_id,)
            )
            await self._connection.execute(
                "DELETE FROM chunks WHERE request_id = ?",
                (request_id,)
            )
            await self._connection.commit()


# Global database instance
db = Database()
