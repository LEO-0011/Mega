"""
Basic tests for the MEGA Telegram bot.
"""

import pytest
import asyncio
from pathlib import Path
import tempfile
import os

# Set test environment variables before imports
os.environ['BOT_TOKEN'] = 'test_token'
os.environ['MEGA_EMAIL'] = 'test@example.com'
os.environ['MEGA_PASSWORD'] = 'test_password'
os.environ['STORAGE_PATH'] = tempfile.mkdtemp()
os.environ['DATABASE_PATH'] = os.path.join(tempfile.mkdtemp(), 'test.db')

from src.utils import (
    format_size,
    format_progress_bar,
    validate_mega_link,
    DownloadSpeedTracker
)
from src.database import Database, DownloadStatus


class TestUtils:
    """Test utility functions."""
    
    def test_format_size(self):
        """Test size formatting."""
        assert "0 Bytes" in format_size(0) or "0B" in format_size(0).replace(" ", "")
        assert "1" in format_size(1024) and ("K" in format_size(1024) or "Ki" in format_size(1024))
        assert "1" in format_size(1024 * 1024) and "M" in format_size(1024 * 1024)
        assert "1" in format_size(1024 * 1024 * 1024) and "G" in format_size(1024 * 1024 * 1024)
    
    def test_format_progress_bar(self):
        """Test progress bar formatting."""
        bar = format_progress_bar(0, 100)
        assert "0" in bar or "░" in bar
        
        bar = format_progress_bar(50, 100)
        assert "50" in bar
        
        bar = format_progress_bar(100, 100)
        assert "100" in bar
    
    def test_validate_mega_link(self):
        """Test MEGA link validation."""
        # Valid links
        assert validate_mega_link("https://mega.nz/folder/ABC123#key456")
        assert validate_mega_link("https://mega.nz/#F!ABC123!key456")
        
        # Invalid links
        assert not validate_mega_link("https://google.com")
        assert not validate_mega_link("not a link")
        assert not validate_mega_link("")
    
    def test_speed_tracker(self):
        """Test download speed tracking."""
        tracker = DownloadSpeedTracker()
        
        # First update
        speed = tracker.update(1000)
        assert speed == 0  # No previous sample
        
        # Simulate time passing
        import time
        time.sleep(0.1)
        
        # Second update
        speed = tracker.update(2000)
        assert speed >= 0


class TestDatabase:
    """Test database operations."""
    
    @pytest.fixture
    async def db(self):
        """Create a test database."""
        db_path = Path(tempfile.mkdtemp()) / "test.db"
        database = Database(db_path)
        await database.connect()
        yield database
        await database.close()
        if db_path.exists():
            db_path.unlink()
    
    @pytest.mark.asyncio
    async def test_create_request(self, db):
        """Test creating a download request."""
        request_id = await db.create_request(
            user_id=12345,
            chat_id=12345,
            mega_link="https://mega.nz/folder/test#key",
            folder_name="Test Folder",
            folder_structure={"files": []}
        )
        
        assert request_id > 0
        
        # Retrieve request
        request = await db.get_request(request_id)
        assert request is not None
        assert request.user_id == 12345
        assert request.folder_name == "Test Folder"
    
    @pytest.mark.asyncio
    async def test_update_request(self, db):
        """Test updating a download request."""
        request_id = await db.create_request(
            user_id=12345,
            chat_id=12345,
            mega_link="https://mega.nz/folder/test#key",
            folder_name="Test Folder",
            folder_structure={"files": []}
        )
        
        await db.update_request(
            request_id,
            status=DownloadStatus.DOWNLOADING.value,
            total_files=10,
            total_size=1024 * 1024 * 100
        )
        
        request = await db.get_request(request_id)
        assert request.status == "downloading"
        assert request.total_files == 10
    
    @pytest.mark.asyncio
    async def test_add_files(self, db):
        """Test adding files to a request."""
        request_id = await db.create_request(
            user_id=12345,
            chat_id=12345,
            mega_link="https://mega.nz/folder/test#key",
            folder_name="Test Folder",
            folder_structure={"files": []}
        )
        
        files = [
            {"path": "file1.txt", "name": "file1.txt", "size": 1024, "handle": "h1"},
            {"path": "file2.txt", "name": "file2.txt", "size": 2048, "handle": "h2"},
        ]
        
        await db.add_files(request_id, files)
        
        pending = await db.get_pending_files(request_id)
        assert len(pending) == 2
    
    @pytest.mark.asyncio
    async def test_download_stats(self, db):
        """Test getting download statistics."""
        request_id = await db.create_request(
            user_id=12345,
            chat_id=12345,
            mega_link="https://mega.nz/folder/test#key",
            folder_name="Test Folder",
            folder_structure={"files": []}
        )
        
        files = [
            {"path": "file1.txt", "name": "file1.txt", "size": 1024, "handle": "h1"},
        ]
        await db.add_files(request_id, files)
        
        stats = await db.get_download_stats(request_id)
        assert stats['total_files'] == 1
        assert stats['total_size'] == 1024
        assert stats['completed_files'] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
