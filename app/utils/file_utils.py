"""
File handling utilities
"""
import os
import tempfile
import shutil
from typing import List, Dict, Any, Optional, BinaryIO
from pathlib import Path
from fastapi import UploadFile

from app.core.logging import logger


def get_file_extension(filename: str) -> str:
    """
    Get the file extension from a filename
    """
    return os.path.splitext(filename)[1][1:].lower()


async def save_upload_file(upload_file: UploadFile, destination: str) -> str:
    """
    Save an uploaded file to a destination path
    
    Args:
        upload_file: The uploaded file
        destination: The destination directory
        
    Returns:
        The full path to the saved file
    """
    try:
        # Create destination directory if it doesn't exist
        os.makedirs(destination, exist_ok=True)
        
        # Create full path
        file_path = os.path.join(destination, upload_file.filename)
        
        # Save file
        with open(file_path, "wb") as f:
            shutil.copyfileobj(upload_file.file, f)
            
        logger.info(f"Saved file to {file_path}")
        return file_path
    
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}")
        raise
    finally:
        upload_file.file.close()


async def save_upload_file_temp(upload_file: UploadFile) -> str:
    """
    Save an uploaded file to a temporary location
    
    Args:
        upload_file: The uploaded file
        
    Returns:
        The full path to the saved temporary file
    """
    try:
        # Create temporary file with the same extension
        suffix = f".{get_file_extension(upload_file.filename)}"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp:
            shutil.copyfileobj(upload_file.file, temp)
            
        logger.info(f"Saved file to temporary location {temp.name}")
        return temp.name
    
    except Exception as e:
        logger.error(f"Error saving temporary file: {str(e)}")
        raise
    finally:
        upload_file.file.close()


def list_files(directory: str, pattern: Optional[str] = None) -> List[str]:
    """
    List files in a directory with optional pattern matching
    
    Args:
        directory: The directory to search
        pattern: Optional glob pattern to match
        
    Returns:
        List of file paths
    """
    try:
        if pattern:
            return [str(f) for f in Path(directory).glob(pattern) if f.is_file()]
        else:
            return [str(f) for f in Path(directory).iterdir() if f.is_file()]
    
    except Exception as e:
        logger.error(f"Error listing files in {directory}: {str(e)}")
        raise


def ensure_directory(directory: str) -> None:
    """
    Ensure a directory exists, creating it if necessary
    
    Args:
        directory: The directory path to ensure
    """
    try:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")
    
    except Exception as e:
        logger.error(f"Error creating directory {directory}: {str(e)}")
        raise


def remove_directory(directory: str, recursive: bool = False) -> None:
    """
    Remove a directory
    
    Args:
        directory: The directory to remove
        recursive: Whether to remove recursively
    """
    try:
        if recursive:
            shutil.rmtree(directory)
        else:
            os.rmdir(directory)
        logger.info(f"Removed directory: {directory}")
    
    except Exception as e:
        logger.error(f"Error removing directory {directory}: {str(e)}")
        raise


def get_file_info(file_path: str) -> Dict[str, Any]:
    """
    Get information about a file
    
    Args:
        file_path: Path to the file
        
    Returns:
        Dictionary with file information
    """
    try:
        file_stat = os.stat(file_path)
        file_info = {
            "name": os.path.basename(file_path),
            "path": file_path,
            "size": file_stat.st_size,
            "created": file_stat.st_ctime,
            "modified": file_stat.st_mtime,
            "is_directory": os.path.isdir(file_path),
            "extension": get_file_extension(file_path) if not os.path.isdir(file_path) else None
        }
        return file_info
    
    except Exception as e:
        logger.error(f"Error getting file info for {file_path}: {str(e)}")
        raise
