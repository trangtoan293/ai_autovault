"""
API dependencies
"""
from typing import Generator
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import get_current_user
from app.models.config import User

# Database dependency
def get_db() -> Generator:
    """
    Dependency for database session
    """
    from app.services.metadata_store import get_session
    db = get_session()
    try:
        yield db
    finally:
        db.close()

# OAuth2 scheme for token-based authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{settings.API_V1_STR}/auth/login")

# Authentication dependencies
def get_current_active_user(current_user: User = Depends(get_current_user)) -> User:
    """
    Get current active user
    """
    # In a real application, check if user is active
    return current_user
