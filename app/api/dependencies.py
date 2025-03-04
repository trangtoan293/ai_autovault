"""
API dependencies
"""
from typing import Generator
from app.core.security import get_current_user,get_current_active_user,oauth2_scheme

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

