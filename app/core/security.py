"""
Authentication logic
"""
from datetime import datetime, timedelta
from typing import Any, Optional, Union

from jose import jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from app.core.config import settings
from app.models.config import TokenData, User, UserInDB
from app.core.logging import logger

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# Use the same path format as in dependencies.py
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{settings.API_V1_STR}/auth/login")

ALGORITHM = "HS256"

def create_access_token(subject: Union[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Create JWT access token
    """
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify password against hashed version
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """
    Hash a password
    """
    return pwd_context.hash(password)


# Fake user database for demonstration purposes
# In a real application, this would be replaced with a database
fake_users_db = {
    "admin": {
        "username": "admin",
        "email": "admin@example.com",
        "hashed_password": "$2b$12$UFypMUFAylDU7gTE6MT2CuXWQ8XHIw0O/QYW/DLWbDUzkJ4UYGHOy",  # 'password'
        "disabled": False,
        "role": "admin"
    },
    "user": {
        "username": "user",
        "email": "user@example.com",
        "hashed_password": "$2b$12$UFypMUFAylDU7gTE6MT2CuXWQ8XHIw0O/QYW/DLWbDUzkJ4UYGHOy",  # 'password'
        "disabled": False,
        "role": "user"
    },
    "testuser": {
        "username": "testuser",
        "email": "test@example.com",
        "hashed_password": "$2b$12$wJHCf1B3e7Jt5Gg9nRILkOIyiQnZj72xHoEwzaYA.rPqWg5j4vu22",  # 'test123'
        "disabled": False,
        "role": "admin"
    },
    "admin2": {
        "username": "admin2",
        "email": "admin2@example.com",
        "hashed_password": "$2b$12$IqvkISjszYxZrOuGELuY3.mnQqHo1/1B7n9S2UUXST.bRQh2jBHN2",  # 'admin123'
        "disabled": False,
        "role": "admin"
    }
}

def get_user(db, username: str) -> Optional[UserInDB]:
    """
    Get user from database
    """
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)
    return None

def authenticate_user(fake_db, username: str, password: str) -> Union[UserInDB, bool]:
    """
    Authenticate user with username and password
    """
    logger.info(f"Authentication attempt for user: {username}")
    
    user = get_user(fake_db, username)
    if not user:
        logger.warning(f"User not found: {username}")
        return False
    
    # Add more logging for debugging
    try:
        password_match = verify_password(password, user.hashed_password)
        logger.info(f"Password verification result for {username}: {password_match}")
        if not password_match:
            return False
        return user
    except Exception as e:
        logger.error(f"Password verification error: {str(e)}")
        return False

async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    """
    Get current user from token
    """
    logger.debug(f"Attempting to authenticate with token: {token[:10]}...")
    
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        logger.debug(f"Decoding JWT with SECRET_KEY length: {len(settings.SECRET_KEY)}")
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[ALGORITHM])
        logger.debug(f"JWT payload decoded: {payload}")
        
        username: str = payload.get("sub")
        if username is None:
            logger.warning("Username not found in token payload")
            raise credentials_exception
            
        token_data = TokenData(username=username)
        logger.debug(f"TokenData created: {token_data}")
    except jwt.JWTError as e:
        logger.error(f"JWT error: {str(e)}")
        raise credentials_exception
    
    # Fetch the user from our fake database
    logger.debug(f"Fetching user from database: {token_data.username}")
    user = get_user(fake_users_db, username=token_data.username)
    
    if user is None:
        logger.warning(f"User not found in database: {token_data.username}")
        raise credentials_exception
    
    logger.debug(f"User authenticated successfully: {user.username} with role: {user.role}")
    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)) -> User:
    """
    Check if user is active
    """
    logger.debug(f"Checking if user {current_user.username} is active")
    
    if current_user.disabled:
        logger.warning(f"User {current_user.username} is disabled/inactive")
        raise HTTPException(status_code=400, detail="Inactive user")
        
    logger.debug(f"User {current_user.username} is active and authorized")
    return current_user

def check_admin_permission(current_user: User = Depends(get_current_user)) -> User:
    """
    Check if user has admin role
    """
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Not enough permissions"
        )
    return current_user
