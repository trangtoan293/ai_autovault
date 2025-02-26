#!/usr/bin/env python
"""
Script to create admin user
"""
import os
import sys
import argparse
from pathlib import Path

# Add parent directory to path so we can import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from passlib.context import CryptContext
from app.core.security import get_password_hash
from app.models.config import UserInDB


def create_admin_user(username, password, email=None):
    """
    Create a new admin user
    """
    try:
        # Import settings
        from app.core.config import settings
        
        # Create hashed password
        hashed_password = get_password_hash(password)
        
        # Create user
        user = UserInDB(
            username=username,
            email=email,
            hashed_password=hashed_password,
            disabled=False,
            role="admin"
        )
        
        # In a real application, you would save this to a database
        # For demo, we'll just print the user details
        print(f"User created successfully:")
        print(f"Username: {user.username}")
        print(f"Email: {user.email}")
        print(f"Role: {user.role}")
        print(f"Hashed password: {user.hashed_password}")
        
        # Provide instructions for adding to fake_users_db
        print("\nTo use this user, add the following to app/core/security.py fake_users_db:")
        print(f'''    "{user.username}": {{
        "username": "{user.username}",
        "email": "{user.email or ''}",
        "hashed_password": "{user.hashed_password}",
        "disabled": False,
        "role": "admin"
    }},''')
        
        return user
        
    except Exception as e:
        print(f"Error creating admin user: {str(e)}")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create admin user')
    parser.add_argument('--username', required=True, help='Admin username')
    parser.add_argument('--password', required=True, help='Admin password')
    parser.add_argument('--email', help='Admin email')
    
    args = parser.parse_args()
    
    user = create_admin_user(args.username, args.password, args.email)
    
    if user:
        print("\nAdmin user created successfully")
    else:
        print("\nFailed to create admin user")
        sys.exit(1)
