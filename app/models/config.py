"""
Configuration data models
"""
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field


class User(BaseModel):
    """User model"""
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = False
    role: Optional[str] = "user"


class UserInDB(User):
    """User model with password hash"""
    hashed_password: str


class TokenData(BaseModel):
    """Token data model"""
    username: Optional[str] = None


class Token(BaseModel):
    """Token model"""
    access_token: str
    token_type: str
    expires_in: int
    user: User


class DatabaseSettings(BaseModel):
    """Database connection settings"""
    type: str = Field(..., description="Database type (snowflake, postgres, etc.)")
    account: Optional[str] = Field(None, description="Account (for Snowflake)")
    user: str = Field(..., description="Database user")
    password: str = Field(..., description="Database password")
    host: Optional[str] = Field(None, description="Database host")
    port: Optional[int] = Field(None, description="Database port")
    database: str = Field(..., description="Database name")
    schema: str = Field(..., description="Database schema")
    warehouse: Optional[str] = Field(None, description="Warehouse (for Snowflake)")
    role: Optional[str] = Field(None, description="Role (for Snowflake)")
    additional_params: Optional[Dict[str, Any]] = Field(None, description="Additional connection parameters")


class DBTSettings(BaseModel):
    """DBT configuration settings"""
    project_name: str = Field(..., description="DBT project name")
    profile_name: str = Field(..., description="DBT profile name")
    target: str = Field("dev", description="DBT target environment")
    models_path: str = Field("models", description="Path to DBT models directory")
    test_path: Optional[str] = Field("tests", description="Path to DBT tests directory")
    analysis_path: Optional[str] = Field("analyses", description="Path to DBT analyses directory")
    data_path: Optional[str] = Field("seeds", description="Path to DBT seed data directory")
    docs_path: Optional[str] = Field("docs", description="Path to DBT documentation directory")
    vars: Optional[Dict[str, Any]] = Field(None, description="Variables to pass to DBT")


class UserSettings(BaseModel):
    """User settings"""
    theme: Optional[str] = Field("light", description="UI theme (light/dark)")
    language: Optional[str] = Field("en", description="Interface language")
    show_welcome: Optional[bool] = Field(True, description="Show welcome message")
    default_model_type: Optional[str] = Field("vault", description="Default data modeling methodology")
    max_results: Optional[int] = Field(100, description="Maximum results to display")
    telemetry_enabled: Optional[bool] = Field(True, description="Enable anonymous telemetry")


class LoginRequest(BaseModel):
    """Login request model"""
    username: str
    password: str


class CompileModelRequest(BaseModel):
    """Request model for compiling DBT models"""
    models: Optional[List[str]] = None
