"""
Application configuration
"""
import os
from typing import List
# from pydantic import BaseSettings, validator
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings"""
    PROJECT_NAME: str = "Data Modeling Automation"
    API_V1_STR: str = "/api"
    
    # CORS
    CORS_ORIGINS: List[str] = ["*"]
    
    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY", "development_secret_key")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8  # 8 days
    
    # DBT
    DBT_PROJECT_DIR: str = os.getenv("DBT_PROJECT_DIR", "./dbt_project")
    DBT_PROFILES_DIR: str = os.getenv("DBT_PROFILES_DIR", "./dbt_profiles")
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./app.db")
    
    # Model Generation
    MODEL_TEMPLATES_DIR: str = os.getenv("MODEL_TEMPLATES_DIR", "./templates/models")
    
    # Git
    GIT_REPO_URL: str = os.getenv("GIT_REPO_URL", "")
    GIT_USERNAME: str = os.getenv("GIT_USERNAME", "")
    GIT_PASSWORD: str = os.getenv("GIT_PASSWORD", "")
    
    # LLM API keys
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
    
    # LLM Configuration
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "groq") 
    LLM_MODEL: str = os.getenv("LLM_MODEL", "llama3-8b-8192")
    
    # Neo4j Configuration
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "password")
    NEO4J_DATABASE: str = os.getenv("NEO4J_DATABASE", "neo4j")
    
    # Redis Cache
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # Default Data Vault settings
    DEFAULT_TARGET_SCHEMA: str = os.getenv("DEFAULT_TARGET_SCHEMA", "INTEGRATION")
    DEFAULT_COLLISION_CODE: str = os.getenv("DEFAULT_COLLISION_CODE", "MDM")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
