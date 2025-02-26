"""
API response models
"""
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime


class ApiResponse(BaseModel):
    """Base API response model"""
    message: str
    status: str = "success"


class ErrorResponse(BaseModel):
    """Error response model"""
    detail: str
    status: str = "error"
    error_code: Optional[str] = None
    additional_info: Optional[Dict[str, Any]] = None


class ModelConfig(BaseModel):
    """Configuration for model generation"""
    table_name: str = Field(..., description="Name of the table to generate model for")
    model_type: Optional[str] = Field(None, description="Type of model to generate (hub, link, satellite, auto). If None or 'auto', all appropriate types will be determined automatically")
    use_ai_enhancement: bool = Field(False, description="Use AI to enhance the model")
    business_keys: Optional[List[str]] = Field(None, description="Columns to use as business keys")
    include_columns: Optional[List[str]] = Field(None, description="Columns to include in the model")
    exclude_columns: Optional[List[str]] = Field(None, description="Columns to exclude from the model")
    additional_config: Optional[Dict[str, Any]] = Field(None, description="Additional configuration")


class ModelGenerationResponse(ApiResponse):
    """Response model for model generation"""
    model_yaml_a: str
    table_name: str
    model_type: str
    metadata_count: int
    warnings: Optional[List[str]] = Field(default_factory=list, description="Validation warnings")
    generation_timestamp: datetime = Field(default_factory=datetime.utcnow)


class DBTResponse(ApiResponse):
    """Response model for DBT operations"""
    details: Optional[Dict[str, Any]] = None


class DBTRunResponse(BaseModel):
    """Response model for DBT run operations"""
    job_id: str
    status: str  # running, completed, failed
    command: str
    results: Optional[Dict[str, Any]] = None
    start_time: datetime = Field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None


class FileUploadResponse(ApiResponse):
    """Response model for file upload"""
    filename: str
    file_size: int
    file_type: str
    rows_processed: Optional[int] = None
    columns_processed: Optional[int] = None
    details: Optional[Dict[str, Any]] = None


class HealthCheckResponse(BaseModel):
    """Health check response"""
    status: str = "ok"
    version: str
    database_connection: bool
    git_connection: Optional[bool] = None
    dbt_connection: Optional[bool] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
