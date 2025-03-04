"""
Metadata API endpoints
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, File, UploadFile, Request
from sqlalchemy.orm import Session
from pydantic import TypeAdapter

from app.api.dependencies import get_db
from app.models.metadata import MetadataCreate, MetadataResponse, Metadata
from app.services.metadata_store import MetadataService
from app.core.logging import logger
from app.api.dependencies import get_current_active_user
from app.core.security import check_admin_permission
from app.models.config import User
router = APIRouter()
metadata_service = MetadataService()


@router.post("/upload", response_model=MetadataResponse)
async def upload_metadata_file(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),

):
    """
    Upload and process metadata file (CSV/Excel)
    """
    logger.info(f"Received upload request with content type: {request.headers.get('content-type')}")
    logger.info(f"File: {file.filename if file else 'No file received'}")
    
    try:
        result = await metadata_service.process_metadata_file(file, db)
        return result
    except Exception as e:
        logger.error(f"Error processing upload: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/", response_model=MetadataResponse)
def get_all_metadata(
    skip: int = 0, 
    limit: int = 100,
    source_system: Optional[str] = Query(None, description="Filter by source system"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)

):
    """
    Retrieve all metadata entries with optional filtering
    """
    metadata_list = metadata_service.get_all_metadata(db, skip=skip, limit=limit, source_system=source_system)
    
    # Convert SQLAlchemy models to Pydantic models
    metadata_adapter = TypeAdapter(List[Metadata])
    pydantic_records = metadata_adapter.validate_python(metadata_list)
    
    # Wrap the result in a MetadataResponse object
    response = MetadataResponse(
        message=f"Retrieved {len(metadata_list)} metadata entries",
        metadata_count=len(metadata_list),
        metadata=pydantic_records
    )
    
    return response


@router.get("/{metadata_id}", response_model=MetadataResponse)
def get_metadata(metadata_id: int, db: Session = Depends(get_db),current_user: User = Depends(get_current_active_user)
):
    """
    Retrieve a specific metadata entry by ID
    """
    metadata = metadata_service.get_metadata(db, metadata_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail="Metadata not found")
    
    # Convert SQLAlchemy model to Pydantic model
    metadata_adapter = TypeAdapter(List[Metadata])
    pydantic_records = metadata_adapter.validate_python([metadata])
    
    # Wrap the result in a MetadataResponse object
    response = MetadataResponse(
        message=f"Retrieved metadata with ID {metadata_id}",
        metadata_count=1,
        metadata=pydantic_records
    )
    
    return response


@router.put("/{metadata_id}", response_model=MetadataResponse)
def update_metadata(
    metadata_id: int, 
    metadata: MetadataCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)

):
    """
    Update a metadata entry
    """
    updated_metadata = metadata_service.update_metadata(db, metadata_id, metadata)
    if updated_metadata is None:
        raise HTTPException(status_code=404, detail="Metadata not found")
    
    # Convert SQLAlchemy model to Pydantic model
    metadata_adapter = TypeAdapter(List[Metadata])
    pydantic_records = metadata_adapter.validate_python([updated_metadata])
    
    # Wrap the result in a MetadataResponse object
    response = MetadataResponse(
        message=f"Updated metadata with ID {metadata_id}",
        metadata_count=1,
        metadata=pydantic_records
    )
    
    return response


@router.delete("/{metadata_id}", response_model=MetadataResponse)
def delete_metadata(metadata_id: int, db: Session = Depends(get_db),current_user: User = Depends(get_current_active_user)):
    """
    Delete a metadata entry
    """
    metadata = metadata_service.delete_metadata(db, metadata_id)
    if metadata is None:
        raise HTTPException(status_code=404, detail="Metadata not found")
    
    # Convert SQLAlchemy model to Pydantic model
    metadata_adapter = TypeAdapter(List[Metadata])
    pydantic_records = metadata_adapter.validate_python([metadata])
    
    # Wrap the result in a MetadataResponse object
    response = MetadataResponse(
        message=f"Deleted metadata with ID {metadata_id}",
        metadata_count=1,
        metadata=pydantic_records
    )
    
    return response
