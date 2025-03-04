"""
API endpoints for handling YAML-based Data Vault modeling and graph creation.
These endpoints allow uploading and processing YAML files to create data vault graphs.
"""

from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile, Form, Query, Body, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
import yaml
import os
import tempfile

from app.core.logging import logger
from app.api.dependencies import get_db
from app.core.security import get_current_active_user, User
from app.knowledge_graph.services.graph_builder import GraphBuilder

router = APIRouter()

@router.post("/build-data-vault-from-yaml", status_code=status.HTTP_201_CREATED)
async def build_data_vault_from_yaml(
    yaml_content: str = Body(..., description="YAML content for Data Vault definition"),
    source_system_name: Optional[str] = Query("Unknown", description="Name of the source system"),
    background_tasks: Optional[BackgroundTasks] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Build a Data Vault graph from YAML content
    
    Args:
        yaml_content: String containing the YAML definition
        source_system_name: Name of the source system (default: Unknown)
        background_tasks: FastAPI background tasks
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        JSON response with operation results
    """
    logger.info(f"User {current_user.username} requested to build Data Vault graph from YAML content")
    
    try:
        # Validate YAML content
        try:
            yaml_data = yaml.safe_load(yaml_content)
            if not yaml_data or not isinstance(yaml_data, dict):
                raise ValueError("Invalid YAML content structure")
        except Exception as e:
            logger.error(f"Invalid YAML content: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid YAML content: {str(e)}"
            )
        
        builder = GraphBuilder()
        
        # Check for required fields
        required_fields = ['source_schema', 'source_table', 'target_schema', 'target_table']
        for field in required_fields:
            if field not in yaml_data:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Missing required field in YAML: {field}"
                )
        
        # If background tasks are provided, run the build in the background
        if background_tasks:
            logger.info("Running Data Vault graph build in background")
            background_tasks.add_task(
                builder.build_detailed_data_vault_with_cache,
                db,
                yaml_content,
                source_system_name
            )
            return {
                "message": "Data Vault graph build started in background",
                "source_system": source_system_name,
                "source_schema": yaml_data.get('source_schema'),
                "target_schema": yaml_data.get('target_schema'),
                "target_table": yaml_data.get('target_table')
            }
        
        # Otherwise, run synchronously
        result = builder.build_detailed_data_vault_with_cache(db, yaml_content, source_system_name)
        
        return {
            "message": "Data Vault graph built successfully",
            "source_system": source_system_name,
            "source_schema": result["summary"]["source_schema"],
            "source_table": result["summary"]["source_table"],
            "target_schema": result["summary"]["target_schema"],
            "target_table": result["summary"]["target_table"],
            "entity_type": result["summary"]["entity_type"],
            "nodes_created": result["summary"]["nodes_count"],
            "relationships_created": result["summary"]["relationships_count"],
            "execution_time": result["summary"]["execution_time"]
        }
    except Exception as e:
        logger.error(f"Error building Data Vault graph from YAML: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error building Data Vault graph from YAML: {str(e)}"
        )

@router.post("/build-data-vault-from-yaml-file", status_code=status.HTTP_201_CREATED)
async def build_data_vault_from_yaml_file(
    file: UploadFile = File(..., description="YAML file for Data Vault definition"),
    source_system_name: Optional[str] = Form("Unknown", description="Name of the source system"),
    background_tasks: Optional[BackgroundTasks] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Build a Data Vault graph from an uploaded YAML file
    
    Args:
        file: Uploaded YAML file
        source_system_name: Name of the source system (default: Unknown)
        background_tasks: FastAPI background tasks
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        JSON response with operation results
    """
    logger.info(f"User {current_user.username} requested to build Data Vault graph from YAML file")
    
    # Validate file
    if not file.filename.endswith(('.yaml', '.yml')):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file must be a YAML file (.yaml or .yml)"
        )
    
    try:
        # Read file content
        yaml_content = await file.read()
        yaml_content_str = yaml_content.decode('utf-8')
        
        # Validate YAML content
        try:
            yaml_data = yaml.safe_load(yaml_content_str)
            if not yaml_data or not isinstance(yaml_data, dict):
                raise ValueError("Invalid YAML file structure")
        except Exception as e:
            logger.error(f"Invalid YAML file: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid YAML file: {str(e)}"
            )
        
        # Check for required fields
        required_fields = ['source_schema', 'source_table', 'target_schema', 'target_table']
        for field in required_fields:
            if field not in yaml_data:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Missing required field in YAML: {field}"
                )
        
        builder = GraphBuilder()
        
        # If background tasks are provided, run the build in the background
        if background_tasks:
            logger.info("Running Data Vault graph build in background")
            background_tasks.add_task(
                builder.build_detailed_data_vault_with_cache,
                db,
                yaml_content_str,
                source_system_name
            )
            return {
                "message": "Data Vault graph build started in background",
                "file": file.filename,
                "source_system": source_system_name,
                "source_schema": yaml_data.get('source_schema'),
                "target_schema": yaml_data.get('target_schema'),
                "target_table": yaml_data.get('target_table')
            }
        
        # Otherwise, run synchronously
        result = builder.build_detailed_data_vault_with_cache(db, yaml_content_str, source_system_name)
        
        return {
            "message": "Data Vault graph built successfully",
            "file": file.filename,
            "source_system": source_system_name,
            "source_schema": result["summary"]["source_schema"],
            "source_table": result["summary"]["source_table"],
            "target_schema": result["summary"]["target_schema"],
            "target_table": result["summary"]["target_table"],
            "entity_type": result["summary"]["entity_type"],
            "nodes_created": result["summary"]["nodes_count"],
            "relationships_created": result["summary"]["relationships_count"],
            "execution_time": result["summary"]["execution_time"]
        }
    except Exception as e:
        logger.error(f"Error building Data Vault graph from YAML file: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error building Data Vault graph from YAML file: {str(e)}"
        )

@router.post("/build-multiple-data-vault-from-yaml-files", status_code=status.HTTP_201_CREATED)
async def build_multiple_data_vault_from_yaml_files(
    files: List[UploadFile] = File(..., description="YAML files for Data Vault definitions"),
    source_system_name: Optional[str] = Form("Unknown", description="Name of the source system"),
    background_tasks: Optional[BackgroundTasks] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Build Data Vault graphs from multiple uploaded YAML files
    
    Args:
        files: List of uploaded YAML files
        source_system_name: Name of the source system (default: Unknown)
        background_tasks: FastAPI background tasks
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        JSON response with operation results
    """
    logger.info(f"User {current_user.username} requested to build Data Vault graphs from multiple YAML files")
    
    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No files uploaded"
        )
    
    results = []
    errors = []
    
    # Initialize node_cache to avoid duplicate nodes across files
    node_cache = {
        "source_schemas": {},  # {name: id}
        "target_schemas": {},  # {name: id}
        "source_tables": {},   # {schema.name: id}
        "target_tables": {},   # {schema.name: id}
        "source_columns": {},  # {table.schema.name: id}
        "target_columns": {}   # {table.schema.name: id}
    }
    
    builder = GraphBuilder()
    
    for file in files:
        try:
            # Validate file
            if not file.filename.endswith(('.yaml', '.yml')):
                errors.append({
                    "file": file.filename,
                    "error": "Not a YAML file"
                })
                continue
            
            # Read file content
            yaml_content = await file.read()
            yaml_content_str = yaml_content.decode('utf-8')
            
            # Validate YAML content
            try:
                yaml_data = yaml.safe_load(yaml_content_str)
                if not yaml_data or not isinstance(yaml_data, dict):
                    raise ValueError("Invalid YAML file structure")
            except Exception as e:
                errors.append({
                    "file": file.filename,
                    "error": f"Invalid YAML content: {str(e)}"
                })
                continue
            
            # Check for required fields
            required_fields = ['source_schema', 'source_table', 'target_schema', 'target_table']
            missing_fields = [field for field in required_fields if field not in yaml_data]
            if missing_fields:
                errors.append({
                    "file": file.filename,
                    "error": f"Missing required fields: {', '.join(missing_fields)}"
                })
                continue
            
            # Build Data Vault graph using the shared node_cache
            result = builder.build_detailed_data_vault_with_cache(
                db, 
                yaml_content_str,
                source_system_name,
                node_cache
            )
            
            results.append({
                "file": file.filename,
                "source_schema": result["summary"]["source_schema"],
                "source_table": result["summary"]["source_table"],
                "target_schema": result["summary"]["target_schema"],
                "target_table": result["summary"]["target_table"],
                "entity_type": result["summary"]["entity_type"],
                "nodes_created": result["summary"]["nodes_count"],
                "relationships_created": result["summary"]["relationships_count"],
                "execution_time": result["summary"]["execution_time"]
            })
            
        except Exception as e:
            logger.error(f"Error processing file {file.filename}: {str(e)}")
            errors.append({
                "file": file.filename,
                "error": str(e)
            })
    
    return {
        "message": f"Processed {len(files)} files: {len(results)} successful, {len(errors)} failed",
        "source_system": source_system_name,
        "successful": results,
        "errors": errors
    }
