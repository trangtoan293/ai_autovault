"""
Data model generation endpoints
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session

from app.api.dependencies import get_db
from app.models.metadata import MetadataResponse
from app.models.response import ModelGenerationResponse, ModelConfig
from app.core.logging import logger
from app.services.model_generator import ModelGeneratorService

router = APIRouter()
model_service = ModelGeneratorService()


@router.post("/generate", response_model=ModelGenerationResponse)
async def generate_models(
    config: ModelConfig = Body(...),
    db: Session = Depends(get_db)
):
    """
    Generate data models based on metadata and configuration
    """
    try:
        # Log request details
        import json
        logger.info(f"Received generate_models request with config: {json.dumps(config.dict(), default=str)}")
        
        result = await model_service.generate_models(config, db)
        print(result)
        return result
    except Exception as e:
        logger.error(f"Error in generate_models: {str(e)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/simple-generate", response_model=dict)
async def simple_generate(config: dict = Body(...)):
    """
    Simple model generation endpoint - for debugging
    """
    try:
        logger.info(f"Received simple_generate request with config: {config}")
        return {
            "message": "Test successful",
            "received_config": config
        }
    except Exception as e:
        logger.error(f"Error in simple_generate: {str(e)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/templates", response_model=List[str])
def get_available_templates():
    """
    List available model templates
    """
    try:
        templates = model_service.get_available_templates()
        return templates
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/preview/{metadata_id}", response_model=ModelGenerationResponse)
async def preview_model(
    metadata_id: int,
    model_type: str = Query(..., description="Model type (hub, link, satellite)"),
    db: Session = Depends(get_db)
):
    """
    Preview a data model based on specific metadata
    """
    try:
        result = await model_service.preview_model(metadata_id, model_type, db)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# @router.post("/hierarchical/{source_system}", response_model=ModelGenerationResponse)
# async def generate_hierarchical_models(
#     source_system: str,
#     db: Session = Depends(get_db)
# ):
#     """
#     Generate Data Vault models using hierarchical metadata structure
#     """
#     try:
#         result = await model_service.generate_hierarchical_models(source_system, db)
#         return result
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))


# @router.post("/batch", response_model=ModelGenerationResponse)
# async def batch_generate_models(
#     metadata_ids: List[int] = Body(...),
#     model_type: str = Body(...),
#     db: Session = Depends(get_db)
# ):
#     """
#     Generate multiple models in batch
#     """
#     try:
#         result = await model_service.batch_generate_models(metadata_ids, model_type, db)
#         return result
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))
