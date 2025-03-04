"""
Enhanced model generation API endpoints
Combines automatic and manual input with unified storage
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Union

from app.api.dependencies import get_db
from app.models.response import ModelGenerationResponse, ModelConfig
from app.models.data_vault import (
    ManualComponentInput, HubComponent, LinkComponent, 
    SatelliteComponent, LinkSatelliteComponent, SimpleManualInput
)
from app.services.model_generator_enhanced import EnhancedModelGeneratorService
from app.services.data_vault_store import DataVaultStoreService
from app.core.logging import logger
from app.core.security import get_current_active_user
from app.models.config import User

router = APIRouter()
model_generator = EnhancedModelGeneratorService()


@router.post("/gen_auto_model/{source_system}/{table_name}", response_model=ModelGenerationResponse)
async def generate_models_from_source(
    source_system: str = Path(..., description="Source system name"),
    table_name: str = Path(..., description="Table name"),
    db: Session = Depends(get_db),
        current_user: User = Depends(get_current_active_user)

) -> ModelGenerationResponse:
    """
    Generate Data Vault models from a specific source and table
    """
    return await model_generator.generate_models_from_source(source_system, table_name, db)


@router.post("/gen_manual_model", response_model=ModelGenerationResponse)
async def generate_models_from_simple_input(
    input_data: SimpleManualInput,
    source_system: str = Query(..., description="Source system name"),
    table_name: str = Query(..., description="Table name"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
) -> ModelGenerationResponse:
    """
    Generate Data Vault models from a simple input format (similar to LLM output)
    """
    return await model_generator.generate_models_from_simple_input(input_data, source_system, table_name, db)


@router.get("/get_models/{source_system}/{table_name}", response_model=Dict[str, List[Dict[str, Any]]])
def get_data_model_by_source_table(
    source_system: str = Path(..., description="Source system name"),
    table_name: str = Path(..., description="Table name"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get saved models for a specific source system and table
    """
    return model_generator.get_data_model_by_source_table(db, source_system, table_name)


@router.get("/get_models/all", response_model=Dict[str, List[Dict[str, Any]]])
def get_all_data_vault_models(db: Session = Depends(get_db),current_user: User = Depends(get_current_active_user)) -> Dict[str, Any]:
    """
    Get all Data Vault models across all source systems and tables
    Returns a hierarchical structure: source_system -> table_name -> component_type -> components
    """
    return model_generator.get_data_model_all(db)


@router.get("/get_models/{component_type}", response_model=Dict[str, List[Dict[str, Any]]])
def get_data_model_by_type(
    component_type: str = Path(..., description="component type (hub, link, satellite, link_satellite)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """
    Get all Data Vault models by component type
    """
    return model_generator.get_data_model_by_type(db, component_type)


@router.get("/lineage/source_to_target/{source_system}/{source_table}", response_model=List[Dict[str, Any]])
def get_source_to_target_lineage(
    source_system: str = Path(..., description="Source system name"),
    source_table: str = Path(..., description="Source table name"),
    source_schema: Optional[str] = Query(None, description="Source schema name"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
) -> List[Dict[str, Any]]:
    """
    Get data lineage from source to target Data Vault components
    """
    data_vault_store = DataVaultStoreService()
    return data_vault_store.get_source_to_target_lineage(db, source_system, source_schema, source_table)


@router.get("/lineage/target_to_source/{target_schema}/{target_table}", response_model=Dict[str, Any])
def get_target_to_source_lineage(
    target_schema: str = Path(..., description="Target schema name"),
    target_table: str = Path(..., description="Target table name"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """
    Get data lineage from target Data Vault component back to source
    """
    data_vault_store = DataVaultStoreService()
    return data_vault_store.get_target_to_source_lineage(db, target_schema, target_table)
