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
from app.core.logging import logger

router = APIRouter()
model_generator = EnhancedModelGeneratorService()


@router.post("/gen_auto_model/{source_system}/{table_name}", response_model=ModelGenerationResponse)
async def generate_models_from_source(
    source_system: str = Path(..., description="Source system name"),
    table_name: str = Path(..., description="Table name"),
    db: Session = Depends(get_db)
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
    db: Session = Depends(get_db)
) -> ModelGenerationResponse:
    """
    Generate Data Vault models from a simple input format (similar to LLM output)
    """
    return await model_generator.generate_models_from_simple_input(input_data, source_system, table_name, db)


@router.get("/get_models/{source_system}/{table_name}", response_model=Dict[str, List[Dict[str, Any]]])
def get_data_model_by_source_table(
    source_system: str = Path(..., description="Source system name"),
    table_name: str = Path(..., description="Table name"),
    db: Session = Depends(get_db)
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get saved models for a specific source system and table
    """
    return model_generator.get_data_model_by_source_table(db, source_system, table_name)


@router.get("/get_models/all", response_model=Dict[str, List[Dict[str, Any]]])
def get_all_data_vault_models(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    Get all Data Vault models across all source systems and tables
    Returns a hierarchical structure: source_system -> table_name -> component_type -> components
    """
    return model_generator.get_data_model_all(db)


@router.get("/get_models/{component_type}", response_model=Dict[str, List[Dict[str, Any]]])
def get_data_model_by_type(component_type:str=Path(..., description="component type (hub, link, satellite, link_satellite)")
                           ,db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    Get all Data Vault models across all source systems and tables
    Returns a hierarchical structure: source_system -> table_name -> component_type -> components
    """
    return model_generator.get_data_model_by_type(db,component_type)



# Legacy endpoints - keeping for backward compatibility
@router.post("/hub", response_model=Dict[str, Any])
async def create_hub_model(
    hub: HubComponent,
    source_system: str = Query(..., description="Source system name"),
    table_name: str = Query(..., description="Table name"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Create a single Hub model (Legacy endpoint - use /auto_model/by_type/hub instead)
    """
    input_data = ManualComponentInput(
        hubs=[hub],
        source_system=source_system,
        table_name=table_name
    )
    
    response = await model_generator.generate_models_from_manual_input(input_data, db)
    
    # Get the saved model
    saved_models = model_generator.get_data_model_by_source_table(db, source_system, table_name)
    
    # Find the matching hub by name
    for saved_hub in saved_models["hubs"]:
        if saved_hub["name"] == hub.name:
            return saved_hub
    
    # If not found, return response message
    return {"message": "Hub created but not found in saved models"}

@router.post("/link", response_model=Dict[str, Any])
async def create_link_model(
    link: LinkComponent,
    source_system: str = Query(..., description="Source system name"),
    table_name: str = Query(..., description="Table name"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Create a single Link model (Legacy endpoint - use /auto_model/by_type/link instead)
    """
    input_data = ManualComponentInput(
        links=[link],
        source_system=source_system,
        table_name=table_name
    )
    
    response = await model_generator.generate_models_from_manual_input(input_data, db)
    
    # Get the saved model
    saved_models = model_generator.get_data_model_by_source_table(db, source_system, table_name)
    
    # Find the matching link by name
    for saved_link in saved_models["links"]:
        if saved_link["name"] == link.name:
            return saved_link
    
    # If not found, return response message
    return {"message": "Link created but not found in saved models"}


@router.post("/satellite", response_model=Dict[str, Any])
async def create_satellite_model(
    satellite: SatelliteComponent,
    source_system: str = Query(..., description="Source system name"),
    table_name: str = Query(..., description="Table name"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Create a single Satellite model (Legacy endpoint - use /auto_model/by_type/satellite instead)
    """
    input_data = ManualComponentInput(
        satellites=[satellite],
        source_system=source_system,
        table_name=table_name
    )
    
    response = await model_generator.generate_models_from_manual_input(input_data, db)
    
    # Get the saved model
    saved_models = model_generator.get_data_model_by_source_table(db, source_system, table_name)
    
    # Find the matching satellite by name
    for saved_sat in saved_models["satellites"]:
        if saved_sat["name"] == satellite.name:
            return saved_sat
    
    # If not found, return response message
    return {"message": "Satellite created but not found in saved models"}


@router.post("/link_satellite", response_model=Dict[str, Any])
async def create_link_satellite_model(
    link_satellite: LinkSatelliteComponent,
    source_system: str = Query(..., description="Source system name"),
    table_name: str = Query(..., description="Table name"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Create a single Link Satellite model (Legacy endpoint - use /auto_model/by_type/link_satellite instead)
    """
    input_data = ManualComponentInput(
        link_satellites=[link_satellite],
        source_system=source_system,
        table_name=table_name
    )
    
    response = await model_generator.generate_models_from_manual_input(input_data, db)
    
    # Get the saved model
    saved_models = model_generator.get_data_model_by_source_table(db, source_system, table_name)
    
    # Find the matching link satellite by name
    for saved_lsat in saved_models["link_satellites"]:
        if saved_lsat["name"] == link_satellite.name:
            return saved_lsat
    
    # If not found, return response message
    return {"message": "Link Satellite created but not found in saved models"}
